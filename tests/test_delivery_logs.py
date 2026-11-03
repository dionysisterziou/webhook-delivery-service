import asyncio
import json
from datetime import UTC, datetime, timedelta
from io import StringIO
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from webhook_delivery_service.consumer import handle_delivery_requested_message
from webhook_delivery_service.delivery_logs import delivery_log_output
from webhook_delivery_service.dispatcher import dispatch_outbox_message
from webhook_delivery_service.http_sender import HttpxWebhookSender
from webhook_delivery_service.models import OutboxMessage, WebhookDelivery
from webhook_delivery_service.retry_scheduler import schedule_due_webhook_retries
from webhook_delivery_service.schemas import WebhookDeliveryCreate
from webhook_delivery_service.services import (
    calculate_request_fingerprint,
    create_webhook_delivery,
)
from webhook_delivery_service.worker import process_webhook_delivery


def records(output: StringIO) -> list[dict]:
    return [json.loads(line) for line in output.getvalue().splitlines()]


def test_api_outputs_one_safe_json_event_for_creation_replay_and_conflict(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from webhook_delivery_service.database import get_database_session
    from webhook_delivery_service.main import app

    secret = "private-canary"
    body = {
        "target_url": f"https://example.test/hook?token={secret}",
        "event_type": "test.api",
        "payload": {"note": secret},
    }
    headers = {"Idempotency-Key": secret}
    existing = WebhookDelivery(
        id=uuid4(),
        **body,
        status="pending",
        attempt_count=0,
        created_at=datetime.now(UTC),
        idempotency_key=secret,
        request_fingerprint=calculate_request_fingerprint(
            WebhookDeliveryCreate.model_validate(body)
        ),
    )
    session = MagicMock(spec=AsyncSession)
    session.scalar.side_effect = [existing, None, None]
    session.scalars.return_value = MagicMock()
    session.scalars.return_value.one.return_value = existing

    async def override_session():
        yield session

    monkeypatch.setitem(
        app.dependency_overrides, get_database_session, override_session
    )
    with TestClient(app) as client:
        assert client.post("/deliveries", json=body, headers=headers).status_code == 201
        assert client.post("/deliveries", json=body, headers=headers).status_code == 200
        changed = body | {"event_type": "test.changed"}
        assert (
            client.post("/deliveries", json=changed, headers=headers).status_code == 409
        )

    output = capsys.readouterr().out
    rows = [json.loads(line) for line in output.splitlines()]
    assert [r["event"] for r in rows] == [
        "delivery.created",
        "delivery.replayed",
        "delivery.key_conflict",
    ]
    assert all(
        r["delivery_id"] == str(existing.id) and r["component"] == "api" for r in rows
    )
    assert secret not in output


def test_trace_correlates_creation_publications_and_both_attempts() -> None:
    async def run() -> None:
        session = MagicMock(spec=AsyncSession)
        messages = []
        secret = "private-canary"

        def track_added(obj: WebhookDelivery | OutboxMessage) -> None:
            # Supply the ORM defaults normally populated by a database flush.
            if isinstance(obj, OutboxMessage):
                obj.id = uuid4()
                messages.append(obj)
            else:
                obj.status, obj.attempt_count = "pending", 0

        session.add.side_effect = track_added
        publisher = AsyncMock()
        codes = iter([503, 200])
        transport = httpx.MockTransport(lambda request: httpx.Response(next(codes)))
        output = StringIO()
        with delivery_log_output(output):
            delivery = await create_webhook_delivery(
                session,
                WebhookDeliveryCreate(
                    target_url=f"https://example.test/hook?token={secret}",
                    event_type="test.trace",
                    payload={"note": secret},
                ),
            )
            session.get.return_value = delivery
            async with httpx.AsyncClient(transport=transport) as client:
                sender = HttpxWebhookSender(client)
                for attempt in (1, 2):
                    if attempt == 2:
                        # Stand in for the database returning a now-due delivery.
                        delivery.next_attempt_at = datetime.now(UTC) - timedelta(
                            seconds=1
                        )
                        session.scalars.return_value = MagicMock()
                        session.scalars.return_value.all.return_value = [delivery]
                        assert await schedule_due_webhook_retries(session) == 1
                    message = messages[-1]
                    await dispatch_outbox_message(session, publisher, message)
                    body = json.dumps(
                        {
                            "message_id": str(message.id),
                            "message_type": message.message_type,
                            "payload": message.payload,
                        }
                    ).encode()
                    await handle_delivery_requested_message(session, sender, body)

                # A duplicate after success must not produce a third attempt.
                await handle_delivery_requested_message(session, sender, body)

        rows = records(output)
        assert {row["delivery_id"] for row in rows} == {str(delivery.id)}
        assert all(
            datetime.fromisoformat(row["timestamp"]).utcoffset() == timedelta(0)
            for row in rows
        )
        assert [row["event"] for row in rows] == [
            "delivery.created",
            "outbox.published",
            "message.received",
            "attempt.started",
            "http.response",
            "delivery.retry_scheduled",
            "retry.queued",
            "outbox.published",
            "message.received",
            "attempt.started",
            "http.response",
            "delivery.succeeded",
            "message.received",
            "delivery.skipped",
        ]
        assert [
            (row["attempt_count"], row["status_code"])
            for row in rows
            if row["event"] == "http.response"
        ] == [(1, 503), (2, 200)]
        assert rows[5]["next_attempt_at"] and rows[5]["status"] == "retry_scheduled"
        assert rows[11]["status"] == "succeeded" and rows[11]["attempt_count"] == 2
        assert secret not in output.getvalue()
        ids = [str(message.id) for message in messages]
        assert len(set(ids)) == 2
        for event in ("outbox.published", "message.received"):
            assert [row["message_id"] for row in rows if row["event"] == event][
                :2
            ] == ids
        assert rows[0]["message_id"] == ids[0] and rows[6]["message_id"] == ids[1]

    asyncio.run(run())


@pytest.mark.parametrize("stage", ["create", "publish", "queue_retry"])
def test_no_committed_transition_is_reported_when_transaction_fails(stage: str) -> None:
    async def run() -> None:
        session = MagicMock(spec=AsyncSession)
        session.commit.side_effect = RuntimeError("commit failed")
        delivery = WebhookDelivery(
            id=uuid4(), status="retry_scheduled", attempt_count=1
        )
        output = StringIO()
        with (
            delivery_log_output(output),
            pytest.raises(RuntimeError, match="commit failed"),
        ):
            if stage == "create":
                await create_webhook_delivery(
                    session,
                    WebhookDeliveryCreate(
                        target_url="https://example.test/",
                        event_type="test.commit",
                        payload={},
                    ),
                )
            elif stage == "publish":
                await dispatch_outbox_message(
                    session,
                    AsyncMock(),
                    OutboxMessage(id=uuid4(), delivery_id=delivery.id),
                )
            else:
                session.scalars.return_value = MagicMock()
                session.scalars.return_value.all.return_value = [delivery]
                await schedule_due_webhook_retries(session)
        assert records(output) == []

    asyncio.run(run())


@pytest.mark.parametrize("code", [200, 400, 503])
def test_observed_http_result_survives_but_no_outcome_is_logged_if_commit_fails(
    code: int,
) -> None:
    async def run() -> None:
        delivery = WebhookDelivery(
            id=uuid4(),
            target_url="https://example.test/",
            event_type="test.commit",
            payload={},
            status="pending",
            attempt_count=0,
        )
        session = MagicMock(spec=AsyncSession)
        session.commit.side_effect = RuntimeError("commit failed")
        transport = httpx.MockTransport(lambda request: httpx.Response(code))
        output = StringIO()
        with delivery_log_output(output):
            async with httpx.AsyncClient(transport=transport) as client:
                with pytest.raises(RuntimeError, match="commit failed"):
                    await process_webhook_delivery(
                        session, HttpxWebhookSender(client), delivery
                    )
        rows = records(output)
        assert [row["event"] for row in rows] == ["attempt.started", "http.response"]
        assert rows[-1]["status_code"] == code

    asyncio.run(run())
