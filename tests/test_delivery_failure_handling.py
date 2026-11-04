import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import httpx
import pytest
from aio_pika.abc import AbstractIncomingMessage
from sqlalchemy.ext.asyncio import AsyncSession

from webhook_delivery_service.http_sender import HttpxWebhookSender
from webhook_delivery_service.models import WebhookDelivery
from webhook_delivery_service.rabbitmq_consumer import consume_rabbitmq_delivery_message


async def consume_stored_delivery(
    sender: HttpxWebhookSender, event_type: str, expected_status: str
) -> WebhookDelivery:
    delivery = WebhookDelivery(
        id=uuid4(),
        target_url="https://example.com/webhooks",
        event_type=event_type,
        payload={"order_id": 123},
        status="pending",
        attempt_count=0,
    )
    session = MagicMock(spec=AsyncSession)
    session.get = AsyncMock(return_value=delivery)

    def commit_outcome() -> None:
        assert delivery.status == expected_status

    session.commit = AsyncMock(side_effect=commit_outcome)

    message = MagicMock(spec=AbstractIncomingMessage)
    message.body = json.dumps(
        {
            "message_id": str(uuid4()),
            "message_type": "webhook.delivery.requested",
            "payload": {"delivery_id": str(delivery.id)},
        }
    ).encode("utf-8")

    def acknowledge_after_commit() -> None:
        session.commit.assert_awaited_once_with()

    message.ack = AsyncMock(side_effect=acknowledge_after_commit)
    message.nack = AsyncMock()
    message.reject = AsyncMock()

    await consume_rabbitmq_delivery_message(message=message, session=session, sender=sender)

    session.get.assert_awaited_once_with(
        WebhookDelivery, delivery.id, with_for_update=True
    )
    message.ack.assert_awaited_once_with()
    message.nack.assert_not_awaited()
    message.reject.assert_not_awaited()
    assert delivery.attempt_count == 1
    return delivery


async def run_delivery_failure_handling_test(
    status_code: int, expected_status: str
) -> None:
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status_code=status_code)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        delivery = await consume_stored_delivery(
            HttpxWebhookSender(client), "order.created", expected_status
        )

    assert len(requests) == 1
    assert delivery.delivered_at is None
    assert delivery.last_error is not None
    assert str(status_code) in delivery.last_error
    if expected_status == "retry_scheduled":
        assert delivery.next_attempt_at is not None
        assert delivery.next_attempt_at > datetime.now(UTC)
    else:
        assert delivery.next_attempt_at is None


@pytest.mark.parametrize(
    ("status_code", "expected_status"),
    [(400, "failed"), (503, "retry_scheduled")],
)
def test_consumer_acknowledges_recorded_http_failure(
    status_code: int, expected_status: str
) -> None:
    asyncio.run(run_delivery_failure_handling_test(status_code, expected_status))


def test_consumer_processes_valid_delivery_after_invalid_stored_event_type() -> None:
    async def consume_both() -> None:
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            sender = HttpxWebhookSender(client)
            invalid = await consume_stored_delivery(sender, "order.é", "failed")
            assert not requests
            assert invalid.last_error is not None
            assert "event_type" in invalid.last_error
            assert "printable ASCII" in invalid.last_error
            assert invalid.next_attempt_at is None
            assert invalid.delivered_at is None

            valid = await consume_stored_delivery(sender, "order  created", "succeeded")

        assert valid.delivered_at is not None
        assert len(requests) == 1
        assert requests[0].headers["X-Delivery-ID"] == str(valid.id)
        assert requests[0].headers["X-Event-Type"] == "order  created"

    asyncio.run(consume_both())
