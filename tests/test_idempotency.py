import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from webhook_delivery_service import main
from webhook_delivery_service.database import get_database_session
from webhook_delivery_service.models import OutboxMessage, WebhookDelivery
from webhook_delivery_service.schemas import WebhookDeliveryCreate
from webhook_delivery_service.services import (
    IdempotencyConflictError,
    calculate_request_fingerprint,
    create_idempotent_webhook_delivery,
)


@pytest.fixture
def delivery_data() -> WebhookDeliveryCreate:
    return WebhookDeliveryCreate(
        target_url="https://example.com/webhooks",
        event_type="order.created",
        payload={"order_id": 123, "details": {"paid": True, "items": [1, 2]}},
    )


@pytest.fixture
def stored_delivery(delivery_data: WebhookDeliveryCreate) -> WebhookDelivery:
    return WebhookDelivery(
        id=uuid4(),
        target_url=str(delivery_data.target_url),
        event_type=delivery_data.event_type,
        payload=delivery_data.payload,
        idempotency_key="test-key",
        request_fingerprint=calculate_request_fingerprint(delivery_data),
        status="pending",
        attempt_count=0,
        created_at=datetime(2026, 10, 25, tzinfo=UTC),
    )


def test_fingerprint_ignores_nested_object_key_order(
    delivery_data: WebhookDeliveryCreate,
) -> None:
    reordered = WebhookDeliveryCreate(
        target_url=delivery_data.target_url,
        event_type=delivery_data.event_type,
        payload={"details": {"items": [1, 2], "paid": True}, "order_id": 123},
    )
    assert calculate_request_fingerprint(reordered) == calculate_request_fingerprint(
        delivery_data
    )


@pytest.mark.parametrize(
    "change",
    [
        {"target_url": "https://example.com/other"},
        {"event_type": "order.cancelled"},
        {"payload": {"order_id": 124}},
        {"payload": {"order_id": 123, "details": {"paid": 1, "items": [1, 2]}}},
        {"payload": {"order_id": 123, "details": {"paid": True, "items": [2, 1]}}},
    ],
)
def test_fingerprint_detects_changed_request(
    delivery_data: WebhookDeliveryCreate, change: dict[str, object]
) -> None:
    changed = WebhookDeliveryCreate.model_validate(
        delivery_data.model_dump(mode="json") | change
    )
    assert calculate_request_fingerprint(changed) != calculate_request_fingerprint(
        delivery_data
    )


def test_new_key_commits_delivery_and_outbox(
    delivery_data: WebhookDeliveryCreate, stored_delivery: WebhookDelivery
) -> None:
    session = MagicMock(spec=AsyncSession)
    session.scalar = AsyncMock(return_value=stored_delivery)
    session.commit = AsyncMock()
    session.refresh = AsyncMock()

    result = asyncio.run(
        create_idempotent_webhook_delivery(session, delivery_data, "test-key")
    )

    assert result == (stored_delivery, True)
    session.add.assert_called_once()
    outbox = session.add.call_args.args[0]
    assert isinstance(outbox, OutboxMessage)
    assert outbox.delivery_id == stored_delivery.id
    assert outbox.payload == {"delivery_id": str(stored_delivery.id)}
    session.commit.assert_awaited_once_with()
    session.refresh.assert_awaited_once_with(stored_delivery)


@pytest.mark.parametrize("conflicting", [False, True])
def test_existing_key_never_adds_an_outbox(
    delivery_data: WebhookDeliveryCreate,
    stored_delivery: WebhookDelivery,
    conflicting: bool,
) -> None:
    session = MagicMock(spec=AsyncSession)
    session.scalar = AsyncMock(return_value=None)
    rows = MagicMock()
    rows.one.return_value = stored_delivery
    session.scalars = AsyncMock(return_value=rows)
    session.commit = AsyncMock()
    stored_delivery.status = "failed"
    stored_delivery.attempt_count = 5
    stored_delivery.last_error = "HTTP 503"

    if conflicting:
        changed = delivery_data.model_copy(update={"event_type": "order.cancelled"})
        with pytest.raises(IdempotencyConflictError):
            asyncio.run(create_idempotent_webhook_delivery(session, changed, "test-key"))
    else:
        result = asyncio.run(
            create_idempotent_webhook_delivery(session, delivery_data, "test-key")
        )
        assert result == (stored_delivery, False)

    session.add.assert_not_called()
    session.commit.assert_not_awaited()
    assert stored_delivery.status == "failed"
    assert stored_delivery.attempt_count == 5
    assert stored_delivery.last_error == "HTTP 503"


@pytest.fixture
def api_client(monkeypatch: pytest.MonkeyPatch):
    async def override_database_session():
        yield MagicMock(spec=AsyncSession)

    monkeypatch.setitem(
        main.app.dependency_overrides,
        get_database_session,
        override_database_session,
    )
    with TestClient(main.app) as client:
        yield client


@pytest.mark.parametrize(("created", "status_code"), [(True, 201), (False, 200)])
def test_api_distinguishes_created_and_existing_delivery(
    monkeypatch: pytest.MonkeyPatch,
    api_client: TestClient,
    delivery_data: WebhookDeliveryCreate,
    stored_delivery: WebhookDelivery,
    created: bool,
    status_code: int,
) -> None:
    service = AsyncMock(return_value=(stored_delivery, created))
    monkeypatch.setattr(main, "create_idempotent_webhook_delivery", service)
    response = api_client.post(
        "/deliveries",
        json=delivery_data.model_dump(mode="json"),
        headers={"Idempotency-Key": "test-key"},
    )
    assert response.status_code == status_code
    assert response.json()["id"] == str(stored_delivery.id)
    assert "idempotency_key" not in response.json()
    assert "request_fingerprint" not in response.json()
    service.assert_awaited_once()
    assert service.await_args.kwargs["idempotency_key"] == "test-key"
    assert service.await_args.kwargs["delivery_data"] == delivery_data


def test_api_returns_conflict_for_reused_key(
    monkeypatch: pytest.MonkeyPatch,
    api_client: TestClient,
    delivery_data: WebhookDeliveryCreate,
) -> None:
    service = AsyncMock(side_effect=IdempotencyConflictError("different request"))
    monkeypatch.setattr(main, "create_idempotent_webhook_delivery", service)
    response = api_client.post(
        "/deliveries",
        json=delivery_data.model_dump(mode="json"),
        headers={"Idempotency-Key": "test-key"},
    )
    assert response.status_code == 409
    assert response.json() == {"detail": "different request"}


@pytest.mark.parametrize("key", ["", "x" * 256])
def test_api_rejects_invalid_key_length(
    monkeypatch: pytest.MonkeyPatch,
    api_client: TestClient,
    delivery_data: WebhookDeliveryCreate,
    key: str,
) -> None:
    service = AsyncMock()
    monkeypatch.setattr(main, "create_idempotent_webhook_delivery", service)
    response = api_client.post(
        "/deliveries",
        json=delivery_data.model_dump(mode="json"),
        headers={"Idempotency-Key": key},
    )
    assert response.status_code == 422
    service.assert_not_awaited()
