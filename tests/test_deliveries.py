from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from webhook_delivery_service.database import get_database_session
from webhook_delivery_service.main import app
from webhook_delivery_service.models import (
    OutboxMessage,
    WebhookDelivery,
)

client = TestClient(app)


def test_create_delivery_returns_created_delivery() -> None:
    created_at = datetime(2026, 8, 15, 20, 0, tzinfo=UTC)

    session = MagicMock(spec=AsyncSession)
    session.flush = AsyncMock()
    session.commit = AsyncMock()

    async def refresh_delivery(delivery: WebhookDelivery) -> None:
        delivery.status = "pending"
        delivery.attempt_count = 0
        delivery.next_attempt_at = None
        delivery.last_error = None
        delivery.created_at = created_at
        delivery.delivered_at = None

    session.refresh = AsyncMock(side_effect=refresh_delivery)

    async def override_database_session():
        yield session

    app.dependency_overrides[get_database_session] = override_database_session

    try:
        response = client.post(
            "/deliveries",
            json={
                "target_url": "https://example.com/webhooks",
                "event_type": "order.created",
                "payload": {"order_id": 123},
            },
        )
    finally:
        app.dependency_overrides.pop(get_database_session, None)

    assert session.add.call_count == 2

    added_delivery = session.add.call_args_list[0].args[0]
    added_outbox_message = session.add.call_args_list[1].args[0]

    assert isinstance(added_delivery, WebhookDelivery)
    assert isinstance(added_outbox_message, OutboxMessage)
    assert added_delivery.id is not None

    assert added_outbox_message.delivery_id == added_delivery.id
    assert added_outbox_message.message_type == "webhook.delivery.requested"
    assert added_outbox_message.payload == {
        "delivery_id": str(added_delivery.id),
    }

    assert response.status_code == 201
    assert response.json() == {
        "id": str(added_delivery.id),
        "target_url": "https://example.com/webhooks",
        "event_type": "order.created",
        "payload": {"order_id": 123},
        "status": "pending",
        "attempt_count": 0,
        "next_attempt_at": None,
        "last_error": None,
        "created_at": "2026-08-15T20:00:00Z",
        "delivered_at": None,
    }

    session.flush.assert_awaited_once_with()
    session.commit.assert_awaited_once_with()
    session.refresh.assert_awaited_once_with(added_delivery)
