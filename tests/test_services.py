import asyncio
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from webhook_delivery_service.models import WebhookDelivery
from webhook_delivery_service.schemas import WebhookDeliveryCreate
from webhook_delivery_service.services import (
    create_webhook_delivery,
    get_webhook_delivery,
)


def test_create_webhook_delivery_adds_and_commits_delivery() -> None:
    session = MagicMock(spec=AsyncSession)
    session.commit = AsyncMock()
    session.refresh = AsyncMock()

    delivery_data = WebhookDeliveryCreate(
        target_url="https://example.com/webhooks",
        event_type="order.created",
        payload={"order_id": 123},
    )

    delivery = asyncio.run(
        create_webhook_delivery(
            session=session,
            delivery_data=delivery_data,
        )
    )

    assert delivery.target_url == "https://example.com/webhooks"
    assert delivery.event_type == "order.created"
    assert delivery.payload == {"order_id": 123}

    session.add.assert_called_once_with(delivery)
    session.commit.assert_awaited_once_with()
    session.refresh.assert_awaited_once_with(delivery)


def test_get_webhook_delivery_returns_existing_delivery() -> None:
    delivery_id = uuid4()

    expected_delivery = WebhookDelivery(
        id=delivery_id,
        target_url="https://example.com/webhooks",
        event_type="order.created",
        payload={"order_id": 123},
    )

    session = MagicMock(spec=AsyncSession)
    session.get = AsyncMock(return_value=expected_delivery)

    delivery = asyncio.run(
        get_webhook_delivery(
            session=session,
            delivery_id=delivery_id,
        )
    )

    assert delivery is expected_delivery
    session.get.assert_awaited_once_with(
        WebhookDelivery,
        delivery_id,
    )


def test_get_webhook_delivery_returns_none_when_not_found() -> None:
    delivery_id = uuid4()

    session = MagicMock(spec=AsyncSession)
    session.get = AsyncMock(return_value=None)

    delivery = asyncio.run(
        get_webhook_delivery(
            session=session,
            delivery_id=delivery_id,
        )
    )

    assert delivery is None
    session.get.assert_awaited_once_with(
        WebhookDelivery,
        delivery_id,
    )
