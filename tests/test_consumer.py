import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from webhook_delivery_service.consumer import (
    WebhookDeliveryNotFoundError,
    handle_delivery_requested_message,
    parse_delivery_requested_message,
)
from webhook_delivery_service.models import WebhookDelivery


def test_parse_delivery_requested_message() -> None:
    message_id = uuid4()
    delivery_id = uuid4()

    message_body = json.dumps(
        {
            "message_id": str(message_id),
            "message_type": "webhook.delivery.requested",
            "payload": {
                "delivery_id": str(delivery_id),
            },
        }
    ).encode("utf-8")

    message = parse_delivery_requested_message(message_body)

    assert message.message_id == message_id
    assert message.message_type == "webhook.delivery.requested"
    assert message.payload.delivery_id == delivery_id


def test_parse_delivery_requested_message_rejects_wrong_type() -> None:
    message_body = json.dumps(
        {
            "message_id": str(uuid4()),
            "message_type": "unknown.message",
            "payload": {
                "delivery_id": str(uuid4()),
            },
        }
    ).encode("utf-8")

    with pytest.raises(ValidationError):
        parse_delivery_requested_message(message_body)


async def run_handle_delivery_requested_message() -> None:
    delivery_id = uuid4()

    delivery = WebhookDelivery(
        id=delivery_id,
        target_url="https://example.com/webhooks",
        event_type="order.created",
        payload={"order_id": 123},
    )

    message_body = json.dumps(
        {
            "message_id": str(uuid4()),
            "message_type": "webhook.delivery.requested",
            "payload": {
                "delivery_id": str(delivery_id),
            },
        }
    ).encode("utf-8")

    session = MagicMock(spec=AsyncSession)
    session.get = AsyncMock(return_value=delivery)

    sender = MagicMock()

    with patch(
        "webhook_delivery_service.consumer.process_webhook_delivery",
        new_callable=AsyncMock,
    ) as process_delivery:
        await handle_delivery_requested_message(
            session=session,
            sender=sender,
            message_body=message_body,
        )

        process_delivery.assert_awaited_once_with(
            session=session,
            sender=sender,
            delivery=delivery,
        )

    session.get.assert_awaited_once_with(
        WebhookDelivery,
        delivery_id,
    )


def test_handle_delivery_requested_message_loads_and_processes_delivery() -> None:
    asyncio.run(run_handle_delivery_requested_message())


async def run_handle_delivery_requested_message_not_found() -> None:
    delivery_id = uuid4()

    message_body = json.dumps(
        {
            "message_id": str(uuid4()),
            "message_type": "webhook.delivery.requested",
            "payload": {
                "delivery_id": str(delivery_id),
            },
        }
    ).encode("utf-8")

    session = MagicMock(spec=AsyncSession)
    session.get = AsyncMock(return_value=None)

    sender = MagicMock()

    with patch(
        "webhook_delivery_service.consumer.process_webhook_delivery",
        new_callable=AsyncMock,
    ) as process_delivery:
        with pytest.raises(
            WebhookDeliveryNotFoundError,
            match=str(delivery_id),
        ):
            await handle_delivery_requested_message(
                session=session,
                sender=sender,
                message_body=message_body,
            )

        process_delivery.assert_not_awaited()

    session.get.assert_awaited_once_with(
        WebhookDelivery,
        delivery_id,
    )


def test_handle_delivery_requested_message_raises_when_delivery_not_found() -> None:
    asyncio.run(run_handle_delivery_requested_message_not_found())
