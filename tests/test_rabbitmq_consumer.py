import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from aio_pika.abc import AbstractIncomingMessage
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from webhook_delivery_service.models import WebhookDelivery
from webhook_delivery_service.rabbitmq_consumer import (
    consume_rabbitmq_delivery_message,
)


async def run_consume_rabbitmq_delivery_message_success() -> None:
    message_body = b"test-message-body"

    message = MagicMock(spec=AbstractIncomingMessage)
    message.body = message_body
    message.ack = AsyncMock()

    session = MagicMock(spec=AsyncSession)
    sender = MagicMock()

    with patch(
        "webhook_delivery_service.rabbitmq_consumer."
        "handle_delivery_requested_message",
        new_callable=AsyncMock,
    ) as handle_message:
        await consume_rabbitmq_delivery_message(
            message=message,
            session=session,
            sender=sender,
        )

        handle_message.assert_awaited_once_with(
            session=session,
            sender=sender,
            message_body=message_body,
        )

    message.ack.assert_awaited_once_with()


def test_consume_rabbitmq_delivery_message_acknowledges_success() -> None:
    asyncio.run(run_consume_rabbitmq_delivery_message_success())


async def run_consume_rabbitmq_delivery_message_invalid_message() -> None:
    message = MagicMock(spec=AbstractIncomingMessage)
    message.body = b"not-json"
    message.ack = AsyncMock()
    message.reject = AsyncMock()

    session = MagicMock(spec=AsyncSession)
    session.get = AsyncMock()

    sender = MagicMock()

    await consume_rabbitmq_delivery_message(
        message=message,
        session=session,
        sender=sender,
    )

    message.reject.assert_awaited_once_with(requeue=False)
    message.ack.assert_not_awaited()
    session.get.assert_not_awaited()


def test_consume_rabbitmq_delivery_message_rejects_invalid_message() -> None:
    asyncio.run(run_consume_rabbitmq_delivery_message_invalid_message())


async def run_consume_rabbitmq_delivery_message_missing_delivery() -> None:
    delivery_id = uuid4()

    message = MagicMock(spec=AbstractIncomingMessage)
    message.body = json.dumps(
        {
            "message_id": str(uuid4()),
            "message_type": "webhook.delivery.requested",
            "payload": {
                "delivery_id": str(delivery_id),
            },
        }
    ).encode("utf-8")
    message.ack = AsyncMock()
    message.reject = AsyncMock()

    session = MagicMock(spec=AsyncSession)
    session.get = AsyncMock(return_value=None)

    sender = MagicMock()

    await consume_rabbitmq_delivery_message(
        message=message,
        session=session,
        sender=sender,
    )

    session.get.assert_awaited_once_with(
        WebhookDelivery,
        delivery_id,
    )
    message.reject.assert_awaited_once_with(requeue=False)
    message.ack.assert_not_awaited()


def test_consume_rabbitmq_delivery_message_rejects_missing_delivery() -> None:
    asyncio.run(run_consume_rabbitmq_delivery_message_missing_delivery())


async def run_consume_rabbitmq_delivery_message_temporary_error() -> None:
    message_body = b"test-message-body"

    message = MagicMock(spec=AbstractIncomingMessage)
    message.body = message_body
    message.ack = AsyncMock()
    message.reject = AsyncMock()
    message.nack = AsyncMock()

    session = MagicMock(spec=AsyncSession)
    sender = MagicMock()

    with patch(
        "webhook_delivery_service.rabbitmq_consumer."
        "handle_delivery_requested_message",
        new_callable=AsyncMock,
    ) as handle_message:
        handle_message.side_effect = SQLAlchemyError("temporary database error")

        await consume_rabbitmq_delivery_message(
            message=message,
            session=session,
            sender=sender,
        )

        handle_message.assert_awaited_once_with(
            session=session,
            sender=sender,
            message_body=message_body,
        )

    message.nack.assert_awaited_once_with(requeue=True)
    message.ack.assert_not_awaited()
    message.reject.assert_not_awaited()


def test_consume_rabbitmq_delivery_message_requeues_temporary_error() -> None:
    asyncio.run(run_consume_rabbitmq_delivery_message_temporary_error())
