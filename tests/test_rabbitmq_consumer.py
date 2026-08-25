import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from aio_pika.abc import (
    AbstractIncomingMessage,
    AbstractQueue,
    AbstractRobustConnection,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from webhook_delivery_service.models import WebhookDelivery
from webhook_delivery_service.rabbitmq_consumer import (
    RabbitMQDeliveryConsumer,
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


async def run_rabbitmq_delivery_consumer_get_message() -> None:
    connection = MagicMock(spec=AbstractRobustConnection)

    message = MagicMock(spec=AbstractIncomingMessage)

    queue = MagicMock(spec=AbstractQueue)
    queue.get = AsyncMock(return_value=message)

    consumer = RabbitMQDeliveryConsumer(
        connection=connection,
        queue=queue,
    )

    received_message = await consumer.get_message()

    assert received_message is message
    queue.get.assert_awaited_once_with(
        no_ack=False,
        fail=False,
    )


def test_rabbitmq_delivery_consumer_gets_one_message() -> None:
    asyncio.run(run_rabbitmq_delivery_consumer_get_message())


async def run_rabbitmq_delivery_consumer_empty_queue() -> None:
    connection = MagicMock(spec=AbstractRobustConnection)
    queue = MagicMock(spec=AbstractQueue)
    queue.get = AsyncMock(return_value=None)

    consumer = RabbitMQDeliveryConsumer(
        connection=connection,
        queue=queue,
    )

    received_message = await consumer.get_message()

    assert received_message is None
    queue.get.assert_awaited_once_with(
        no_ack=False,
        fail=False,
    )


def test_rabbitmq_delivery_consumer_returns_none_when_queue_is_empty() -> None:
    asyncio.run(run_rabbitmq_delivery_consumer_empty_queue())


async def run_rabbitmq_delivery_consumer_close() -> None:
    connection = MagicMock(spec=AbstractRobustConnection)
    connection.close = AsyncMock()
    queue = MagicMock(spec=AbstractQueue)

    consumer = RabbitMQDeliveryConsumer(
        connection=connection,
        queue=queue,
    )

    await consumer.close()

    connection.close.assert_awaited_once_with()


def test_rabbitmq_delivery_consumer_closes_connection() -> None:
    asyncio.run(run_rabbitmq_delivery_consumer_close())
