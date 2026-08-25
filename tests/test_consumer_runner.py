import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy.ext.asyncio import AsyncEngine

from webhook_delivery_service.config import Settings
from webhook_delivery_service.consumer_runner import run_consumer_once
from webhook_delivery_service.rabbitmq_consumer import (
    RabbitMQDeliveryConsumer,
)


async def run_consumer_once_with_empty_queue() -> None:
    settings = MagicMock(spec=Settings)

    consumer = MagicMock(spec=RabbitMQDeliveryConsumer)
    consumer.get_message = AsyncMock(return_value=None)
    consumer.close = AsyncMock()

    consumer_class = MagicMock(spec=RabbitMQDeliveryConsumer)
    consumer_class.connect = AsyncMock(return_value=consumer)

    engine = MagicMock(spec=AsyncEngine)
    engine.dispose = AsyncMock()

    with (
        patch(
            "webhook_delivery_service.consumer_runner.Settings",
            return_value=settings,
        ) as settings_factory,
        patch(
            "webhook_delivery_service.consumer_runner.RabbitMQDeliveryConsumer",
            consumer_class,
        ),
        patch(
            "webhook_delivery_service.consumer_runner.engine",
            engine,
        ),
    ):
        processed_count = await run_consumer_once()

    assert processed_count == 0
    settings_factory.assert_called_once_with()
    consumer_class.connect.assert_awaited_once_with(settings)
    consumer.get_message.assert_awaited_once_with()
    consumer.close.assert_awaited_once_with()
    engine.dispose.assert_awaited_once_with()


def test_consumer_runner_returns_zero_for_empty_queue() -> None:
    asyncio.run(run_consumer_once_with_empty_queue())


async def run_consumer_once_with_message() -> None:
    settings = MagicMock(spec=Settings)

    message = MagicMock()

    consumer = MagicMock(spec=RabbitMQDeliveryConsumer)
    consumer.get_message = AsyncMock(return_value=message)
    consumer.close = AsyncMock()

    consumer_class = MagicMock(spec=RabbitMQDeliveryConsumer)
    consumer_class.connect = AsyncMock(return_value=consumer)

    http_client = MagicMock()
    http_client.__aenter__ = AsyncMock(return_value=http_client)
    http_client.__aexit__ = AsyncMock(return_value=None)

    sender = MagicMock()

    session = MagicMock()
    session_context = MagicMock()
    session_context.__aenter__ = AsyncMock(return_value=session)
    session_context.__aexit__ = AsyncMock(return_value=None)

    engine = MagicMock(spec=AsyncEngine)
    engine.dispose = AsyncMock()

    with (
        patch(
            "webhook_delivery_service.consumer_runner.Settings",
            return_value=settings,
        ) as settings_factory,
        patch(
            "webhook_delivery_service.consumer_runner.RabbitMQDeliveryConsumer",
            consumer_class,
        ),
        patch(
            "webhook_delivery_service.consumer_runner.httpx.AsyncClient",
            return_value=http_client,
        ) as http_client_factory,
        patch(
            "webhook_delivery_service.consumer_runner.HttpxWebhookSender",
            return_value=sender,
        ) as sender_factory,
        patch(
            "webhook_delivery_service.consumer_runner.async_session_factory",
            return_value=session_context,
        ) as session_factory,
        patch(
            "webhook_delivery_service.consumer_runner.consume_rabbitmq_delivery_message",
            new_callable=AsyncMock,
        ) as consume_message,
        patch(
            "webhook_delivery_service.consumer_runner.engine",
            engine,
        ),
    ):
        processed_count = await run_consumer_once()

    assert processed_count == 1

    settings_factory.assert_called_once_with()
    consumer_class.connect.assert_awaited_once_with(settings)
    consumer.get_message.assert_awaited_once_with()

    http_client_factory.assert_called_once_with()
    sender_factory.assert_called_once_with(http_client)
    session_factory.assert_called_once_with()

    consume_message.assert_awaited_once_with(
        message=message,
        session=session,
        sender=sender,
    )

    session_context.__aexit__.assert_awaited_once()
    http_client.__aexit__.assert_awaited_once()
    consumer.close.assert_awaited_once_with()
    engine.dispose.assert_awaited_once_with()


def test_consumer_runner_processes_one_message() -> None:
    asyncio.run(run_consumer_once_with_message())
