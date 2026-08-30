import asyncio
from unittest.mock import AsyncMock, MagicMock, call, patch

from aio_pika.abc import AbstractQueueIterator
from sqlalchemy.ext.asyncio import AsyncEngine

from webhook_delivery_service.config import Settings
from webhook_delivery_service.consumer_runner import main, run_consumer
from webhook_delivery_service.rabbitmq_consumer import (
    RabbitMQDeliveryConsumer,
)


async def run_consumer_with_multiple_messages() -> None:
    settings = MagicMock(spec=Settings)

    message_1 = MagicMock()
    message_2 = MagicMock()

    message_iterator = MagicMock(spec=AbstractQueueIterator)
    message_iterator.__aenter__ = AsyncMock(return_value=message_iterator)
    message_iterator.__aexit__ = AsyncMock(return_value=None)
    message_iterator.__aiter__.return_value = [message_1, message_2]

    consumer = MagicMock(spec=RabbitMQDeliveryConsumer)
    consumer.iter_messages = MagicMock(return_value=message_iterator)
    consumer.close = AsyncMock()

    consumer_class = MagicMock(spec=RabbitMQDeliveryConsumer)
    consumer_class.connect = AsyncMock(return_value=consumer)

    http_client = MagicMock()
    http_client.__aenter__ = AsyncMock(return_value=http_client)
    http_client.__aexit__ = AsyncMock(return_value=None)

    sender = MagicMock()

    session_1 = MagicMock()
    session_1.__aenter__ = AsyncMock(return_value=session_1)
    session_1.__aexit__ = AsyncMock(return_value=None)

    session_2 = MagicMock()
    session_2.__aenter__ = AsyncMock(return_value=session_2)
    session_2.__aexit__ = AsyncMock(return_value=None)

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
            side_effect=[session_1, session_2],
        ) as session_factory,
        patch(
            "webhook_delivery_service.consumer_runner."
            "consume_rabbitmq_delivery_message",
            new_callable=AsyncMock,
        ) as consume_message,
        patch(
            "webhook_delivery_service.consumer_runner.engine",
            engine,
        ),
    ):
        await run_consumer()

    settings_factory.assert_called_once_with()

    consumer_class.connect.assert_awaited_once_with(settings)

    http_client_factory.assert_called_once_with()
    http_client.__aenter__.assert_awaited_once_with()

    sender_factory.assert_called_once_with(http_client)

    consumer.iter_messages.assert_called_once_with()
    message_iterator.__aenter__.assert_awaited_once_with()
    message_iterator.__aiter__.assert_called_once_with()

    assert session_factory.call_count == 2

    session_1.__aenter__.assert_awaited_once_with()
    session_2.__aenter__.assert_awaited_once_with()

    consume_message.assert_has_awaits(
        [
            call(
                message=message_1,
                session=session_1,
                sender=sender,
            ),
            call(
                message=message_2,
                session=session_2,
                sender=sender,
            ),
        ]
    )
    assert consume_message.await_count == 2

    session_1.__aexit__.assert_awaited_once()
    session_2.__aexit__.assert_awaited_once()

    message_iterator.__aexit__.assert_awaited_once()
    http_client.__aexit__.assert_awaited_once()

    consumer.close.assert_awaited_once_with()
    engine.dispose.assert_awaited_once_with()


def test_consumer_runner_processes_multiple_messages() -> None:
    asyncio.run(run_consumer_with_multiple_messages())


def test_consumer_runner_stops_cleanly_on_keyboard_interrupt() -> None:
    consumer_coroutine = MagicMock()
    run_consumer_mock = MagicMock(return_value=consumer_coroutine)

    with (
        patch(
            "webhook_delivery_service.consumer_runner.sys.platform",
            "win32",
        ),
        patch(
            "webhook_delivery_service.consumer_runner.run_consumer",
            run_consumer_mock,
        ),
        patch(
            "webhook_delivery_service.consumer_runner.asyncio.run",
            side_effect=KeyboardInterrupt,
        ) as asyncio_run_mock,
        patch("builtins.print") as print_mock,
    ):
        main()

    run_consumer_mock.assert_called_once_with()
    asyncio_run_mock.assert_called_once_with(
        consumer_coroutine,
        loop_factory=asyncio.SelectorEventLoop,
    )
    print_mock.assert_called_once_with("Consumer stopped.")
