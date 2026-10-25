import asyncio
import json
import os
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from urllib.parse import quote
from uuid import uuid4

import httpx
import pytest
from aio_pika import DeliveryMode, Message, connect_robust
from aio_pika.abc import AbstractIncomingMessage
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from webhook_delivery_service.config import Settings
from webhook_delivery_service.models import OutboxMessage, WebhookDelivery
from webhook_delivery_service.rabbitmq import RabbitMQOutboxPublisher
from webhook_delivery_service.rabbitmq_consumer import (
    RabbitMQDeliveryConsumer,
    consume_rabbitmq_delivery_message,
)
from webhook_delivery_service.rabbitmq_setup import (
    configure_dead_lettering,
    create_management_client,
)
from webhook_delivery_service.worker import WebhookSendError

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_RABBITMQ_INTEGRATION_TESTS") != "1",
    reason="RabbitMQ integration tests are disabled",
)


@asynccontextmanager
async def isolated_broker() -> AsyncGenerator[tuple[Settings, httpx.AsyncClient]]:
    # Never consume from or delete the application's real delivery/DLQ queues.
    queue_name = f"test.webhook.dlq.{uuid4().hex}"
    settings = Settings().model_copy(update={"rabbitmq_delivery_queue": queue_name})
    vhost = quote(settings.rabbitmq_default_vhost, safe="")

    async with create_management_client(settings) as client:
        try:
            await configure_dead_lettering(
                client,
                virtual_host=settings.rabbitmq_default_vhost,
                queue_name=queue_name,
            )
            yield settings, client
        finally:
            # Also clean partially created test resources if setup fails.
            failures: list[Exception] = []
            for kind, name in (
                ("policies", f"{queue_name}.dead-letter"),
                ("queues", queue_name),
                ("queues", f"{queue_name}.dlq"),
                ("exchanges", f"{queue_name}.dlx"),
            ):
                try:
                    response = await client.delete(
                        f"/api/{kind}/{vhost}/{quote(name, safe='')}"
                    )
                    if response.status_code != 404:
                        response.raise_for_status()
                except httpx.HTTPError as error:
                    failures.append(error)
            if failures:
                raise ExceptionGroup("Could not clean RabbitMQ test resources", failures)


async def wait_for_message(
    receive: Callable[[], Awaitable[AbstractIncomingMessage | None]],
) -> AbstractIncomingMessage:
    async with asyncio.timeout(10):
        while True:
            message = await receive()
            if message is not None:
                return message
            await asyncio.sleep(0.05)


async def check_dead_lettering(scenario: str) -> None:
    async with isolated_broker() as (settings, client), AsyncExitStack() as stack:
        connection = await connect_robust(
            host=settings.rabbitmq_host,
            port=settings.rabbitmq_port,
            login=settings.rabbitmq_default_user,
            password=settings.rabbitmq_default_pass.get_secret_value(),
            virtualhost=settings.rabbitmq_default_vhost,
            timeout=10,
        )
        stack.push_async_callback(connection.close)
        channel = await connection.channel(publisher_confirms=True, on_return_raises=True)
        queue_name = settings.rabbitmq_delivery_queue
        dead_queue = await channel.declare_queue(f"{queue_name}.dlq", durable=True)

        publisher = await RabbitMQOutboxPublisher.connect(settings)
        stack.push_async_callback(publisher.close)
        consumer = await RabbitMQDeliveryConsumer.connect(settings)
        stack.push_async_callback(consumer.close)

        delivery = WebhookDelivery(
            id=uuid4(),
            target_url="https://example.com/webhooks/test",
            event_type="integration.dead-letter",
            payload={"source": "dead-letter-test"},
            status="pending",
            attempt_count=0,
        )
        outbox = OutboxMessage(
            id=uuid4(),
            delivery_id=delivery.id,
            message_type="webhook.delivery.requested",
            payload={"delivery_id": str(delivery.id)},
        )
        expected_body = json.dumps(
            {
                "message_id": str(outbox.id),
                "message_type": outbox.message_type,
                "payload": outbox.payload,
            }
        ).encode("utf-8")
        if scenario == "invalid-json":
            expected_body = b"not-json"
            await channel.default_exchange.publish(
                Message(
                    body=expected_body,
                    message_id=str(outbox.id),
                    delivery_mode=DeliveryMode.PERSISTENT,
                ),
                routing_key=queue_name,
                mandatory=True,
            )
        else:
            await publisher.publish(outbox)

        if scenario == "success":
            # Re-running setup must retain a message already waiting in the queue.
            await configure_dead_lettering(
                client,
                virtual_host=settings.rabbitmq_default_vhost,
                queue_name=queue_name,
            )

        # The broker, messages and acknowledgements are real. Only the database
        # session and HTTP sender are replaced; PostgreSQL/HTTP are not under test.
        session = MagicMock(spec=AsyncSession)
        session.get = AsyncMock(
            return_value=None if scenario == "missing-delivery" else delivery
        )
        session.commit = AsyncMock()
        sender = MagicMock()
        sender.send = AsyncMock()
        if scenario == "permanent-failure":
            sender.send.side_effect = WebhookSendError("HTTP 400", retryable=False)
        elif scenario == "retryable-failure":
            sender.send.side_effect = WebhookSendError("HTTP 503", retryable=True)
        elif scenario == "database-error":
            session.get.side_effect = SQLAlchemyError("temporary database error")

        message = await wait_for_message(consumer.get_message)
        assert message.body == expected_body
        await consume_rabbitmq_delivery_message(message, session, sender)

        if scenario in {"invalid-json", "missing-delivery"}:
            async def receive_dead_letter() -> AbstractIncomingMessage | None:
                return await dead_queue.get(no_ack=False, fail=False)

            dead_letter = await wait_for_message(receive_dead_letter)
            assert dead_letter.body == expected_body
            assert dead_letter.message_id == str(outbox.id)
            deaths = dead_letter.headers["x-death"]
            assert isinstance(deaths, list)
            assert any(
                isinstance(entry, dict)
                and entry["queue"] == queue_name
                and entry["reason"] == "rejected"
                and entry["count"] == 1
                for entry in deaths
            )
            await dead_letter.ack()
            sender.send.assert_not_awaited()
            session.commit.assert_not_awaited()
        elif scenario == "database-error":
            redelivered = await wait_for_message(consumer.get_message)
            assert redelivered.body == expected_body
            assert redelivered.redelivered is True
            await redelivered.ack()
            sender.send.assert_not_awaited()
            session.commit.assert_not_awaited()
        else:
            sender.send.assert_awaited_once_with(delivery)
            session.commit.assert_awaited_once_with()
            assert delivery.attempt_count == 1
            if scenario == "success":
                assert delivery.status == "succeeded"
                assert delivery.delivered_at is not None
                assert delivery.last_error is None
                assert delivery.next_attempt_at is None
            elif scenario == "permanent-failure":
                assert delivery.status == "failed"
                assert delivery.next_attempt_at is None
            else:
                assert delivery.status == "retry_scheduled"
                assert delivery.next_attempt_at is not None
                assert delivery.next_attempt_at > datetime.now(UTC)

        if scenario == "invalid-json":
            session.get.assert_not_awaited()
        else:
            session.get.assert_awaited_once_with(
                WebhookDelivery, delivery.id, with_for_update=True
            )

        assert await consumer.get_message() is None
        assert await dead_queue.get(no_ack=False, fail=False) is None


@pytest.mark.parametrize(
    "scenario",
    [
        "invalid-json",
        "missing-delivery",
        "success",
        "permanent-failure",
        "retryable-failure",
        "database-error",
    ],
)
def test_rabbitmq_dead_lettering(scenario: str) -> None:
    asyncio.run(check_dead_lettering(scenario))
