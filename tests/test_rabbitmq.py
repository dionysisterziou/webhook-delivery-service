import asyncio
import json
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from aio_pika import DeliveryMode

from webhook_delivery_service.models import OutboxMessage
from webhook_delivery_service.rabbitmq import RabbitMQOutboxPublisher


async def run_publish_test() -> None:
    exchange = MagicMock()
    exchange.publish = AsyncMock()

    channel = MagicMock()
    channel.default_exchange = exchange

    message_id = uuid4()
    delivery_id = uuid4()

    outbox_message = OutboxMessage(
        id=message_id,
        delivery_id=delivery_id,
        message_type="webhook.delivery.requested",
        payload={"delivery_id": str(delivery_id)},
    )

    publisher = RabbitMQOutboxPublisher(
        connection=MagicMock(),
        channel=channel,
        queue_name="webhook.deliveries",
    )

    await publisher.publish(outbox_message)

    exchange.publish.assert_awaited_once()

    publish_call = exchange.publish.await_args
    assert publish_call is not None

    published_message = publish_call.args[0]

    assert json.loads(published_message.body.decode("utf-8")) == {
        "message_id": str(message_id),
        "message_type": "webhook.delivery.requested",
        "payload": {"delivery_id": str(delivery_id)},
    }
    assert published_message.content_type == "application/json"
    assert published_message.delivery_mode == DeliveryMode.PERSISTENT
    assert published_message.message_id == str(message_id)
    assert publish_call.kwargs == {
        "routing_key": "webhook.deliveries",
        "mandatory": True,
    }


def test_publish_sends_persistent_json_message() -> None:
    asyncio.run(run_publish_test())
