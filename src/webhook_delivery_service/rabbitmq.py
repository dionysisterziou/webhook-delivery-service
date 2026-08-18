import json

from aio_pika import DeliveryMode, Message, connect_robust
from aio_pika.abc import AbstractChannel, AbstractRobustConnection

from webhook_delivery_service.config import Settings
from webhook_delivery_service.models import OutboxMessage


class RabbitMQOutboxPublisher:
    def __init__(
        self,
        connection: AbstractRobustConnection,
        channel: AbstractChannel,
        queue_name: str,
    ) -> None:
        self._connection = connection
        self._channel = channel
        self._queue_name = queue_name

    @classmethod
    async def connect(
        cls,
        settings: Settings,
    ) -> "RabbitMQOutboxPublisher":
        connection = await connect_robust(
            host=settings.rabbitmq_host,
            port=settings.rabbitmq_port,
            login=settings.rabbitmq_default_user,
            password=settings.rabbitmq_default_pass.get_secret_value(),
            virtualhost=settings.rabbitmq_default_vhost,
        )

        channel = await connection.channel(
            publisher_confirms=True,
            on_return_raises=True,
        )

        await channel.declare_queue(
            settings.rabbitmq_delivery_queue,
            durable=True,
        )

        return cls(
            connection=connection,
            channel=channel,
            queue_name=settings.rabbitmq_delivery_queue,
        )

    async def publish(
        self,
        outbox_message: OutboxMessage,
    ) -> None:
        body = json.dumps(
            {
                "message_id": str(outbox_message.id),
                "message_type": outbox_message.message_type,
                "payload": outbox_message.payload,
            }
        ).encode("utf-8")

        message = Message(
            body=body,
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=str(outbox_message.id),
            type=outbox_message.message_type,
        )

        await self._channel.default_exchange.publish(
            message,
            routing_key=self._queue_name,
            mandatory=True,
        )

    async def close(self) -> None:
        await self._connection.close()
