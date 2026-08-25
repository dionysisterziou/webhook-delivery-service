from aio_pika import connect_robust
from aio_pika.abc import (
    AbstractIncomingMessage,
    AbstractQueue,
    AbstractRobustConnection,
)
from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from webhook_delivery_service.config import Settings
from webhook_delivery_service.consumer import (
    WebhookDeliveryNotFoundError,
    handle_delivery_requested_message,
)
from webhook_delivery_service.worker import WebhookSender


class RabbitMQDeliveryConsumer:
    def __init__(
        self,
        connection: AbstractRobustConnection,
        queue: AbstractQueue,
    ) -> None:
        self._connection = connection
        self._queue = queue

    @classmethod
    async def connect(
        cls,
        settings: Settings,
    ) -> RabbitMQDeliveryConsumer:
        connection = await connect_robust(
            host=settings.rabbitmq_host,
            port=settings.rabbitmq_port,
            login=settings.rabbitmq_default_user,
            password=settings.rabbitmq_default_pass.get_secret_value(),
            virtualhost=settings.rabbitmq_default_vhost,
        )

        channel = await connection.channel()

        queue = await channel.declare_queue(
            settings.rabbitmq_delivery_queue,
            durable=True,
        )

        return cls(
            connection=connection,
            queue=queue,
        )

    async def get_message(self) -> AbstractIncomingMessage | None:
        return await self._queue.get(
            no_ack=False,
            fail=False,
        )

    async def close(self) -> None:
        await self._connection.close()


async def consume_rabbitmq_delivery_message(
    message: AbstractIncomingMessage,
    session: AsyncSession,
    sender: WebhookSender,
) -> None:
    try:
        await handle_delivery_requested_message(
            session=session,
            sender=sender,
            message_body=message.body,
        )
    except ValidationError, WebhookDeliveryNotFoundError:
        await message.reject(requeue=False)
        return
    except SQLAlchemyError:
        await message.nack(requeue=True)
        return

    await message.ack()
