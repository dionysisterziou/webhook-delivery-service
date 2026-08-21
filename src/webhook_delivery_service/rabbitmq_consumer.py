from aio_pika.abc import AbstractIncomingMessage
from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from webhook_delivery_service.consumer import (
    WebhookDeliveryNotFoundError,
    handle_delivery_requested_message,
)
from webhook_delivery_service.worker import WebhookSender


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
