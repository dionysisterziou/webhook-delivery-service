from typing import Literal
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from webhook_delivery_service.models import WebhookDelivery
from webhook_delivery_service.worker import (
    WebhookSender,
    process_webhook_delivery,
)


class DeliveryRequestedPayload(BaseModel):
    delivery_id: UUID


class DeliveryRequestedMessage(BaseModel):
    message_id: UUID
    message_type: Literal["webhook.delivery.requested"]
    payload: DeliveryRequestedPayload


def parse_delivery_requested_message(
    message_body: bytes,
) -> DeliveryRequestedMessage:
    return DeliveryRequestedMessage.model_validate_json(message_body)


class WebhookDeliveryNotFoundError(Exception):
    pass


async def handle_delivery_requested_message(
    session: AsyncSession,
    sender: WebhookSender,
    message_body: bytes,
) -> None:
    message = parse_delivery_requested_message(message_body)

    delivery = await session.get(
        WebhookDelivery,
        message.payload.delivery_id,
    )

    if delivery is None:
        raise WebhookDeliveryNotFoundError(
            f"webhook delivery {message.payload.delivery_id} was not found"
        )

    await process_webhook_delivery(
        session=session,
        sender=sender,
        delivery=delivery,
    )
