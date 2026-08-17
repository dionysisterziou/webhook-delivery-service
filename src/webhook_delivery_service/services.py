from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from webhook_delivery_service.models import (
    OutboxMessage,
    WebhookDelivery,
)
from webhook_delivery_service.schemas import WebhookDeliveryCreate


async def create_webhook_delivery(
    session: AsyncSession,
    delivery_data: WebhookDeliveryCreate,
) -> WebhookDelivery:
    delivery_id = uuid4()

    delivery = WebhookDelivery(
        id=delivery_id,
        target_url=str(delivery_data.target_url),
        event_type=delivery_data.event_type,
        payload=delivery_data.payload,
    )

    session.add(delivery)
    await session.flush()

    outbox_message = OutboxMessage(
        delivery_id=delivery_id,
        message_type="webhook.delivery.requested",
        payload={"delivery_id": str(delivery_id)},
    )

    session.add(outbox_message)

    await session.commit()
    await session.refresh(delivery)

    return delivery


async def get_webhook_delivery(
    session: AsyncSession,
    delivery_id: UUID,
) -> WebhookDelivery | None:
    return await session.get(WebhookDelivery, delivery_id)
