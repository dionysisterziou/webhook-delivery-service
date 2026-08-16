from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from webhook_delivery_service.models import WebhookDelivery
from webhook_delivery_service.schemas import WebhookDeliveryCreate


async def create_webhook_delivery(
    session: AsyncSession,
    delivery_data: WebhookDeliveryCreate,
) -> WebhookDelivery:
    delivery = WebhookDelivery(
        target_url=str(delivery_data.target_url),
        event_type=delivery_data.event_type,
        payload=delivery_data.payload,
    )

    session.add(delivery)
    await session.commit()
    await session.refresh(delivery)

    return delivery


async def get_webhook_delivery(
    session: AsyncSession,
    delivery_id: UUID,
) -> WebhookDelivery | None:
    return await session.get(WebhookDelivery, delivery_id)
