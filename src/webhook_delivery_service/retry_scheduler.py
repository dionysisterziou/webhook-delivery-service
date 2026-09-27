from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from webhook_delivery_service.models import OutboxMessage, WebhookDelivery


async def schedule_due_webhook_retries(
    session: AsyncSession,
    batch_size: int = 100,
) -> int:
    now = datetime.now(UTC)

    result = await session.scalars(
        select(WebhookDelivery)
        .where(
            WebhookDelivery.status == "retry_scheduled",
            WebhookDelivery.next_attempt_at <= now,
        )
        .order_by(
            WebhookDelivery.next_attempt_at,
            WebhookDelivery.id,
        )
        .limit(batch_size)
        .with_for_update(skip_locked=True)
    )
    deliveries = result.all()

    for delivery in deliveries:
        outbox_message = OutboxMessage(
            delivery_id=delivery.id,
            message_type="webhook.delivery.requested",
            payload={"delivery_id": str(delivery.id)},
        )
        session.add(outbox_message)

        delivery.status = "pending"
        delivery.next_attempt_at = None

    await session.commit()

    return len(deliveries)
