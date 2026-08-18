from datetime import UTC, datetime
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from webhook_delivery_service.models import OutboxMessage


class OutboxMessagePublisher(Protocol):
    async def publish(
        self,
        outbox_message: OutboxMessage,
    ) -> None: ...


async def dispatch_outbox_message(
    session: AsyncSession,
    publisher: OutboxMessagePublisher,
    outbox_message: OutboxMessage,
) -> None:
    await publisher.publish(outbox_message)

    outbox_message.published_at = datetime.now(UTC)

    await session.commit()


async def dispatch_unpublished_outbox_messages(
    session: AsyncSession,
    publisher: OutboxMessagePublisher,
    batch_size: int = 100,
) -> int:
    result = await session.scalars(
        select(OutboxMessage)
        .where(OutboxMessage.published_at.is_(None))
        .order_by(OutboxMessage.created_at)
        .limit(batch_size)
    )

    outbox_messages = result.all()

    for outbox_message in outbox_messages:
        await dispatch_outbox_message(
            session=session,
            publisher=publisher,
            outbox_message=outbox_message,
        )

    return len(outbox_messages)
