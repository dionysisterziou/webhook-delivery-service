import hashlib
import json
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from webhook_delivery_service.models import (
    OutboxMessage,
    WebhookDelivery,
)
from webhook_delivery_service.schemas import WebhookDeliveryCreate


class IdempotencyConflictError(Exception):
    """The key already belongs to a different creation request."""


def calculate_request_fingerprint(delivery_data: WebhookDeliveryCreate) -> str:
    canonical_request = json.dumps(
        delivery_data.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()


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

    await _commit_delivery_with_outbox(session, delivery)
    return delivery


async def create_idempotent_webhook_delivery(
    session: AsyncSession,
    delivery_data: WebhookDeliveryCreate,
    idempotency_key: str,
) -> tuple[WebhookDelivery, bool]:
    """Return the delivery and whether this request created it."""
    fingerprint = calculate_request_fingerprint(delivery_data)
    statement = (
        insert(WebhookDelivery)
        .values(
            id=uuid4(),
            target_url=str(delivery_data.target_url),
            event_type=delivery_data.event_type,
            payload=delivery_data.payload,
            idempotency_key=idempotency_key,
            request_fingerprint=fingerprint,
        )
        .on_conflict_do_nothing(index_elements=[WebhookDelivery.idempotency_key])
        .returning(WebhookDelivery)
    )
    # The unique constraint arbitrates concurrent requests for the same key.
    # No outbox is added unless this transaction actually inserts a delivery.
    delivery = await session.scalar(statement)
    if delivery is None:
        # Under PostgreSQL's default READ COMMITTED isolation, this new SELECT
        # sees the competing transaction after ON CONFLICT has waited for it.
        result = await session.scalars(
            select(WebhookDelivery).where(
                WebhookDelivery.idempotency_key == idempotency_key
            )
        )
        existing = result.one()
        if existing.request_fingerprint != fingerprint:
            raise IdempotencyConflictError(
                "Idempotency-Key was already used with a different request"
            )
        # Preserve the current status and attempt history. Closing the request's
        # session releases this read transaction without changing the delivery.
        return existing, False

    await _commit_delivery_with_outbox(session, delivery)
    return delivery, True


async def _commit_delivery_with_outbox(
    session: AsyncSession,
    delivery: WebhookDelivery,
) -> None:
    outbox_message = OutboxMessage(
        delivery_id=delivery.id,
        message_type="webhook.delivery.requested",
        payload={"delivery_id": str(delivery.id)},
    )

    session.add(outbox_message)

    await session.commit()
    await session.refresh(delivery)


async def get_webhook_delivery(
    session: AsyncSession,
    delivery_id: UUID,
) -> WebhookDelivery | None:
    return await session.get(WebhookDelivery, delivery_id)
