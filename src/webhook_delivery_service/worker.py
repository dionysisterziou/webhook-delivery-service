from datetime import UTC, datetime, timedelta
from random import uniform
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from webhook_delivery_service.models import WebhookDelivery

BASE_RETRY_DELAY_SECONDS = 30
MAX_RETRY_DELAY_SECONDS = 3600
JITTER_RATIO = 0.1
MAX_DELIVERY_ATTEMPTS = 5


class WebhookSendError(Exception):
    pass


class WebhookSender(Protocol):
    async def send(
        self,
        delivery: WebhookDelivery,
    ) -> None: ...


def calculate_retry_delay(attempt_count: int) -> timedelta:
    if attempt_count < 1:
        raise ValueError("attempt_count must be at least 1")

    exponential_delay_seconds = BASE_RETRY_DELAY_SECONDS * 2 ** (attempt_count - 1)

    capped_delay_seconds = min(
        exponential_delay_seconds,
        MAX_RETRY_DELAY_SECONDS,
    )

    jitter_seconds = uniform(
        0,
        capped_delay_seconds * JITTER_RATIO,
    )

    return timedelta(seconds=capped_delay_seconds + jitter_seconds)


async def process_webhook_delivery(
    session: AsyncSession,
    sender: WebhookSender,
    delivery: WebhookDelivery,
) -> None:
    if delivery.status in {"succeeded", "failed"}:
        return

    delivery.status = "processing"
    delivery.attempt_count += 1

    try:
        await sender.send(delivery)
    except WebhookSendError as error:
        delivery.last_error = str(error)

        if delivery.attempt_count >= MAX_DELIVERY_ATTEMPTS:
            delivery.status = "failed"
            delivery.next_attempt_at = None
        else:
            delivery.status = "retry_scheduled"
            delivery.next_attempt_at = datetime.now(UTC) + calculate_retry_delay(
                delivery.attempt_count
            )

        await session.commit()
        return

    delivery.status = "succeeded"
    delivery.delivered_at = datetime.now(UTC)
    delivery.last_error = None
    delivery.next_attempt_at = None

    await session.commit()
