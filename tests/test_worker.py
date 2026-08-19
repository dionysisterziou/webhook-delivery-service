import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from webhook_delivery_service.models import WebhookDelivery
from webhook_delivery_service.worker import (
    MAX_DELIVERY_ATTEMPTS,
    WebhookSendError,
    calculate_retry_delay,
    process_webhook_delivery,
)


async def run_successful_delivery_test() -> None:
    session = MagicMock()
    session.commit = AsyncMock()

    sender = MagicMock()
    sender.send = AsyncMock()

    delivery = WebhookDelivery(
        id=uuid4(),
        target_url="https://example.com/webhooks",
        event_type="order.created",
        payload={"order_id": 123},
        status="pending",
        attempt_count=0,
    )

    await process_webhook_delivery(
        session=session,
        sender=sender,
        delivery=delivery,
    )

    sender.send.assert_awaited_once_with(delivery)

    assert delivery.status == "succeeded"
    assert delivery.attempt_count == 1
    assert delivery.delivered_at is not None
    assert delivery.last_error is None
    assert delivery.next_attempt_at is None

    session.commit.assert_awaited_once_with()


def test_process_webhook_delivery_marks_delivery_as_succeeded() -> None:
    asyncio.run(run_successful_delivery_test())


async def run_failed_delivery_test() -> None:
    session = MagicMock()
    session.commit = AsyncMock()

    sender = MagicMock()
    sender.send = AsyncMock(side_effect=WebhookSendError("destination unavailable"))

    delivery = WebhookDelivery(
        id=uuid4(),
        target_url="https://example.com/webhooks",
        event_type="order.created",
        payload={"order_id": 123},
        status="pending",
        attempt_count=0,
    )

    await process_webhook_delivery(
        session=session,
        sender=sender,
        delivery=delivery,
    )

    sender.send.assert_awaited_once_with(delivery)

    assert delivery.status == "retry_scheduled"
    assert delivery.attempt_count == 1
    assert delivery.last_error == "destination unavailable"
    assert delivery.next_attempt_at is not None
    assert delivery.next_attempt_at > datetime.now(UTC)
    assert delivery.delivered_at is None

    session.commit.assert_awaited_once_with()


def test_process_webhook_delivery_schedules_retry_after_failure() -> None:
    asyncio.run(run_failed_delivery_test())


def test_calculate_retry_delay_uses_exponential_backoff_and_jitter() -> None:
    first_retry_seconds = calculate_retry_delay(1).total_seconds()
    second_retry_seconds = calculate_retry_delay(2).total_seconds()
    third_retry_seconds = calculate_retry_delay(3).total_seconds()

    assert 30 <= first_retry_seconds <= 33
    assert 60 <= second_retry_seconds <= 66
    assert 120 <= third_retry_seconds <= 132


async def run_terminal_failure_test() -> None:
    session = MagicMock()
    session.commit = AsyncMock()

    sender = MagicMock()
    sender.send = AsyncMock(side_effect=WebhookSendError("destination unavailable"))

    delivery = WebhookDelivery(
        id=uuid4(),
        target_url="https://example.com/webhooks",
        event_type="order.created",
        payload={"order_id": 123},
        status="retry_scheduled",
        attempt_count=MAX_DELIVERY_ATTEMPTS - 1,
    )

    await process_webhook_delivery(
        session=session,
        sender=sender,
        delivery=delivery,
    )

    sender.send.assert_awaited_once_with(delivery)

    assert delivery.status == "failed"
    assert delivery.attempt_count == MAX_DELIVERY_ATTEMPTS
    assert delivery.last_error == "destination unavailable"
    assert delivery.next_attempt_at is None
    assert delivery.delivered_at is None

    session.commit.assert_awaited_once_with()


def test_process_webhook_delivery_marks_final_attempt_as_failed() -> None:
    asyncio.run(run_terminal_failure_test())


async def run_succeeded_delivery_skip_test() -> None:
    session = MagicMock()
    session.commit = AsyncMock()

    sender = MagicMock()
    sender.send = AsyncMock()

    delivered_at = datetime.now(UTC)

    delivery = WebhookDelivery(
        id=uuid4(),
        target_url="https://example.com/webhooks",
        event_type="order.created",
        payload={"order_id": 123},
        status="succeeded",
        attempt_count=1,
        delivered_at=delivered_at,
    )

    await process_webhook_delivery(
        session=session,
        sender=sender,
        delivery=delivery,
    )

    sender.send.assert_not_awaited()
    session.commit.assert_not_awaited()

    assert delivery.status == "succeeded"
    assert delivery.attempt_count == 1
    assert delivery.delivered_at == delivered_at


def test_process_webhook_delivery_skips_succeeded_delivery() -> None:
    asyncio.run(run_succeeded_delivery_skip_test())
