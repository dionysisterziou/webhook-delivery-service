import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import httpx
import pytest
from aio_pika.abc import AbstractIncomingMessage
from sqlalchemy.ext.asyncio import AsyncSession

from webhook_delivery_service.http_sender import HttpxWebhookSender
from webhook_delivery_service.models import WebhookDelivery
from webhook_delivery_service.rabbitmq_consumer import consume_rabbitmq_delivery_message


async def run_delivery_failure_handling_test(
    status_code: int, expected_status: str
) -> None:
    delivery = WebhookDelivery(
        id=uuid4(),
        target_url="https://example.com/webhooks",
        event_type="order.created",
        payload={"order_id": 123},
        status="pending",
        attempt_count=0,
    )
    session = MagicMock(spec=AsyncSession)
    session.get = AsyncMock(return_value=delivery)
    session.commit = AsyncMock()

    message = MagicMock(spec=AbstractIncomingMessage)
    message.body = json.dumps(
        {
            "message_id": str(uuid4()),
            "message_type": "webhook.delivery.requested",
            "payload": {"delivery_id": str(delivery.id)},
        }
    ).encode("utf-8")

    def acknowledge_after_commit() -> None:
        session.commit.assert_awaited_once_with()
        assert delivery.status == expected_status

    message.ack = AsyncMock(side_effect=acknowledge_after_commit)
    message.nack = AsyncMock()
    message.reject = AsyncMock()

    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status_code=status_code)

    # Exercise the real handler, worker and sender; only external I/O is replaced.
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await consume_rabbitmq_delivery_message(
            message=message,
            session=session,
            sender=HttpxWebhookSender(client),
        )

    assert len(requests) == 1
    session.get.assert_awaited_once_with(
        WebhookDelivery, delivery.id, with_for_update=True
    )
    message.ack.assert_awaited_once_with()
    message.nack.assert_not_awaited()
    message.reject.assert_not_awaited()
    assert delivery.attempt_count == 1
    assert delivery.delivered_at is None
    assert delivery.last_error is not None
    assert str(status_code) in delivery.last_error
    if expected_status == "retry_scheduled":
        assert delivery.next_attempt_at is not None
        assert delivery.next_attempt_at > datetime.now(UTC)
    else:
        assert delivery.next_attempt_at is None


@pytest.mark.parametrize(
    ("status_code", "expected_status"),
    [(400, "failed"), (503, "retry_scheduled")],
)
def test_consumer_acknowledges_recorded_http_failure(
    status_code: int, expected_status: str
) -> None:
    asyncio.run(run_delivery_failure_handling_test(status_code, expected_status))
