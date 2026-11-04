import os
from datetime import UTC, datetime

import pytest

from postgres_api import isolated_api, run_check
from webhook_delivery_service.models import OutboxMessage, WebhookDelivery

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_DATABASE_INTEGRATION_TESTS") != "1",
    reason="database integration tests are disabled",
)


async def check_metrics_follow_stored_state(monkeypatch: pytest.MonkeyPatch) -> None:
    zero_counts = {
        "pending": 0,
        "processing": 0,
        "retry_scheduled": 0,
        "succeeded": 0,
        "failed": 0,
    }
    async with isolated_api(monkeypatch) as (client, sessions):
        empty = await client.get("/metrics")
        assert empty.status_code == 200, empty.text
        assert empty.json() == {
            "deliveries_by_status": zero_counts,
            "unpublished_outbox_messages": 0,
        }

        async with sessions() as session:
            delivery = WebhookDelivery(
                target_url="https://example.com/webhooks",
                event_type="order.created",
                payload={},
                status="retry_scheduled",
                attempt_count=1,
            )
            session.add(delivery)
            await session.flush()
            messages = [
                OutboxMessage(
                    delivery_id=delivery.id,
                    message_type="webhook.delivery.requested",
                    payload={"delivery_id": str(delivery.id)},
                    published_at=published_at,
                )
                for published_at in (datetime.now(UTC), None)
            ]
            session.add_all(messages)
            await session.commit()

            retrying = await client.get("/metrics")
            assert retrying.status_code == 200, retrying.text
            assert retrying.json() == {
                "deliveries_by_status": zero_counts | {"retry_scheduled": 1},
                "unpublished_outbox_messages": 1,
            }

            delivery.status = "succeeded"
            delivery.attempt_count = 2
            messages[1].published_at = datetime.now(UTC)
            await session.commit()

        succeeded = await client.get("/metrics")
        assert succeeded.status_code == 200, succeeded.text
        metrics = succeeded.json()
        assert metrics["deliveries_by_status"] == zero_counts | {"succeeded": 1}
        assert sum(metrics["deliveries_by_status"].values()) == 1
        assert metrics["unpublished_outbox_messages"] == 0


def test_metrics_follow_stored_state(monkeypatch: pytest.MonkeyPatch) -> None:
    run_check(check_metrics_follow_stored_state(monkeypatch))
