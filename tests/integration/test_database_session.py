import asyncio
import os
import sys
from uuid import UUID

import pytest

RUN_DATABASE_INTEGRATION_TESTS = os.getenv("RUN_DATABASE_INTEGRATION_TESTS") == "1"


async def run_webhook_delivery_round_trip() -> None:
    from sqlalchemy import delete

    from webhook_delivery_service.database import async_session_factory, engine
    from webhook_delivery_service.models import WebhookDelivery

    delivery_id: UUID | None = None

    try:
        async with async_session_factory() as write_session:
            delivery = WebhookDelivery(
                target_url="https://example.com/webhooks/test",
                event_type="integration.test",
                payload={"message": "database round trip"},
            )

            write_session.add(delivery)
            await write_session.commit()

            delivery_id = delivery.id
            assert delivery_id is not None

        async with async_session_factory() as read_session:
            stored_delivery = await read_session.get(
                WebhookDelivery,
                delivery_id,
            )

            assert stored_delivery is not None
            assert stored_delivery.target_url == ("https://example.com/webhooks/test")
            assert stored_delivery.event_type == "integration.test"
            assert stored_delivery.payload == {"message": "database round trip"}
            assert stored_delivery.status == "pending"
            assert stored_delivery.attempt_count == 0
            assert stored_delivery.created_at is not None
            assert stored_delivery.delivered_at is None
    finally:
        try:
            if delivery_id is not None:
                async with async_session_factory() as cleanup_session:
                    await cleanup_session.execute(
                        delete(WebhookDelivery).where(WebhookDelivery.id == delivery_id)
                    )
                    await cleanup_session.commit()
        finally:
            await engine.dispose()


@pytest.mark.skipif(
    not RUN_DATABASE_INTEGRATION_TESTS,
    reason="database integration tests are disabled",
)
def test_webhook_delivery_round_trip() -> None:
    loop_factory = asyncio.SelectorEventLoop if sys.platform == "win32" else None

    asyncio.run(
        run_webhook_delivery_round_trip(),
        loop_factory=loop_factory,
    )
