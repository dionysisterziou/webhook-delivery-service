import asyncio
import os
import sys
from uuid import UUID

import pytest

RUN_DATABASE_INTEGRATION_TESTS = os.getenv("RUN_DATABASE_INTEGRATION_TESTS") == "1"


async def run_webhook_delivery_round_trip() -> None:
    from sqlalchemy import delete, select

    from webhook_delivery_service.database import async_session_factory, engine
    from webhook_delivery_service.models import (
        OutboxMessage,
        WebhookDelivery,
    )
    from webhook_delivery_service.schemas import WebhookDeliveryCreate
    from webhook_delivery_service.services import create_webhook_delivery

    delivery_id: UUID | None = None

    try:
        async with async_session_factory() as write_session:
            delivery = await create_webhook_delivery(
                session=write_session,
                delivery_data=WebhookDeliveryCreate(
                    target_url="https://example.com/webhooks/test",
                    event_type="integration.test",
                    payload={"message": "database round trip"},
                ),
            )

            delivery_id = delivery.id
            assert delivery_id is not None

        async with async_session_factory() as read_session:
            stored_delivery = await read_session.get(
                WebhookDelivery,
                delivery_id,
            )

            outbox_result = await read_session.execute(
                select(OutboxMessage).where(OutboxMessage.delivery_id == delivery_id)
            )
            stored_outbox_message = outbox_result.scalar_one_or_none()

            assert stored_delivery is not None
            assert stored_delivery.target_url == ("https://example.com/webhooks/test")
            assert stored_delivery.event_type == "integration.test"
            assert stored_delivery.payload == {"message": "database round trip"}
            assert stored_delivery.status == "pending"
            assert stored_delivery.attempt_count == 0
            assert stored_delivery.created_at is not None
            assert stored_delivery.delivered_at is None

            assert stored_outbox_message is not None
            assert stored_outbox_message.delivery_id == delivery_id
            assert stored_outbox_message.message_type == "webhook.delivery.requested"
            assert stored_outbox_message.payload == {"delivery_id": str(delivery_id)}
            assert stored_outbox_message.created_at is not None
            assert stored_outbox_message.published_at is None
    finally:
        try:
            if delivery_id is not None:
                async with async_session_factory() as cleanup_session:
                    await cleanup_session.execute(
                        delete(OutboxMessage).where(
                            OutboxMessage.delivery_id == delivery_id
                        )
                    )
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
