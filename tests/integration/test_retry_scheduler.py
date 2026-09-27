import asyncio
import os
import sys
from collections.abc import AsyncGenerator, Callable, Coroutine
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.schema import CreateSchema, DropSchema

from webhook_delivery_service import retry_scheduler
from webhook_delivery_service.models import Base, OutboxMessage, WebhookDelivery

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_DATABASE_INTEGRATION_TESTS") != "1",
    reason="database integration tests are disabled",
)


@asynccontextmanager
async def isolated_database() -> (
    AsyncGenerator[tuple[async_sessionmaker[AsyncSession], str]]
):
    from webhook_delivery_service.database import engine

    schema_name = f"test_retry_scheduler_{uuid4().hex}"
    test_engine = engine.execution_options(
        schema_translate_map={None: schema_name},
    )
    schema_created = False

    try:
        # The scheduler must not select real deliveries from the application schema.
        async with test_engine.begin() as connection:
            await connection.execute(CreateSchema(schema_name))
            await connection.run_sync(Base.metadata.create_all)
        schema_created = True

        yield async_sessionmaker(test_engine, expire_on_commit=False), schema_name
    finally:
        try:
            if schema_created:
                async with engine.begin() as connection:
                    await connection.execute(DropSchema(schema_name, cascade=True))
        finally:
            await engine.dispose()


def make_delivery(
    next_attempt_at: datetime | None,
    status: str = "retry_scheduled",
) -> WebhookDelivery:
    return WebhookDelivery(
        id=uuid4(),
        target_url="https://example.com/webhooks/test",
        event_type="integration.retry",
        payload={"message": "retry scheduler test"},
        status=status,
        attempt_count=2,
        next_attempt_at=next_attempt_at,
        last_error="previous HTTP timeout",
    )


async def check_due_retries_and_repeated_poll() -> None:
    now = datetime.now(UTC)
    due = [
        make_delivery(now - timedelta(minutes=1)),
        make_delivery(now),
    ]
    unchanged = [
        make_delivery(now + timedelta(hours=1)),
        make_delivery(None),
        *(
            make_delivery(now - timedelta(minutes=1), status)
            for status in ("pending", "processing", "succeeded", "failed")
        ),
    ]

    async with isolated_database() as (sessions, _):
        async with sessions() as session:
            session.add_all([*due, *unchanged])
            await session.flush()
            original_messages = [
                OutboxMessage(
                    delivery_id=delivery.id,
                    message_type="webhook.delivery.requested",
                    payload={"delivery_id": str(delivery.id)},
                    published_at=now - timedelta(minutes=2),
                )
                for delivery in due
            ]
            session.add_all(original_messages)
            await session.commit()
        original_ids = {message.id for message in original_messages}

        # Freeze the cutoff to exercise next_attempt_at == now exactly.
        with patch.object(retry_scheduler, "datetime") as clock:
            clock.now.return_value = now
            async with sessions() as session:
                assert await retry_scheduler.schedule_due_webhook_retries(session) == 2

            async with sessions() as session:
                stored = {
                    delivery.id: delivery
                    for delivery in (
                        await session.scalars(select(WebhookDelivery))
                    ).all()
                }
                messages = (await session.scalars(select(OutboxMessage))).all()

                assert set(stored) == {delivery.id for delivery in [*due, *unchanged]}
                assert len(messages) == 4
                assert original_ids <= {message.id for message in messages}
                new_messages = [m for m in messages if m.id not in original_ids]
                assert len(new_messages) == 2
                assert {m.delivery_id for m in new_messages} == {d.id for d in due}

                for message in new_messages:
                    assert message.message_type == "webhook.delivery.requested"
                    assert message.payload == {"delivery_id": str(message.delivery_id)}
                    assert message.created_at is not None
                    assert message.published_at is None

                for delivery in due:
                    saved = stored[delivery.id]
                    assert saved.status == "pending"
                    assert saved.next_attempt_at is None
                    assert saved.attempt_count == 2
                    assert saved.last_error == "previous HTTP timeout"
                    assert saved.delivered_at is None

                for delivery in unchanged:
                    saved = stored[delivery.id]
                    assert saved.status == delivery.status
                    assert saved.next_attempt_at == delivery.next_attempt_at
                    assert saved.attempt_count == delivery.attempt_count

            async with sessions() as session:
                assert await retry_scheduler.schedule_due_webhook_retries(session) == 0

        async with sessions() as session:
            messages = (await session.scalars(select(OutboxMessage))).all()
            assert len(messages) == 4


async def check_batch_limit_and_order() -> None:
    now = datetime.now(UTC)
    deliveries = [
        make_delivery(now - timedelta(minutes=minutes)) for minutes in (3, 2, 1)
    ]

    async with isolated_database() as (sessions, _):
        async with sessions() as session:
            session.add_all(deliveries)
            await session.commit()

        async with sessions() as session:
            assert (
                await retry_scheduler.schedule_due_webhook_retries(
                    session, batch_size=2
                )
                == 2
            )

        async with sessions() as session:
            messages = (await session.scalars(select(OutboxMessage))).all()
            assert len(messages) == 2
            assert {m.delivery_id for m in messages} == {d.id for d in deliveries[:2]}
            remaining = await session.get(WebhookDelivery, deliveries[2].id)
            assert remaining is not None
            assert remaining.status == "retry_scheduled"

        async with sessions() as session:
            assert (
                await retry_scheduler.schedule_due_webhook_retries(
                    session, batch_size=2
                )
                == 1
            )

        async with sessions() as session:
            messages = (await session.scalars(select(OutboxMessage))).all()
            assert len(messages) == 3
            assert {m.delivery_id for m in messages} == {d.id for d in deliveries}


async def check_locked_delivery_is_skipped() -> None:
    now = datetime.now(UTC)
    locked = make_delivery(now - timedelta(minutes=2))
    available = make_delivery(now - timedelta(minutes=1))

    async with isolated_database() as (sessions, _):
        async with sessions() as session:
            session.add_all([locked, available])
            await session.commit()

        async with sessions() as locking_session:
            await locking_session.get(WebhookDelivery, locked.id, with_for_update=True)

            async with sessions() as scheduler_session:
                # Fail promptly if SKIP LOCKED is removed and the query waits.
                await scheduler_session.execute(
                    text("SET LOCAL statement_timeout = '3s'")
                )
                assert (
                    await retry_scheduler.schedule_due_webhook_retries(
                        scheduler_session
                    )
                    == 1
                )

            async with sessions() as session:
                message = (await session.scalars(select(OutboxMessage))).one()
                assert message.delivery_id == available.id
                saved = await session.get(WebhookDelivery, locked.id)
                assert saved is not None
                assert saved.status == "retry_scheduled"

            await locking_session.rollback()

        async with sessions() as session:
            assert await retry_scheduler.schedule_due_webhook_retries(session) == 1

        async with sessions() as session:
            messages = (await session.scalars(select(OutboxMessage))).all()
            assert len(messages) == 2
            assert {m.delivery_id for m in messages} == {locked.id, available.id}


async def check_outbox_failure_rolls_back_delivery() -> None:
    delivery = make_delivery(datetime.now(UTC) - timedelta(minutes=1))

    async with isolated_database() as (sessions, schema_name):
        async with sessions() as session:
            session.add(delivery)
            await session.commit()

            # This test-only constraint forces an actual PostgreSQL insert failure.
            # The schema identifier consists only of our fixed prefix and UUID hex.
            await session.execute(
                text(
                    f'ALTER TABLE "{schema_name}".outbox_messages '
                    "ADD CONSTRAINT reject_retry_message CHECK (false)"
                )
            )
            await session.commit()

        async with sessions() as session:
            with pytest.raises(IntegrityError):
                await retry_scheduler.schedule_due_webhook_retries(session)
            await session.rollback()

        async with sessions() as session:
            saved = await session.get(WebhookDelivery, delivery.id)
            assert saved is not None
            assert saved.status == "retry_scheduled"
            assert saved.next_attempt_at == delivery.next_attempt_at
            assert saved.attempt_count == 2
            assert saved.last_error == "previous HTTP timeout"
            assert (await session.scalars(select(OutboxMessage))).all() == []


@pytest.mark.parametrize(
    "scenario",
    [
        pytest.param(check_due_retries_and_repeated_poll, id="due-and-repeat"),
        pytest.param(check_batch_limit_and_order, id="batch-limit"),
        pytest.param(check_locked_delivery_is_skipped, id="skip-locked"),
        pytest.param(check_outbox_failure_rolls_back_delivery, id="rollback"),
    ],
)
def test_retry_scheduler(
    scenario: Callable[[], Coroutine[object, object, None]],
) -> None:
    loop_factory = asyncio.SelectorEventLoop if sys.platform == "win32" else None
    asyncio.run(scenario(), loop_factory=loop_factory)
