import asyncio
import json
import os
import sys
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.schema import CreateSchema, DropSchema

from webhook_delivery_service.consumer import handle_delivery_requested_message
from webhook_delivery_service.models import Base, WebhookDelivery

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_DATABASE_INTEGRATION_TESTS") != "1",
    reason="database integration tests are disabled",
)


@asynccontextmanager
async def isolated_database() -> AsyncGenerator[async_sessionmaker[AsyncSession]]:
    from webhook_delivery_service.database import engine

    schema_name = f"test_consumer_concurrency_{uuid4().hex}"
    test_engine = engine.execution_options(
        schema_translate_map={None: schema_name},
    )
    schema_created = False

    try:
        async with test_engine.begin() as connection:
            await connection.execute(CreateSchema(schema_name))
            await connection.run_sync(Base.metadata.create_all)
        schema_created = True

        yield async_sessionmaker(test_engine, expire_on_commit=False)
    finally:
        try:
            if schema_created:
                async with engine.begin() as connection:
                    await connection.execute(DropSchema(schema_name, cascade=True))
        finally:
            await engine.dispose()


class PausedSuccessSender:
    """Hold the first send open while a second handler loads the same delivery."""

    def __init__(self) -> None:
        self.delivery_ids: list[UUID] = []
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def send(self, delivery: WebhookDelivery) -> None:
        self.delivery_ids.append(delivery.id)
        self.started.set()
        await self.release.wait()


async def wait_for_second_handler(
    observer: AsyncSession,
    sender: PausedSuccessSender,
    second_task: asyncio.Task[None],
    first_pid: int,
    second_pid: int,
) -> None:
    # Observe actual progress, rather than assuming a fixed sleep was long enough.
    # Before the fix, the second handler reaches send(). After the fix, PostgreSQL
    # reports that it is waiting for the first handler's row lock.
    while len(sender.delivery_ids) < 2:
        if second_task.done():
            await second_task  # Surface any handler error before asserting.
            pytest.fail("The duplicate handler finished before the first send ended")

        blocked_by_first = await observer.scalar(
            text("SELECT :first_pid = ANY(pg_blocking_pids(:second_pid))"),
            {"first_pid": first_pid, "second_pid": second_pid},
        )
        if blocked_by_first:
            return

        await asyncio.sleep(0.02)


async def check_concurrent_messages_send_delivery_once() -> None:
    delivery_id = uuid4()
    message_body = json.dumps(
        {
            "message_id": str(uuid4()),
            "message_type": "webhook.delivery.requested",
            "payload": {"delivery_id": str(delivery_id)},
        }
    ).encode("utf-8")
    sender = PausedSuccessSender()

    async with isolated_database() as sessions:
        async with sessions() as session:
            session.add(
                WebhookDelivery(
                    id=delivery_id,
                    target_url="https://example.com/webhooks/test",
                    event_type="integration.consumer.concurrency",
                    payload={"source": "concurrent-duplicate-check"},
                    status="pending",
                    attempt_count=0,
                )
            )
            await session.commit()

        # Each handler gets an independent session and PostgreSQL connection.
        async with sessions() as first_session, sessions() as second_session:
            first_pid = await first_session.scalar(text("SELECT pg_backend_pid()"))
            second_pid = await second_session.scalar(text("SELECT pg_backend_pid()"))
            assert first_pid is not None
            assert second_pid is not None
            assert first_pid != second_pid
            tasks: list[asyncio.Task[None]] = []

            try:
                # This timeout bounds failures; it does not decide whether a
                # duplicate was prevented. The events/lock observation do that.
                async with asyncio.timeout(15):
                    first_task = asyncio.create_task(
                        handle_delivery_requested_message(
                            session=first_session,
                            sender=sender,
                            message_body=message_body,
                        )
                    )
                    tasks.append(first_task)
                    await sender.started.wait()

                    second_task = asyncio.create_task(
                        handle_delivery_requested_message(
                            session=second_session,
                            sender=sender,
                            message_body=message_body,
                        )
                    )
                    tasks.append(second_task)

                    async with sessions() as observer:
                        await wait_for_second_handler(
                            observer, sender, second_task, first_pid, second_pid
                        )

                    sender.release.set()
                    await asyncio.gather(*tasks)
            finally:
                sender.release.set()
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

        assert (
            len(sender.delivery_ids) == 1
        ), "Two concurrent messages sent the same delivery more than once"
        assert sender.delivery_ids == [delivery_id]

        # Read the committed outcome through a fresh session.
        async with sessions() as session:
            saved = await session.get(WebhookDelivery, delivery_id)
            assert saved is not None
            assert saved.status == "succeeded"
            assert saved.attempt_count == 1
            assert saved.delivered_at is not None
            assert saved.next_attempt_at is None
            assert saved.last_error is None


def test_concurrent_messages_send_delivery_once() -> None:
    loop_factory = asyncio.SelectorEventLoop if sys.platform == "win32" else None
    asyncio.run(
        check_concurrent_messages_send_delivery_once(),
        loop_factory=loop_factory,
    )
