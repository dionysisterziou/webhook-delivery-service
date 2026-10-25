import asyncio
import os
import sys
from collections.abc import AsyncGenerator, Coroutine
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema

from webhook_delivery_service.models import Base, OutboxMessage, WebhookDelivery

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_DATABASE_INTEGRATION_TESTS") != "1",
    reason="database integration tests are disabled",
)


@asynccontextmanager
async def isolated_database(
    session_class: type[AsyncSession] = AsyncSession,
) -> AsyncGenerator[async_sessionmaker[AsyncSession]]:
    from webhook_delivery_service.config import Settings

    schema_name = f"test_api_idempotency_{uuid4().hex}"
    # Use a dedicated pool and schema; application tables are never modified.
    engine = create_async_engine(
        Settings().build_database_url(),
        execution_options={"schema_translate_map": {None: schema_name}},
        connect_args={"connect_timeout": 5},
    )
    schema_created = False

    try:
        async with engine.begin() as connection:
            await connection.execute(CreateSchema(schema_name))
            await connection.run_sync(Base.metadata.create_all)
        schema_created = True

        yield async_sessionmaker(engine, class_=session_class, expire_on_commit=False)
    finally:
        try:
            if schema_created:
                async with engine.begin() as connection:
                    await connection.execute(DropSchema(schema_name, cascade=True))
        finally:
            await engine.dispose()


@asynccontextmanager
async def isolated_api(
    monkeypatch: pytest.MonkeyPatch,
    session_class: type[AsyncSession] = AsyncSession,
    backend_pids: list[int] | None = None,
) -> AsyncGenerator[tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]]]:
    from webhook_delivery_service.database import get_database_session
    from webhook_delivery_service.main import app

    async with isolated_database(session_class) as sessions:

        async def override_database_session() -> AsyncGenerator[AsyncSession]:
            async with sessions() as session:
                if backend_pids is not None:
                    pid = await session.scalar(text("SELECT pg_backend_pid()"))
                    assert pid is not None
                    backend_pids.append(pid)
                yield session

        # Run the real FastAPI route and service, using real PostgreSQL sessions.
        # ASGITransport calls the app in-process; no Uvicorn server is needed.
        with monkeypatch.context() as patch:
            patch.setitem(
                app.dependency_overrides,
                get_database_session,
                override_database_session,
            )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://testserver",
            ) as client:
                yield client, sessions


def request_body() -> dict[str, object]:
    return {
        "target_url": "https://example.com/webhooks/test",
        "event_type": "integration.api.idempotency",
        "payload": {"order_id": 123},
    }


async def read_rows(
    sessions: async_sessionmaker[AsyncSession],
) -> tuple[list[WebhookDelivery], list[OutboxMessage]]:
    # A fresh session observes committed rows from all completed requests.
    async with sessions() as session:
        deliveries = list((await session.scalars(select(WebhookDelivery))).all())
        messages = list((await session.scalars(select(OutboxMessage))).all())
    return deliveries, messages


async def check_repeated_request_creates_one_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The client creates one key and reuses it for the repeated request.
    headers = {"Idempotency-Key": str(uuid4())}
    async with isolated_api(monkeypatch) as (client, sessions):
        first = await client.post("/deliveries", json=request_body(), headers=headers)
        assert first.status_code == 201, first.text
        second = await client.post("/deliveries", json=request_body(), headers=headers)
        assert second.status_code == 200, second.text

        deliveries, outbox_messages = await read_rows(sessions)

        assert (len(deliveries), len(outbox_messages)) == (1, 1), (
            "Repeated POST with the same Idempotency-Key must create one delivery "
            "and one initial outbox message; "
            f"found {len(deliveries)} deliveries and {len(outbox_messages)} messages"
        )
        assert second.json()["id"] == first.json()["id"] == str(deliveries[0].id)
        assert deliveries[0].status == "pending"
        assert deliveries[0].attempt_count == 0
        assert outbox_messages[0].delivery_id == deliveries[0].id
        assert outbox_messages[0].payload == {"delivery_id": str(deliveries[0].id)}
        assert outbox_messages[0].published_at is None


def run_check(check: Coroutine[Any, Any, None]) -> None:
    loop_factory = asyncio.SelectorEventLoop if sys.platform == "win32" else None
    asyncio.run(check, loop_factory=loop_factory)


def test_repeated_post_with_same_key_creates_one_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_check(check_repeated_request_creates_one_delivery(monkeypatch))


async def check_changed_request_conflicts(
    monkeypatch: pytest.MonkeyPatch, change: dict[str, object]
) -> None:
    headers = {"Idempotency-Key": str(uuid4())}
    async with isolated_api(monkeypatch) as (client, sessions):
        first = await client.post("/deliveries", json=request_body(), headers=headers)
        assert first.status_code == 201, first.text
        conflict = await client.post(
            "/deliveries", json=request_body() | change, headers=headers
        )
        assert conflict.status_code == 409, conflict.text
        replay = await client.post("/deliveries", json=request_body(), headers=headers)
        assert replay.status_code == 200, replay.text
        assert replay.json()["id"] == first.json()["id"]
        deliveries, messages = await read_rows(sessions)
        assert (len(deliveries), len(messages)) == (1, 1)
        assert deliveries[0].payload == request_body()["payload"]


@pytest.mark.parametrize(
    "change",
    [
        {"target_url": "https://example.com/other"},
        {"event_type": "order.cancelled"},
        {"payload": {"order_id": 124}},
    ],
)
def test_same_key_with_changed_request_returns_conflict(
    monkeypatch: pytest.MonkeyPatch, change: dict[str, object]
) -> None:
    run_check(check_changed_request_conflicts(monkeypatch, change))


async def check_independent_requests(
    monkeypatch: pytest.MonkeyPatch, use_keys: bool
) -> None:
    async with isolated_api(monkeypatch) as (client, sessions):
        responses = []
        for _ in range(2):
            headers = {"Idempotency-Key": str(uuid4())} if use_keys else {}
            responses.append(
                await client.post("/deliveries", json=request_body(), headers=headers)
            )
        assert [response.status_code for response in responses] == [201, 201]
        assert responses[0].json()["id"] != responses[1].json()["id"]
        deliveries, messages = await read_rows(sessions)
        assert (len(deliveries), len(messages)) == (2, 2)
        assert {message.delivery_id for message in messages} == {
            delivery.id for delivery in deliveries
        }


@pytest.mark.parametrize("use_keys", [False, True], ids=["no-key", "different-keys"])
def test_independent_requests_create_separate_deliveries(
    monkeypatch: pytest.MonkeyPatch, use_keys: bool
) -> None:
    run_check(check_independent_requests(monkeypatch, use_keys))


async def check_replay_preserves_terminal_state(
    monkeypatch: pytest.MonkeyPatch, terminal_status: str
) -> None:
    from datetime import UTC, datetime
    from uuid import UUID

    headers = {"Idempotency-Key": str(uuid4())}
    async with isolated_api(monkeypatch) as (client, sessions):
        first = await client.post("/deliveries", json=request_body(), headers=headers)
        assert first.status_code == 201, first.text
        async with sessions() as session:
            delivery = await session.get(WebhookDelivery, UUID(first.json()["id"]))
            assert delivery is not None
            delivery.status = terminal_status
            delivery.attempt_count = 5
            delivery.last_error = "HTTP 503" if terminal_status == "failed" else None
            delivery.delivered_at = (
                datetime.now(UTC) if terminal_status == "succeeded" else None
            )
            await session.commit()

        replay = await client.post("/deliveries", json=request_body(), headers=headers)
        assert replay.status_code == 200, replay.text
        assert replay.json()["id"] == first.json()["id"]
        assert replay.json()["status"] == terminal_status
        assert replay.json()["attempt_count"] == 5
        assert replay.json()["last_error"] == delivery.last_error
        assert (replay.json()["delivered_at"] is not None) == (
            terminal_status == "succeeded"
        )
        deliveries, messages = await read_rows(sessions)
        assert (len(deliveries), len(messages)) == (1, 1)
        assert deliveries[0].status == terminal_status
        assert deliveries[0].attempt_count == 5


@pytest.mark.parametrize("terminal_status", ["failed", "succeeded"])
def test_replay_does_not_restart_a_terminal_delivery(
    monkeypatch: pytest.MonkeyPatch, terminal_status: str
) -> None:
    run_check(check_replay_preserves_terminal_state(monkeypatch, terminal_status))


async def check_concurrent_requests(
    monkeypatch: pytest.MonkeyPatch, conflicting: bool
) -> None:
    first_waiting = asyncio.Event()
    release_first = asyncio.Event()
    backend_pids: list[int] = []

    class PausedCommitSession(AsyncSession):
        async def commit(self) -> None:
            if not first_waiting.is_set():
                first_waiting.set()
                await release_first.wait()
            await super().commit()

    headers = {"Idempotency-Key": str(uuid4())}
    second_body = request_body()
    if conflicting:
        second_body["payload"] = {"order_id": 124}

    async with isolated_api(
        monkeypatch, PausedCommitSession, backend_pids
    ) as (client, sessions):
        tasks: list[asyncio.Task[httpx.Response]] = []
        try:
            async with asyncio.timeout(15):
                first_task = asyncio.create_task(
                    client.post("/deliveries", json=request_body(), headers=headers)
                )
                tasks.append(first_task)
                await first_waiting.wait()
                second_task = asyncio.create_task(
                    client.post("/deliveries", json=second_body, headers=headers)
                )
                tasks.append(second_task)

                # Observe PostgreSQL waiting on the competing insertion. A fixed
                # delay cannot establish that the requests genuinely overlapped.
                async with sessions() as observer:
                    while True:
                        if second_task.done():
                            response = await second_task
                            pytest.fail(
                                "Second request completed before the first committed: "
                                f"{response.status_code}"
                            )
                        if len(backend_pids) == 2:
                            assert backend_pids[0] != backend_pids[1]
                            blocked = await observer.scalar(
                                text(
                                    "SELECT :first_pid = "
                                    "ANY(pg_blocking_pids(:second_pid))"
                                ),
                                {
                                    "first_pid": backend_pids[0],
                                    "second_pid": backend_pids[1],
                                },
                            )
                            if blocked:
                                break
                        await asyncio.sleep(0.02)

                release_first.set()
                first, second = await asyncio.gather(*tasks)
        finally:
            release_first.set()
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        assert first.status_code == 201, first.text
        assert second.status_code == (409 if conflicting else 200), second.text
        if not conflicting:
            assert second.json()["id"] == first.json()["id"]
        deliveries, messages = await read_rows(sessions)
        assert (len(deliveries), len(messages)) == (1, 1)
        assert str(deliveries[0].id) == first.json()["id"]
        assert deliveries[0].payload == request_body()["payload"]
        assert messages[0].delivery_id == deliveries[0].id


@pytest.mark.parametrize("conflicting", [False, True], ids=["same-body", "changed-body"])
def test_concurrent_requests_share_one_key(
    monkeypatch: pytest.MonkeyPatch, conflicting: bool
) -> None:
    run_check(check_concurrent_requests(monkeypatch, conflicting))


async def check_failed_transaction_does_not_reserve_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fail_next_commit = True

    class FailOnceCommitSession(AsyncSession):
        async def commit(self) -> None:
            nonlocal fail_next_commit
            if fail_next_commit:
                fail_next_commit = False
                # Flush both rows into the real transaction, then simulate a
                # failure before commit. Session cleanup must roll back both.
                await self.flush()
                raise RuntimeError("simulated failure before commit")
            await super().commit()

    headers = {"Idempotency-Key": str(uuid4())}
    async with isolated_api(monkeypatch, FailOnceCommitSession) as (client, sessions):
        with pytest.raises(RuntimeError, match="simulated failure before commit"):
            await client.post("/deliveries", json=request_body(), headers=headers)
        deliveries, messages = await read_rows(sessions)
        assert (len(deliveries), len(messages)) == (0, 0)
        retry = await client.post("/deliveries", json=request_body(), headers=headers)
        assert retry.status_code == 201, retry.text
        deliveries, messages = await read_rows(sessions)
        assert (len(deliveries), len(messages)) == (1, 1)
        assert messages[0].delivery_id == deliveries[0].id


def test_failed_transaction_does_not_reserve_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_check(check_failed_transaction_does_not_reserve_key(monkeypatch))
