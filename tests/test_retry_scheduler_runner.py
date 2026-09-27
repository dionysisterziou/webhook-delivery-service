import asyncio
from collections.abc import AsyncGenerator
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call

import pytest
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from webhook_delivery_service import retry_scheduler_runner


def test_runner_polls_after_empty_batch_and_closes_sessions_before_waiting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from contextlib import asynccontextmanager

    sessions = [MagicMock(spec=AsyncSession), MagicMock(spec=AsyncSession)]
    remaining_sessions = iter(sessions)
    closed_sessions = []

    @asynccontextmanager
    async def session_factory() -> AsyncGenerator[AsyncSession]:
        session = next(remaining_sessions)
        try:
            yield session
        finally:
            closed_sessions.append(session)

    schedule = AsyncMock(side_effect=[0, 2])
    dispose = AsyncMock()

    async def pause(seconds: float) -> None:
        assert seconds == 1
        assert len(closed_sessions) == schedule.await_count
        if len(closed_sessions) == 2:
            raise asyncio.CancelledError()

    sleep = AsyncMock(side_effect=pause)
    monkeypatch.setattr(retry_scheduler_runner, "async_session_factory", session_factory)
    monkeypatch.setattr(retry_scheduler_runner, "schedule_due_webhook_retries", schedule)
    monkeypatch.setattr(retry_scheduler_runner.asyncio, "sleep", sleep)
    monkeypatch.setattr(retry_scheduler_runner, "engine", MagicMock(dispose=dispose))

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(retry_scheduler_runner.run_retry_scheduler())

    assert schedule.await_args_list == [call(session=session) for session in sessions]
    assert closed_sessions == sessions
    assert sleep.await_args_list == [call(1), call(1)]
    dispose.assert_awaited_once_with()


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(SQLAlchemyError("database unavailable"), id="database-error"),
        pytest.param(asyncio.CancelledError(), id="cancelled-during-poll"),
    ],
)
def test_runner_releases_resources_when_poll_is_interrupted(
    monkeypatch: pytest.MonkeyPatch,
    error: BaseException,
) -> None:
    session = MagicMock(spec=AsyncSession)
    context = MagicMock()
    context.__aenter__.return_value = session
    context.__aexit__.return_value = False
    schedule = AsyncMock(side_effect=error)
    sleep = AsyncMock()
    dispose = AsyncMock()

    monkeypatch.setattr(
        retry_scheduler_runner,
        "async_session_factory",
        MagicMock(return_value=context),
    )
    monkeypatch.setattr(retry_scheduler_runner, "schedule_due_webhook_retries", schedule)
    monkeypatch.setattr(retry_scheduler_runner.asyncio, "sleep", sleep)
    monkeypatch.setattr(retry_scheduler_runner, "engine", MagicMock(dispose=dispose))

    with pytest.raises(type(error)):
        asyncio.run(retry_scheduler_runner.run_retry_scheduler())

    schedule.assert_awaited_once_with(session=session)
    context.__aexit__.assert_awaited_once()
    sleep.assert_not_awaited()
    dispose.assert_awaited_once_with()


@pytest.mark.parametrize("platform", ["win32", "linux"])
def test_main_selects_event_loop_and_reports_keyboard_interrupt(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    platform: str,
) -> None:
    expected_factory = asyncio.SelectorEventLoop if platform == "win32" else None

    def interrupt(coroutine, *, loop_factory) -> None:
        coroutine.close()
        assert loop_factory is expected_factory
        raise KeyboardInterrupt()

    monkeypatch.setattr(retry_scheduler_runner, "sys", SimpleNamespace(platform=platform))
    monkeypatch.setattr(
        retry_scheduler_runner,
        "asyncio",
        SimpleNamespace(run=interrupt, SelectorEventLoop=asyncio.SelectorEventLoop),
    )

    retry_scheduler_runner.main()

    assert capsys.readouterr().out == "Retry scheduler stopped.\n"
