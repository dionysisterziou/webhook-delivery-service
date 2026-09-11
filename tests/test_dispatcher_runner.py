import asyncio
from unittest.mock import AsyncMock, MagicMock, call

import pytest

from webhook_delivery_service import dispatcher_runner


def test_run_dispatcher_polls_again_when_outbox_is_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publisher = MagicMock()
    publisher.close = AsyncMock()

    connect = AsyncMock(return_value=publisher)
    session_factory = MagicMock()
    dispatch = AsyncMock(side_effect=[0, 0])
    sleep = AsyncMock(side_effect=[None, asyncio.CancelledError()])
    dispose = AsyncMock()

    monkeypatch.setattr(dispatcher_runner, "Settings", MagicMock())
    monkeypatch.setattr(
        dispatcher_runner.RabbitMQOutboxPublisher,
        "connect",
        connect,
    )
    monkeypatch.setattr(
        dispatcher_runner,
        "async_session_factory",
        session_factory,
    )
    monkeypatch.setattr(
        dispatcher_runner,
        "dispatch_unpublished_outbox_messages",
        dispatch,
    )
    monkeypatch.setattr(dispatcher_runner.asyncio, "sleep", sleep)
    monkeypatch.setattr(
        dispatcher_runner,
        "engine",
        MagicMock(dispose=dispose),
    )

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(dispatcher_runner.run_dispatcher())

    assert dispatch.await_count == 2
    assert sleep.await_args_list == [call(1), call(1)]

    connect.assert_awaited_once()
    publisher.close.assert_awaited_once_with()
    dispose.assert_awaited_once_with()
