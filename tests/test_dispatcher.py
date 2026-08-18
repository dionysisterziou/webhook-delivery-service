import asyncio
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from webhook_delivery_service.dispatcher import (
    OutboxMessagePublisher,
    dispatch_outbox_message,
    dispatch_unpublished_outbox_messages,
)
from webhook_delivery_service.models import OutboxMessage


def build_outbox_message() -> OutboxMessage:
    delivery_id = uuid4()

    return OutboxMessage(
        delivery_id=delivery_id,
        message_type="webhook.delivery.requested",
        payload={"delivery_id": str(delivery_id)},
    )


def test_dispatch_outbox_message_marks_message_as_published() -> None:
    session = MagicMock(spec=AsyncSession)
    session.commit = AsyncMock()

    publisher = MagicMock(spec=OutboxMessagePublisher)
    publisher.publish = AsyncMock()

    outbox_message = build_outbox_message()

    asyncio.run(
        dispatch_outbox_message(
            session=session,
            publisher=publisher,
            outbox_message=outbox_message,
        )
    )

    publisher.publish.assert_awaited_once_with(outbox_message)
    assert outbox_message.published_at is not None
    session.commit.assert_awaited_once_with()


def test_dispatch_outbox_message_keeps_message_pending_when_publish_fails() -> None:
    session = MagicMock(spec=AsyncSession)
    session.commit = AsyncMock()

    publisher = MagicMock(spec=OutboxMessagePublisher)
    publisher.publish = AsyncMock(side_effect=RuntimeError("RabbitMQ unavailable"))

    outbox_message = build_outbox_message()

    with pytest.raises(RuntimeError, match="RabbitMQ unavailable"):
        asyncio.run(
            dispatch_outbox_message(
                session=session,
                publisher=publisher,
                outbox_message=outbox_message,
            )
        )

        assert outbox_message.published_at is None
        session.commit.assert_not_awaited()


def test_dispatch_unpublished_outbox_messages_dispatches_each_message() -> None:
    first_message = build_outbox_message()
    second_message = build_outbox_message()

    scalar_result = MagicMock()
    scalar_result.all.return_value = [
        first_message,
        second_message,
    ]

    session = MagicMock(spec=AsyncSession)
    session.scalars = AsyncMock(return_value=scalar_result)
    session.commit = AsyncMock()

    publisher = MagicMock(spec=OutboxMessagePublisher)
    publisher.publish = AsyncMock()

    dispatched_count = asyncio.run(
        dispatch_unpublished_outbox_messages(
            session=session,
            publisher=publisher,
        )
    )

    assert dispatched_count == 2
    session.scalars.assert_awaited_once()

    assert publisher.publish.await_count == 2
    assert publisher.publish.await_args_list[0].args[0] is first_message
    assert publisher.publish.await_args_list[1].args[0] is second_message

    assert first_message.published_at is not None
    assert second_message.published_at is not None
    assert session.commit.await_count == 2


def test_dispatch_unpublished_outbox_messages_does_nothing_when_empty() -> None:
    scalar_result = MagicMock()
    scalar_result.all.return_value = []

    session = MagicMock(spec=AsyncSession)
    session.scalars = AsyncMock(return_value=scalar_result)
    session.commit = AsyncMock()

    publisher = MagicMock(spec=OutboxMessagePublisher)
    publisher.publish = AsyncMock()

    dispatched_count = asyncio.run(
        dispatch_unpublished_outbox_messages(
            session=session,
            publisher=publisher,
        )
    )

    assert dispatched_count == 0
    session.scalars.assert_awaited_once()
    publisher.publish.assert_not_awaited()
    session.commit.assert_not_awaited()
