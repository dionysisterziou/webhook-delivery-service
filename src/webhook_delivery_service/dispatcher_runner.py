import asyncio
import sys

from webhook_delivery_service.config import Settings
from webhook_delivery_service.database import async_session_factory, engine
from webhook_delivery_service.dispatcher import (
    dispatch_unpublished_outbox_messages,
)
from webhook_delivery_service.rabbitmq import RabbitMQOutboxPublisher


async def run_dispatcher_once() -> int:
    settings = Settings()

    publisher = await RabbitMQOutboxPublisher.connect(settings)

    try:
        async with async_session_factory() as session:
            return await dispatch_unpublished_outbox_messages(
                session=session,
                publisher=publisher,
            )
    finally:
        await publisher.close()
        await engine.dispose()


def main() -> None:
    loop_factory = asyncio.SelectorEventLoop if sys.platform == "win32" else None

    dispatched_count = asyncio.run(
        run_dispatcher_once(),
        loop_factory=loop_factory,
    )

    print(f"Dispatched {dispatched_count} outbox message(s).")


if __name__ == "__main__":
    main()
