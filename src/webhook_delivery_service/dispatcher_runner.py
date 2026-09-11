import asyncio
import sys

from webhook_delivery_service.config import Settings
from webhook_delivery_service.database import async_session_factory, engine
from webhook_delivery_service.dispatcher import (
    dispatch_unpublished_outbox_messages,
)
from webhook_delivery_service.rabbitmq import RabbitMQOutboxPublisher


async def run_dispatcher() -> None:
    settings = Settings()
    publisher = await RabbitMQOutboxPublisher.connect(settings)

    try:
        while True:
            async with async_session_factory() as session:
                await dispatch_unpublished_outbox_messages(
                    session=session,
                    publisher=publisher,
                )

            await asyncio.sleep(1)
    finally:
        await publisher.close()
        await engine.dispose()


def main() -> None:
    loop_factory = asyncio.SelectorEventLoop if sys.platform == "win32" else None

    try:
        asyncio.run(
            run_dispatcher(),
            loop_factory=loop_factory,
        )
    except KeyboardInterrupt:
        print("Dispatcher stopped.")


if __name__ == "__main__":
    main()
