import asyncio
import sys

import httpx

from webhook_delivery_service.config import Settings
from webhook_delivery_service.database import async_session_factory, engine
from webhook_delivery_service.http_sender import HttpxWebhookSender
from webhook_delivery_service.rabbitmq_consumer import (
    RabbitMQDeliveryConsumer,
    consume_rabbitmq_delivery_message,
)


async def run_consumer_once() -> int:
    settings = Settings()
    consumer = await RabbitMQDeliveryConsumer.connect(settings)

    try:
        message = await consumer.get_message()

        if message is None:
            return 0

        async with httpx.AsyncClient() as client:
            sender = HttpxWebhookSender(client)

            async with async_session_factory() as session:
                await consume_rabbitmq_delivery_message(
                    message=message,
                    session=session,
                    sender=sender,
                )

        return 1
    finally:
        await consumer.close()
        await engine.dispose()


def main() -> None:
    loop_factory = asyncio.SelectorEventLoop if sys.platform == "win32" else None

    processed_count = asyncio.run(
        run_consumer_once(),
        loop_factory=loop_factory,
    )

    print(f"Processed {processed_count} RabbitMQ message(s).")


if __name__ == "__main__":
    main()
