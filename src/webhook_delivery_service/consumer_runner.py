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


async def run_consumer() -> None:
    settings = Settings()
    consumer = await RabbitMQDeliveryConsumer.connect(settings)

    try:
        async with httpx.AsyncClient() as client:
            sender = HttpxWebhookSender(client)

            async with consumer.iter_messages() as messages:
                async for message in messages:
                    async with async_session_factory() as session:
                        await consume_rabbitmq_delivery_message(
                            message=message,
                            session=session,
                            sender=sender,
                        )
    finally:
        await consumer.close()
        await engine.dispose()


def main() -> None:
    loop_factory = asyncio.SelectorEventLoop if sys.platform == "win32" else None

    try:
        asyncio.run(
            run_consumer(),
            loop_factory=loop_factory,
        )
    except KeyboardInterrupt:
        print("Consumer stopped.")


if __name__ == "__main__":
    main()
