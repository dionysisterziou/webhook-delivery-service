import asyncio
import sys

from webhook_delivery_service.database import async_session_factory, engine
from webhook_delivery_service.delivery_logs import delivery_log_output
from webhook_delivery_service.retry_scheduler import schedule_due_webhook_retries

RETRY_POLL_INTERVAL_SECONDS = 1


async def run_retry_scheduler() -> None:
    try:
        while True:
            async with async_session_factory() as session:
                await schedule_due_webhook_retries(session=session)

            await asyncio.sleep(RETRY_POLL_INTERVAL_SECONDS)
    finally:
        await engine.dispose()


def main() -> None:
    loop_factory = asyncio.SelectorEventLoop if sys.platform == "win32" else None

    try:
        with delivery_log_output():
            asyncio.run(
                run_retry_scheduler(),
                loop_factory=loop_factory,
            )
    except KeyboardInterrupt:
        print("Retry scheduler stopped.")


if __name__ == "__main__":
    main()
