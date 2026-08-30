import asyncio
import sys

import uvicorn

from webhook_delivery_service.database import engine


async def serve_api() -> None:
    config = uvicorn.Config(
        "webhook_delivery_service.main:app",
        host="127.0.0.1",
        port=8000,
        log_level="info",
    )
    server = uvicorn.Server(config)

    try:
        await server.serve()
    finally:
        await engine.dispose()


def main() -> None:
    loop_factory = asyncio.SelectorEventLoop if sys.platform == "win32" else None

    try:
        asyncio.run(
            serve_api(),
            loop_factory=loop_factory,
        )
    except KeyboardInterrupt:
        print("API stopped.")


if __name__ == "__main__":
    main()
