"""Run the real services with the disposable settings supplied by the pytest case."""

import asyncio
import os
import socket
import sys
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import uvicorn
from alembic import command
from alembic.config import Config
from fastapi import FastAPI, Request

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "src"), str(ROOT)]


@asynccontextmanager
async def serve(app: FastAPI):
    # Keep the ephemeral port reserved from bind through server shutdown.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        server = uvicorn.Server(uvicorn.Config(app, access_log=False, log_level="warning"))
        task = asyncio.create_task(server.serve(sockets=[listener]))
        try:
            async with asyncio.timeout(10):
                while not server.started:
                    if task.done():
                        await task
                        raise RuntimeError("HTTP server exited during startup")
                    await asyncio.sleep(0.05)
            yield f"http://127.0.0.1:{port}"
        finally:
            server.should_exit = True
            await task


async def check_retry() -> None:
    from examples.webhook_receiver import app as receiver
    from webhook_delivery_service.consumer_runner import run_consumer
    from webhook_delivery_service.dispatcher_runner import run_dispatcher
    from webhook_delivery_service.main import app as api
    from webhook_delivery_service.retry_scheduler_runner import run_retry_scheduler

    observed: list[tuple[str, int]] = []
    delivery_id = None
    state = None

    @receiver.middleware("http")
    async def record_response(request: Request, call_next):
        response = await call_next(request)
        if request.url.path == "/webhooks/fail-once":
            observed.append((request.headers["X-Delivery-ID"], response.status_code))
        return response

    try:
        command.upgrade(Config(toml_file=ROOT / "pyproject.toml"), "head")
        async with (
            serve(api) as api_url,
            serve(receiver) as receiver_url,
            httpx.AsyncClient(base_url=api_url, trust_env=False) as client,
            asyncio.TaskGroup() as group,
        ):
            runners = [
                group.create_task(run_dispatcher()),
                group.create_task(run_consumer()),
                group.create_task(run_retry_scheduler()),
            ]
            try:
                async with asyncio.timeout(90):
                    created = await client.post("/deliveries", json={
                        "target_url": f"{receiver_url}/webhooks/fail-once",
                        "event_type": "demo.delivery.retry",
                        "payload": {"source": "retry-e2e"},
                    })
                    assert created.status_code == 201, created.status_code
                    state = created.json()
                    delivery_id = state["id"]
                    print(f"delivery_id={delivery_id}, last_state={state}")
                    while True:
                        result = await client.get(f"/deliveries/{delivery_id}")
                        result.raise_for_status()
                        latest = result.json()
                        if latest != state:
                            state = latest
                            print(f"delivery_id={delivery_id}, last_state={state}")
                        if state["status"] in {"succeeded", "failed"}:
                            break
                        await asyncio.sleep(0.25)
                    assert state["id"] == delivery_id
                    assert (state["status"], state["attempt_count"]) == ("succeeded", 2)
                    assert observed == [(delivery_id, 503), (delivery_id, 200)]
            finally:
                for runner in runners:
                    runner.cancel()
    except Exception as error:
        error.add_note(f"delivery_id={delivery_id}, last_state={state}, receiver_responses={observed}")
        raise


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("Run tests/e2e/test_retry_delivery.py to provision the disposable services.")
    os.environ["POSTGRES_PORT"], os.environ["RABBITMQ_PORT"] = sys.argv[1:]
    from webhook_delivery_service.config import Settings

    Settings.model_config["env_file"] = None
    loop_factory = asyncio.SelectorEventLoop if sys.platform == "win32" else None
    asyncio.run(check_retry(), loop_factory=loop_factory)
