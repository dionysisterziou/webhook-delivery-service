import asyncio
import re
from urllib.parse import quote

import httpx

from webhook_delivery_service.config import Settings


def create_management_client(settings: Settings) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=httpx.URL(
            scheme="http",
            host=settings.rabbitmq_host,
            port=settings.rabbitmq_management_port,
        ),
        auth=(
            settings.rabbitmq_default_user,
            settings.rabbitmq_default_pass.get_secret_value(),
        ),
        timeout=10,
        trust_env=False,
    )


async def configure_dead_lettering(
    client: httpx.AsyncClient,
    *,
    virtual_host: str,
    queue_name: str,
) -> None:
    """Configure the local broker without deleting or purging existing queues."""
    vhost_path = quote(virtual_host, safe="")
    queue_path = quote(queue_name, safe="")
    exchange_name = f"{queue_name}.dlx"
    exchange_path = quote(exchange_name, safe="")
    dead_queue_path = quote(f"{queue_name}.dlq", safe="")
    policy_path = quote(f"{queue_name}.dead-letter", safe="")
    queue_options = {"durable": True, "auto_delete": False, "arguments": {}}

    # Declare the destination and binding before enabling dead-letter routing.
    response = await client.put(
        f"/api/exchanges/{vhost_path}/{exchange_path}",
        json={
            "type": "direct",
            "durable": True,
            "auto_delete": False,
            "internal": False,
            "arguments": {},
        },
    )
    response.raise_for_status()
    response = await client.put(
        f"/api/queues/{vhost_path}/{dead_queue_path}", json=queue_options
    )
    response.raise_for_status()
    response = await client.post(
        f"/api/bindings/{vhost_path}/e/{exchange_path}/q/{dead_queue_path}",
        json={"routing_key": queue_name, "arguments": {}},
    )
    response.raise_for_status()
    response = await client.put(
        f"/api/queues/{vhost_path}/{queue_path}", json=queue_options
    )
    response.raise_for_status()

    definition = {
        "dead-letter-exchange": exchange_name,
        "dead-letter-routing-key": queue_name,
    }
    response = await client.put(
        f"/api/policies/{vhost_path}/{policy_path}",
        json={
            "pattern": f"^{re.escape(queue_name)}$",
            "apply-to": "queues",
            "priority": 10,
            "definition": definition,
        },
    )
    response.raise_for_status()

    # Policy updates propagate asynchronously. Confirm the effective routing
    # before reporting success or letting an integration test publish messages.
    try:
        async with asyncio.timeout(10):
            while True:
                response = await client.get(f"/api/queues/{vhost_path}/{queue_path}")
                response.raise_for_status()
                effective = response.json().get("effective_policy_definition") or {}
                if all(effective.get(key) == value for key, value in definition.items()):
                    return
                await asyncio.sleep(0.05)
    except TimeoutError:
        raise RuntimeError(
            f"Dead-letter policy did not become effective on queue {queue_name!r}; "
            "check for conflicting RabbitMQ policies"
        ) from None


async def setup_rabbitmq() -> None:
    settings = Settings()
    async with create_management_client(settings) as client:
        await configure_dead_lettering(
            client,
            virtual_host=settings.rabbitmq_default_vhost,
            queue_name=settings.rabbitmq_delivery_queue,
        )
    print(
        f"Dead-letter routing configured: {settings.rabbitmq_delivery_queue} "
        f"-> {settings.rabbitmq_delivery_queue}.dlq"
    )


def main() -> None:
    asyncio.run(setup_rabbitmq())


if __name__ == "__main__":
    main()
