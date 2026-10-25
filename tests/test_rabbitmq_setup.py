import asyncio
import json
import re
from urllib.parse import quote

import httpx
import pytest

from webhook_delivery_service.rabbitmq_setup import configure_dead_lettering


async def check_dead_letter_configuration(virtual_host: str, queue_name: str) -> None:
    requests: list[httpx.Request] = []
    policy_reads = 0
    definition = {
        "dead-letter-exchange": f"{queue_name}.dlx",
        "dead-letter-routing-key": queue_name,
    }

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal policy_reads
        requests.append(request)
        if request.method == "GET":
            policy_reads += 1
            # Exercise waiting for propagation, not just the policy PUT response.
            effective = {} if policy_reads == 1 else definition
            return httpx.Response(200, json={"effective_policy_definition": effective})
        return httpx.Response(201)

    async with httpx.AsyncClient(
        base_url="http://rabbitmq.test", transport=httpx.MockTransport(handle)
    ) as client:
        await configure_dead_lettering(
            client, virtual_host=virtual_host, queue_name=queue_name
        )

    vhost = quote(virtual_host, safe="")
    queue_path = quote(queue_name, safe="")
    exchange_path = quote(f"{queue_name}.dlx", safe="")
    dead_queue_path = quote(f"{queue_name}.dlq", safe="")
    policy_path = quote(f"{queue_name}.dead-letter", safe="")
    expected = [
        ("PUT", f"/api/exchanges/{vhost}/{exchange_path}"),
        ("PUT", f"/api/queues/{vhost}/{dead_queue_path}"),
        (
            "POST",
            f"/api/bindings/{vhost}/e/{exchange_path}/q/{dead_queue_path}",
        ),
        ("PUT", f"/api/queues/{vhost}/{queue_path}"),
        ("PUT", f"/api/policies/{vhost}/{policy_path}"),
    ]
    assert [(r.method, r.url.raw_path.decode()) for r in requests[:5]] == expected
    assert all(r.method in {"PUT", "POST", "GET"} for r in requests)
    assert json.loads(requests[0].content)["type"] == "direct"
    for index in (0, 1, 3):
        assert json.loads(requests[index].content)["durable"] is True
    for index in (1, 3):
        assert json.loads(requests[index].content)["arguments"] == {}
    assert json.loads(requests[2].content)["routing_key"] == queue_name

    policy = json.loads(requests[4].content)
    assert policy["definition"] == definition
    assert policy["apply-to"] == "queues"
    assert re.fullmatch(policy["pattern"], queue_name)
    assert not re.fullmatch(policy["pattern"], f"{queue_name}.dlq")
    assert not re.fullmatch(policy["pattern"], f"unrelated.{queue_name}")
    assert not re.fullmatch(policy["pattern"], queue_name.replace(".", "x"))
    assert policy_reads == 2


@pytest.mark.parametrize(
    ("virtual_host", "queue_name"),
    [("webhook_delivery", "webhook.deliveries"), ("/", "webhook.deliveries+[test]")],
)
def test_dead_letter_policy_targets_only_the_delivery_queue(
    virtual_host: str, queue_name: str
) -> None:
    asyncio.run(check_dead_letter_configuration(virtual_host, queue_name))


async def check_setup_failure() -> None:
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(403, json={"error": "not_authorised"})

    async with httpx.AsyncClient(
        base_url="http://rabbitmq.test", transport=httpx.MockTransport(handle)
    ) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await configure_dead_lettering(
                client, virtual_host="webhook_delivery", queue_name="webhook.deliveries"
            )
    assert len(requests) == 1


def test_setup_stops_when_the_management_api_rejects_a_request() -> None:
    asyncio.run(check_setup_failure())
