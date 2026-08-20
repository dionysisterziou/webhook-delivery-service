import asyncio
import json
from uuid import uuid4

import httpx
import pytest

from webhook_delivery_service.http_sender import HttpxWebhookSender
from webhook_delivery_service.models import WebhookDelivery
from webhook_delivery_service.worker import WebhookSendError


async def run_successful_http_send_test() -> None:
    received_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        received_requests.append(request)
        return httpx.Response(status_code=204)

    transport = httpx.MockTransport(handler)

    delivery_id = uuid4()
    delivery = WebhookDelivery(
        id=delivery_id,
        target_url="https://example.com/webhooks",
        event_type="order.created",
        payload={"order_id": 123},
        status="pending",
        attempt_count=0,
    )

    async with httpx.AsyncClient(transport=transport) as client:
        sender = HttpxWebhookSender(client)

        await sender.send(delivery)

    assert len(received_requests) == 1

    request = received_requests[0]

    assert request.method == "POST"
    assert str(request.url) == "https://example.com/webhooks"
    assert json.loads(request.content) == {"order_id": 123}
    assert request.headers["X-Delivery-ID"] == str(delivery_id)
    assert request.headers["X-Event-Type"] == "order.created"


def test_httpx_webhook_sender_sends_delivery() -> None:
    asyncio.run(run_successful_http_send_test())


async def run_httpx_webhook_sender_server_error() -> None:
    async def handle_request(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=500,
            request=request,
        )

    delivery = WebhookDelivery(
        id=uuid4(),
        target_url="https://example.com/webhooks",
        event_type="order.created",
        payload={"order_id": 123},
    )

    transport = httpx.MockTransport(handle_request)

    async with httpx.AsyncClient(transport=transport) as client:
        sender = HttpxWebhookSender(client)

        with pytest.raises(WebhookSendError):
            await sender.send(delivery)


def test_httpx_webhook_sender_raises_for_server_error() -> None:
    asyncio.run(run_httpx_webhook_sender_server_error())
