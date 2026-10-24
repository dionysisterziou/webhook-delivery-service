import asyncio
import json
from uuid import uuid4

import httpx
import pytest

from webhook_delivery_service.http_sender import HttpxWebhookSender
from webhook_delivery_service.models import WebhookDelivery
from webhook_delivery_service.worker import WebhookSendError


async def run_successful_http_send_test(status_code: int) -> None:
    received_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        received_requests.append(request)
        return httpx.Response(status_code=status_code)

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


@pytest.mark.parametrize("status_code", [200, 204])
def test_httpx_webhook_sender_sends_delivery(status_code: int) -> None:
    asyncio.run(run_successful_http_send_test(status_code))


async def run_http_status_failure_test(status_code: int, retryable: bool) -> None:
    async def handle_request(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=status_code,
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

        with pytest.raises(WebhookSendError) as caught:
            await sender.send(delivery)

    assert caught.value.retryable is retryable
    assert str(status_code) in str(caught.value)
    assert isinstance(caught.value.__cause__, httpx.HTTPStatusError)


@pytest.mark.parametrize(
    ("status_code", "retryable"),
    [
        (302, False),
        (400, False),
        (404, False),
        (408, True),
        (429, True),
        (499, False),
        (500, True),
        (503, True),
        (599, True),
    ],
)
def test_httpx_webhook_sender_classifies_http_failure(
    status_code: int, retryable: bool
) -> None:
    asyncio.run(run_http_status_failure_test(status_code, retryable))


async def run_http_request_failure_test(
    error_type: type[httpx.RequestError],
) -> None:
    original_error = None

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal original_error
        original_error = error_type("destination unavailable", request=request)
        raise original_error

    delivery = WebhookDelivery(
        id=uuid4(),
        target_url="https://example.com/webhooks",
        event_type="order.created",
        payload={"order_id": 123},
    )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        sender = HttpxWebhookSender(client)
        with pytest.raises(WebhookSendError) as caught:
            await sender.send(delivery)

    assert caught.value.retryable is True
    assert str(caught.value) == "destination unavailable"
    assert caught.value.__cause__ is original_error


@pytest.mark.parametrize("error_type", [httpx.ReadTimeout, httpx.ConnectError])
def test_httpx_webhook_sender_keeps_request_failures_retryable(
    error_type: type[httpx.RequestError],
) -> None:
    asyncio.run(run_http_request_failure_test(error_type))
