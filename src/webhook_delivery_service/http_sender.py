import httpx

from webhook_delivery_service.models import WebhookDelivery
from webhook_delivery_service.worker import WebhookSendError

WEBHOOK_REQUEST_TIMEOUT_SECONDS = 10.0


class HttpxWebhookSender:
    def __init__(
        self,
        client: httpx.AsyncClient,
    ) -> None:
        self._client = client

    async def send(
        self,
        delivery: WebhookDelivery,
    ) -> None:
        try:
            response = await self._client.post(
                delivery.target_url,
                json=delivery.payload,
                headers={
                    "X-Delivery-ID": str(delivery.id),
                    "X-Event-Type": delivery.event_type,
                },
                timeout=WEBHOOK_REQUEST_TIMEOUT_SECONDS,
            )

            response.raise_for_status()
        except httpx.HTTPStatusError as error:
            status_code = error.response.status_code
            retryable = status_code in {408, 429} or 500 <= status_code < 600
            raise WebhookSendError(str(error), retryable=retryable) from error
        except httpx.RequestError as error:
            raise WebhookSendError(str(error)) from error
