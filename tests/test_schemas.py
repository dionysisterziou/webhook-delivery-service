from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from pydantic import ValidationError

from webhook_delivery_service.schemas import (
    WebhookDeliveryCreate,
    WebhookDeliveryResponse,
)


def test_webhook_delivery_create_accepts_valid_input() -> None:
    request = WebhookDeliveryCreate(
        target_url="https://example.com/webhooks",
        event_type="order.created",
        payload={"order_id": 123},
    )

    assert request.target_url.scheme == "https"
    assert request.target_url.host == "example.com"
    assert request.event_type == "order.created"
    assert request.payload == {"order_id": 123}


def test_webhook_delivery_create_rejects_invalid_url() -> None:
    with pytest.raises(ValidationError):
        WebhookDeliveryCreate(
            target_url="not-a-url",
            event_type="order.created",
            payload={"order_id": 123},
        )


def test_webhook_delivery_response_reads_object_attributes() -> None:
    delivery_id = uuid4()
    created_at = datetime(2026, 8, 15, 20, 0, tzinfo=UTC)

    delivery = SimpleNamespace(
        id=delivery_id,
        target_url="https://example.com/webhooks",
        event_type="order.created",
        payload={"order_id": 123},
        status="pending",
        attempt_count=0,
        next_attempt_at=None,
        last_error=None,
        created_at=created_at,
        delivered_at=None,
    )

    response = WebhookDeliveryResponse.model_validate(delivery)

    assert response.id == delivery_id
    assert str(response.target_url) == "https://example.com/webhooks"
    assert response.event_type == "order.created"
    assert response.payload == {"order_id": 123}
    assert response.status == "pending"
    assert response.attempt_count == 0
    assert response.created_at == created_at
