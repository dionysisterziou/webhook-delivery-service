from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl


class WebhookDeliveryCreate(BaseModel):
    target_url: HttpUrl
    event_type: str = Field(min_length=1, max_length=255)
    payload: dict[str, Any]


class WebhookDeliveryResponse(BaseModel):
    id: UUID
    target_url: HttpUrl
    event_type: str
    payload: dict[str, Any]
    status: str
    attempt_count: int
    next_attempt_at: datetime | None
    last_error: str | None
    created_at: datetime
    delivered_at: datetime | None

    model_config = ConfigDict(from_attributes=True)
