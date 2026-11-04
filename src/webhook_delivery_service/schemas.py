from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

EVENT_TYPE_REQUIREMENT = (
    "event_type must contain 1 to 255 printable ASCII characters with no leading or trailing spaces"
)
EventType = Annotated[
    str,
    Field(
        strict=True,
        min_length=1,
        max_length=255,
        # The first and last characters must be visible ASCII. Spaces are valid inside.
        pattern=r"^[!-~](?:[ -~]*[!-~])?$",
        description=EVENT_TYPE_REQUIREMENT,
    ),
]


class WebhookDeliveryCreate(BaseModel):
    target_url: HttpUrl
    event_type: EventType
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
