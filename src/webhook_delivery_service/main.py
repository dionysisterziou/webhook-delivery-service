from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, status
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from webhook_delivery_service.database import (
    check_database_connection,
    get_database_session,
)
from webhook_delivery_service.models import WebhookDelivery
from webhook_delivery_service.schemas import (
    WebhookDeliveryCreate,
    WebhookDeliveryResponse,
)
from webhook_delivery_service.services import create_webhook_delivery

app = FastAPI(title="Webhook Delivery Service")


@app.get("/health/live")
async def liveness() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/ready")
async def readiness() -> dict[str, str]:
    try:
        database_is_read = await check_database_connection()
    except SQLAlchemyError:
        database_is_read = False

    if not database_is_read:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database is unavailable",
        )

    return {"status": "ok", "database": "ok"}


@app.post(
    "/deliveries",
    response_model=WebhookDeliveryResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_delivery(
    delivery_data: WebhookDeliveryCreate,
    session: Annotated[AsyncSession, Depends(get_database_session)],
) -> WebhookDelivery:
    return await create_webhook_delivery(
        session=session,
        delivery_data=delivery_data,
    )
