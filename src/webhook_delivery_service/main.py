from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException, Response, status
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
from webhook_delivery_service.services import (
    IdempotencyConflictError,
    create_idempotent_webhook_delivery,
    create_webhook_delivery,
    get_webhook_delivery,
)

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
    responses={
        200: {
            "model": WebhookDeliveryResponse,
            "description": "Existing delivery for the same Idempotency-Key and request",
        },
        409: {"description": "Idempotency-Key was used with a different request"},
    },
)
async def create_delivery(
    delivery_data: WebhookDeliveryCreate,
    session: Annotated[AsyncSession, Depends(get_database_session)],
    response: Response,
    idempotency_key: Annotated[
        str | None,
        Header(min_length=1, max_length=255),
    ] = None,
) -> WebhookDelivery:
    if idempotency_key is None:
        return await create_webhook_delivery(
            session=session,
            delivery_data=delivery_data,
        )

    try:
        delivery, created = await create_idempotent_webhook_delivery(
            session=session,
            delivery_data=delivery_data,
            idempotency_key=idempotency_key,
        )
    except IdempotencyConflictError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(error),
        ) from error

    response.status_code = status.HTTP_201_CREATED if created else status.HTTP_200_OK
    return delivery


@app.get(
    "/deliveries/{delivery_id}",
    response_model=WebhookDeliveryResponse,
)
async def get_delivery(
    delivery_id: UUID,
    session: Annotated[AsyncSession, Depends(get_database_session)],
) -> WebhookDelivery:
    delivery = await get_webhook_delivery(
        session=session,
        delivery_id=delivery_id,
    )

    if delivery is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Webhook delivery not found",
        )

    return delivery
