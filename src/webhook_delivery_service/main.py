from fastapi import FastAPI, HTTPException, status
from sqlalchemy.exc import SQLAlchemyError

from webhook_delivery_service.database import check_database_connection

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
