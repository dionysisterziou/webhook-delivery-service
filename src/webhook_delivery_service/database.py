from sqlalchemy import URL, text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from webhook_delivery_service.config import Settings

settings = Settings()

database_url = URL.create(
    drivername="postgresql+psycopg",
    username=settings.postgres_user,
    password=settings.postgres_password.get_secret_value(),
    host=settings.postgres_host,
    port=settings.postgres_port,
    database=settings.postgres_db,
)

engine: AsyncEngine = create_async_engine(database_url, pool_pre_ping=True)


async def check_database_connection() -> bool:
    async with engine.connect() as connection:
        result = await connection.scalar(text("SELECT 1"))

    return result == 1
