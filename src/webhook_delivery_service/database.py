from collections.abc import AsyncIterator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from webhook_delivery_service.config import Settings

settings = Settings()
database_url = settings.build_database_url()

engine: AsyncEngine = create_async_engine(database_url, pool_pre_ping=True)

async_session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
)


async def get_database_session() -> AsyncIterator[AsyncSession]:
    async with async_session_factory() as session:
        yield session


async def check_database_connection() -> bool:
    async with engine.connect() as connection:
        result = await connection.scalar(text("SELECT 1"))

    return result == 1
