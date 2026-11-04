import asyncio
import sys
from collections.abc import AsyncGenerator, Coroutine
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema

from webhook_delivery_service.models import Base


@asynccontextmanager
async def isolated_database(
    session_class: type[AsyncSession] = AsyncSession,
) -> AsyncGenerator[async_sessionmaker[AsyncSession]]:
    from webhook_delivery_service.config import Settings

    schema_name = f"test_api_{uuid4().hex}"
    # Use a dedicated pool and schema; application tables are never modified.
    engine = create_async_engine(
        Settings().build_database_url(),
        execution_options={"schema_translate_map": {None: schema_name}},
        connect_args={"connect_timeout": 5},
    )
    schema_created = False

    try:
        async with engine.begin() as connection:
            await connection.execute(CreateSchema(schema_name))
            await connection.run_sync(Base.metadata.create_all)
        schema_created = True

        yield async_sessionmaker(engine, class_=session_class, expire_on_commit=False)
    finally:
        try:
            if schema_created:
                async with engine.begin() as connection:
                    await connection.execute(DropSchema(schema_name, cascade=True))
        finally:
            await engine.dispose()


@asynccontextmanager
async def isolated_api(
    monkeypatch: pytest.MonkeyPatch,
    session_class: type[AsyncSession] = AsyncSession,
    backend_pids: list[int] | None = None,
) -> AsyncGenerator[tuple[httpx.AsyncClient, async_sessionmaker[AsyncSession]]]:
    from webhook_delivery_service.database import get_database_session
    from webhook_delivery_service.main import app

    async with isolated_database(session_class) as sessions:

        async def override_database_session() -> AsyncGenerator[AsyncSession]:
            async with sessions() as session:
                if backend_pids is not None:
                    pid = await session.scalar(text("SELECT pg_backend_pid()"))
                    assert pid is not None
                    backend_pids.append(pid)
                yield session

        # Run the real FastAPI route and service, using real PostgreSQL sessions.
        # ASGITransport calls the app in-process; no Uvicorn server is needed.
        with monkeypatch.context() as patch:
            patch.setitem(
                app.dependency_overrides,
                get_database_session,
                override_database_session,
            )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://testserver",
            ) as client:
                yield client, sessions


def run_check(check: Coroutine[Any, Any, None]) -> None:
    loop_factory = asyncio.SelectorEventLoop if sys.platform == "win32" else None
    asyncio.run(check, loop_factory=loop_factory)
