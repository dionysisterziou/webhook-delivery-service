from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from webhook_delivery_service.database import get_database_session
from webhook_delivery_service.main import app


def test_metrics_returns_each_status_count(monkeypatch: pytest.MonkeyPatch) -> None:
    counts = {"pending": 5, "processing": 4, "retry_scheduled": 3, "succeeded": 2, "failed": 1}
    session = MagicMock(spec=AsyncSession)
    result = MagicMock()
    result.mappings.return_value.one.return_value = counts | {"unpublished_outbox_messages": 6}
    session.execute.return_value = result
    monkeypatch.setitem(app.dependency_overrides, get_database_session, lambda: session)

    with TestClient(app) as client:
        response = client.get("/metrics")

    assert response.status_code == 200
    assert response.json() == {
        "deliveries_by_status": counts,
        "unpublished_outbox_messages": 6,
    }


def test_metrics_returns_service_unavailable_on_query_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = MagicMock(spec=AsyncSession)
    session.execute.side_effect = SQLAlchemyError("query failed")
    monkeypatch.setitem(app.dependency_overrides, get_database_session, lambda: session)

    with TestClient(app) as client:
        response = client.get("/metrics")

    assert response.status_code == 503
    assert response.json() == {"detail": "Database is unavailable"}
