import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import SQLAlchemyError

from webhook_delivery_service import main

client = TestClient(main.app)


def test_liveness_returns_ok() -> None:
    response = client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_readiness_returns_ok(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def successful_database_check() -> bool:
        return True

    monkeypatch.setattr(
        main,
        "check_database_connection",
        successful_database_check,
    )

    response = client.get("/health/ready")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "database": "ok",
    }


def test_readiness_returns_service_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def failed_database_check() -> bool:
        raise SQLAlchemyError("Database unavailable")

    monkeypatch.setattr(
        main,
        "check_database_connection",
        failed_database_check,
    )

    response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json() == {
        "detail": "Database is unavailable",
    }
