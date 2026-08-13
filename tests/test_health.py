from fastapi.testclient import TestClient

from webhook_delivery_service.main import app

client = TestClient(app)


def test_liveness_returns_ok() -> None:
    response = client.get("/health/live")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
