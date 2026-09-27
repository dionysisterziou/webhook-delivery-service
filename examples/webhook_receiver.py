from typing import Any

from fastapi import FastAPI, Header, HTTPException

app = FastAPI(title="Local Webhook Receiver")

# In-memory state for the manual retry demo; use one receiver process.
attempts_by_delivery: dict[str, int] = {}


@app.post("/webhooks")
async def receive_webhook(
    payload: dict[str, Any],
    x_delivery_id: str = Header(),
    x_event_type: str = Header(),
) -> dict[str, bool]:
    print(
        {
            "delivery_id": x_delivery_id,
            "event_type": x_event_type,
            "payload": payload,
        },
        flush=True,
    )

    return {"received": True}


@app.post("/webhooks/fail-once")
async def receive_webhook_with_first_attempt_failure(
    payload: dict[str, Any],
    x_delivery_id: str = Header(),
    x_event_type: str = Header(),
) -> dict[str, bool]:
    attempt = attempts_by_delivery.get(x_delivery_id, 0) + 1
    attempts_by_delivery[x_delivery_id] = attempt
    status_code = 503 if attempt == 1 else 200

    print(
        {
            "delivery_id": x_delivery_id,
            "event_type": x_event_type,
            "payload": payload,
            "receiver_attempt": attempt,
            "status_code": status_code,
        },
        flush=True,
    )

    if attempt == 1:
        raise HTTPException(
            status_code=503,
            detail="Intentional first-attempt failure for the retry demo",
        )

    return {"received": True}
