from typing import Any

from fastapi import FastAPI, Header

app = FastAPI(title="Local Webhook Receiver")


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
