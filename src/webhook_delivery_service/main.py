from fastapi import FastAPI

app = FastAPI(title="Webhook Delivery Service")


@app.get("/health/live")
async def liveness() -> dict[str, str]:
    return {"status": "ok"}
