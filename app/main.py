from fastapi import FastAPI, Header, HTTPException, Request
from typing import Optional
import logging

app = FastAPI()

# 🔐 MUST match Apify webhook header value exactly
APIFY_WEBHOOK_SECRET = "a9K3pLq5Vn8R7sT2Xw6bY4dF1mC0zHjR"

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@app.post("/api/v1/actor-webhook")
async def apify_webhook(
    request: Request,
    x_apify_webhook_secret: Optional[str] = Header(default=None),
):
    """
    Apify webhook receiver
    """

    # 🔍 THIS LOG WILL PROVE HEADER ARRIVAL
    logger.info(f"Webhook header received: {x_apify_webhook_secret}")

    # ❌ Authorization check
    if x_apify_webhook_secret != APIFY_WEBHOOK_SECRET:
        logger.error("❌ Unauthorized webhook attempt")
        raise HTTPException(status_code=401, detail="Unauthorized")

    # ✅ Authorized
    payload = await request.json()

    dataset_id = payload.get("datasetId")
    run_id = payload.get("runId")
    status = payload.get("status")

    logger.info("✅ Webhook authorized")
    logger.info(f"📦 Dataset ID: {dataset_id}")
    logger.info(f"🏃 Run ID: {run_id}")
    logger.info(f"📊 Status: {status}")

    return {"ok": True}
