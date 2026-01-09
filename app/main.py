"""
Webhook-only Amazon Scraper Processor
Production-ready, event-driven
"""

import os
import json
import time
import requests
from datetime import datetime
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.config import config
from app.logger import logger
from app.services.google_service import google_sheets_service
from app.memory_manager import memory_manager

# ======================
# App lifespan
# ======================
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Webhook Processor Starting")

    await memory_manager.initialize()
    await google_sheets_service.initialize()

    yield

    logger.info("🛑 Webhook Processor Stopped")

# ======================
# FastAPI App
# ======================
app = FastAPI(
    title="Apify Webhook Processor",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ======================
# Health
# ======================
@app.get("/health")
async def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}

# ======================
# WEBHOOK ENDPOINT (SOURCE OF TRUTH)
# ======================
@app.post("/api/v1/actor-webhook")
async def apify_webhook(request: Request):
    # ---- Security (optional but recommended)
    secret = request.headers.get("X-Apify-Webhook-Secret")
    if secret != os.getenv("APIFY_WEBHOOK_SECRET", "apify_to_railway_prod"):
        raise HTTPException(status_code=401, detail="Invalid webhook secret")

    payload = await request.json()

    logger.info("📬 Apify webhook received")
    logger.info(json.dumps(payload, indent=2))

    # ---- REQUIRED FIELDS
    dataset_id = payload.get("datasetId")
    run_id = payload.get("runId")
    keyword = payload.get("keyword", "unknown")

    if not dataset_id:
        logger.error("❌ datasetId missing – webhook misconfigured")
        return {"status": "error", "reason": "datasetId missing"}

    # ======================
    # FETCH DATASET
    # ======================
    logger.info(f"📥 Fetching dataset {dataset_id}")

    dataset_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items"
    headers = {"Authorization": f"Bearer {config.APIFY_API_KEY}"}

    r = requests.get(dataset_url, headers=headers, timeout=60)
    r.raise_for_status()

    items = r.json()
    logger.info(f"✅ {len(items)} items fetched")

    # ======================
    # PROCESS + SAVE
    # ======================
    rows = []
    for item in items[:100]:
        rows.append({
            "timestamp": datetime.utcnow().isoformat(),
            "asin": item.get("asin"),
            "keyword": keyword,
            "title": item.get("title", "")[:200],
            "price": item.get("price"),
            "rating": item.get("rating"),
            "run_id": run_id,
            "dataset_id": dataset_id
        })

    if google_sheets_service.is_available and rows:
        await google_sheets_service.append_to_sheet(
            spreadsheet_id=config.GOOGLE_SHEETS_SPREADSHEET_ID,
            worksheet_name="Sheet1",
            data=rows
        )

    logger.info("🎯 Webhook processing complete")

    return {
        "status": "success",
        "items_processed": len(rows),
        "dataset_id": dataset_id,
        "run_id": run_id
    }

# ======================
# Local run
# ======================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
