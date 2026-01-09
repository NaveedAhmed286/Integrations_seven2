"""
Webhook-only processor for Apify Web Scraper Actor
Author: Senior Engineer
Purpose: Handle Apify webhook, fetch dataset, process items, save to Google Sheets
"""

import asyncio
import json
import os
import time
from datetime import datetime

import requests
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.config import config
from app.logger import logger
from app.services.google_service import google_sheets_service
from app.memory_manager import memory_manager

# ======================
# SECURITY - TOKEN
# ======================
WEBHOOK_SECRET_TOKEN = "a9K3pLq5Vn8R7sT2Xw6bY4dF1mC0zHjR"  # Secure token for webhook auth

# ======================
# FastAPI app with CORS
# ======================
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Webhook Processor Starting")

    # Initialize memory manager
    try:
        await memory_manager.initialize()
        logger.info("✅ Memory manager initialized")
    except Exception as e:
        logger.error(f"❌ Memory manager init failed: {e}")

    # Initialize Google Sheets
    try:
        await google_sheets_service.initialize()
        logger.info("✅ Google Sheets service initialized successfully")
    except Exception as e:
        logger.warning(f"⚠️ Google Sheets init failed: {e}")

    yield

    logger.info("Shutting down Webhook Processor")
    await memory_manager.close() if hasattr(memory_manager, "close") else None


app = FastAPI(
    title="Apify Webhook Processor",
    version="1.0.0",
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ======================
# HEALTH CHECKS
# ======================
@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


# ======================
# WEBHOOK ENDPOINT
# ======================
@app.post("/api/v1/actor-webhook")
async def apify_webhook(request: Request):
    # 1. Check authorization header
    auth_header = request.headers.get("Authorization", "")
    if auth_header != f"Bearer {WEBHOOK_SECRET_TOKEN}":
        logger.warning(f"❌ Unauthorized webhook attempt. Header: {auth_header}")
        raise HTTPException(status_code=401, detail="Unauthorized")

    # 2. Parse JSON payload
    try:
        payload = await request.json()
    except Exception as e:
        logger.error(f"❌ Failed to parse webhook payload: {e}")
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    logger.info(f"📬 Received webhook event: {payload.get('eventType', 'unknown')}")

    # 3. Extract dataset and run info
    dataset_id = payload.get("datasetId") or payload.get("resource", {}).get("defaultDatasetId")
    run_id = payload.get("runId") or payload.get("resource", {}).get("id")
    keyword = payload.get("keyword") or payload.get("customData", {}).get("keyword", "unknown")

    if not dataset_id:
        logger.error("❌ No datasetId found in webhook payload")
        return {"status": "error", "message": "No datasetId found"}

    # 4. Fetch dataset from Apify
    try:
        headers = {"Authorization": f"Bearer {config.APIFY_API_KEY}"} if config.APIFY_API_KEY else {}
        dataset_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items"
        response = requests.get(dataset_url, headers=headers, timeout=30)
        if response.status_code != 200:
            logger.error(f"❌ Failed to fetch dataset {dataset_id}: {response.status_code}")
            return {"status": "error", "message": f"Dataset fetch failed: {response.status_code}"}
        items = response.json()
        if isinstance(items, dict) and "items" in items:
            items = items["items"]
        logger.info(f"✅ Fetched {len(items)} items from dataset {dataset_id}")
    except Exception as e:
        logger.error(f"❌ Dataset fetch exception: {e}")
        return {"status": "error", "message": str(e)}

    # 5. Process each item (simple AI analysis placeholder)
    rows = []
    for idx, item in enumerate(items[:100]):  # limit to 100 items
        try:
            asin = item.get("asin", "unknown")
            price = item.get("price") or 0
            rating = item.get("rating") or 0
            reviews = item.get("reviews") or 0

            # Simple opportunity score
            opportunity_score = 0
            if rating >= 4.5: opportunity_score += 30
            elif rating >= 4: opportunity_score += 20
            elif rating >= 3.5: opportunity_score += 10
            if reviews >= 1000: opportunity_score += 30
            elif reviews >= 500: opportunity_score += 20
            elif reviews >= 100: opportunity_score += 10
            if price and price < 50: opportunity_score += 20
            elif price and price < 100: opportunity_score += 10
            opportunity_score = min(opportunity_score, 100)

            if opportunity_score >= 70:
                recommendation = "High potential - Consider investing"
            elif opportunity_score >= 50:
                recommendation = "Moderate potential - Worth monitoring"
            else:
                recommendation = "Low potential - Continue research"

            row_data = {
                "timestamp": datetime.utcnow().isoformat(),
                "asin": asin,
                "keyword": keyword,
                "ai_recommendation": recommendation,
                "opportunity_score": opportunity_score,
                "Product_rating": rating,
                "count_review": reviews,
                "price": price,
                "run_id": run_id,
                "dataset_id": dataset_id
            }
            rows.append(row_data)
        except Exception as e:
            logger.warning(f"⚠️ Failed to process item {idx}: {e}")
            continue

    # 6. Save to Google Sheets
    sheets_success = False
    if rows and google_sheets_service.is_available:
        try:
            success = await google_sheets_service.append_to_sheet(
                spreadsheet_id=config.GOOGLE_SHEETS_SPREADSHEET_ID,
                worksheet_name="Sheet1",
                data=rows
            )
            sheets_success = success
            logger.info(f"✅ Written {len(rows)} rows to Google Sheets")
        except Exception as e:
            logger.error(f"❌ Google Sheets write failed: {e}")

    return {
        "status": "success",
        "items_fetched": len(items),
        "items_processed": len(rows),
        "rows_written": len(rows) if sheets_success else 0,
        "dataset_id": dataset_id,
        "run_id": run_id,
        "keyword": keyword,
        "google_sheets_success": sheets_success
    }


# ======================
# DEBUG
# ======================
@app.get("/debug/alive")
async def debug_alive():
    return {"alive": True, "timestamp": datetime.utcnow().isoformat()}


# ======================
# RUN LOCAL
# ======================
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
