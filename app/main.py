"""
Webhook-only processor for Apify Web Scraper Actor
Author: Senior Engineer
Purpose: Handle Apify webhook, fetch dataset, process items, save to Google Sheets
"""

import asyncio
import json
import os
from datetime import datetime

import requests
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.config import config
from app.logger import logger
from app.services.google_service import google_sheets_service
from app.memory_manager import memory_manager

# ======================
# SECURITY - WEBHOOK TOKEN
# ======================
WEBHOOK_SECRET_TOKEN = "a9K3pLq5Vn8R7sT2Xw6bY4dF1mC0zHjR"  # <-- Your Apify webhook secret

# ======================
# FastAPI app with lifespan
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

    logger.info("🛑 Shutting down Webhook Processor")
    await memory_manager.close() if hasattr(memory_manager, "close") else None


app = FastAPI(
    title="Apify Webhook Processor",
    version="1.0.0",
    lifespan=lifespan
)

# ---- CORS ----
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ======================
# HEALTH CHECK
# ======================
@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}


# ======================
# WEBHOOK ENDPOINT
# ======================
@app.post("/api/v1/actor-webhook")
async def apify_webhook(request: Request):
    # 1. Validate token in Authorization header - FLEXIBLE VERSION
    auth_header = request.headers.get("Authorization", "")
    
    # Extract token from header (supports both "Bearer <token>" and "<token>" formats)
    received_token = None
    
    if auth_header.startswith("Bearer "):
        received_token = auth_header[7:]  # Remove "Bearer " prefix
        logger.info(f"✅ Received Authorization header with 'Bearer' prefix")
    else:
        received_token = auth_header  # Assume it's just the token
        logger.info(f"✅ Received Authorization header without 'Bearer' prefix")
    
    # Compare with expected token
    if received_token != WEBHOOK_SECRET_TOKEN:
        logger.warning(f"❌ Unauthorized webhook attempt.")
        logger.warning(f"❌ Full Authorization header: '{auth_header}'")
        logger.warning(f"❌ Extracted token: '{received_token}'")
        logger.warning(f"❌ Expected token: '{WEBHOOK_SECRET_TOKEN}'")
        raise HTTPException(status_code=401, detail="Unauthorized")
    
    logger.info(f"✅ Authorization successful!")

    # 2. Parse JSON payload
    try:
        payload = await request.json()
    except Exception as e:
        logger.error(f"❌ Failed to parse webhook payload: {e}")
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    # === CRITICAL DEBUG: Log FULL Apify payload structure ===
    logger.info("=" * 70)
    logger.info("🔬 DEBUG: FULL APIFY WEBHOOK PAYLOAD STRUCTURE:")
    logger.info(json.dumps(payload, indent=2))
    logger.info("🔬 DEBUG: Checking for datasetId in all possible locations:")
    
    # Check all possible locations
    if "datasetId" in payload:
        logger.info(f"   ✅ Found: payload.datasetId = {payload.get('datasetId')}")
    else:
        logger.info("   ❌ Not found: payload.datasetId")
    
    if "resource" in payload:
        resource = payload.get("resource", {})
        if "defaultDatasetId" in resource:
            logger.info(f"   ✅ Found: payload.resource.defaultDatasetId = {resource.get('defaultDatasetId')}")
        else:
            logger.info("   ❌ Not found: payload.resource.defaultDatasetId")
            
        # Also check other possible resource fields
        logger.info(f"   🔍 payload.resource keys: {list(resource.keys())}")
    
    # Also check other possible locations
    logger.info(f"   🔍 All top-level payload keys: {list(payload.keys())}")
    logger.info("=" * 70)
    # === END DEBUG ===

    logger.info(f"📬 Received webhook event: {payload.get('eventType', 'unknown')}")

    # 3. Extract dataset and run info - ENHANCED VERSION
    dataset_id = None
    
    # Try multiple possible locations
    possible_dataset_paths = [
        payload.get("datasetId"),
        payload.get("resource", {}).get("defaultDatasetId"),
        payload.get("resource", {}).get("datasetId"),
        payload.get("data", {}).get("datasetId"),
        payload.get("details", {}).get("datasetId"),
        payload.get("runId"),  # Sometimes runId is used to fetch data
    ]
    
    for i, dataset in enumerate(possible_dataset_paths):
        if dataset:
            dataset_id = dataset
            logger.info(f"✅ Found datasetId at position {i}: {dataset_id}")
            break
    
    # If still not found, check the entire payload structure
    if not dataset_id:
        logger.error("❌ Could not find datasetId in any expected location")
        # Log the entire payload for debugging
        logger.error(f"❌ Full payload structure: {json.dumps(payload, indent=2)}")
        return {"status": "error", "message": "No datasetId found in webhook payload"}

    run_id = payload.get("runId") or payload.get("resource", {}).get("id")
    keyword = payload.get("keyword") or payload.get("customData", {}).get("keyword", "unknown")

    if not dataset_id:
        logger.error("❌ No datasetId found in webhook payload")
        return {"status": "error", "message": "No datasetId found"}

    # 4. Fetch dataset from Apify
    try:
        headers = {"Authorization": f"Bearer {config.APIFY_API_KEY}"} if config.APIFY_API_KEY else {}
        dataset_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?format=json"
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

    # 5. Process items (simple AI/score calculation)
    rows = []
    for idx, item in enumerate(items[:100]):  # limit to 100 items
        try:
            asin = item.get("asin", "unknown")
            price = item.get("price") or 0
            rating = item.get("rating") or 0
            reviews = item.get("reviews") or 0

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

    # 6. Write to Google Sheets
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
# DEBUG ENDPOINT
# ======================
@app.get("/debug/alive")
async def debug_alive():
    return {"alive": True, "timestamp": datetime.utcnow().isoformat()}


# ======================
# RUN LOCAL / DEPLOY
# ======================
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
