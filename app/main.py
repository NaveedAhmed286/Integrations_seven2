"""
Main application entry point.
"""

import asyncio
import signal
import sys
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

from app.config import config
from app.logger import logger
from app.errors import ExternalServiceError, NormalizationError
from app.services.apify_service import ApifyService
from app.services.google_service import google_sheets_service
from app.memory_manager import MemoryManager
from app.normalizers.amazon import AmazonNormalizer


# ======================
# Service initialization
# ======================
apify_service = ApifyService()
memory_manager = MemoryManager()
normalizer = AmazonNormalizer()


# ======================
# App lifespan
# ======================
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Amazon Scraper System")

    # Initialize services with better error handling
    services_initialized = {
        "apify": False,
        "memory": False,
        "google_sheets": False
    }
    
    # Apify service (critical)
    try:
        await apify_service.initialize()
        services_initialized["apify"] = True
        logger.info("✅ Apify service initialized")
    except Exception as e:
        logger.error(f"❌ Apify init failed: {e}")
        # Don't crash - app can still handle some requests
    
    # Memory manager (critical)
    try:
        await memory_manager.initialize()
        services_initialized["memory"] = True
        logger.info("✅ Memory manager initialized")
    except Exception as e:
        logger.error(f"❌ Memory manager init failed: {e}")
        # Don't crash
    
    # Google Sheets (non-critical - can fail without breaking app)
    try:
        await google_sheets_service.initialize()
        services_initialized["google_sheets"] = google_sheets_service.is_available
        if google_sheets_service.is_available:
            logger.info("✅ Google Sheets service initialized")
        else:
            logger.warning("⚠️ Google Sheets service disabled (credentials issue)")
    except Exception as e:
        logger.warning(f"⚠️ Google Sheets init failed (non-critical): {e}")
        services_initialized["google_sheets"] = False
    
    # CRITICAL: Wait for services to stabilize before starting background tasks
    logger.info("⏳ Waiting 10 seconds for services to stabilize...")
    await asyncio.sleep(10)
    
    # Initialize retry queue but don't start processing immediately
    # (If you have retry queue, import it here)
    try:
        from app.queue.retry_queue import retry_queue
        app.state.retry_queue = retry_queue
        logger.info("✅ Retry queue initialized (not started)")
    except ImportError:
        logger.info("ℹ️ Retry queue not found, skipping")
    except Exception as e:
        logger.warning(f"⚠️ Retry queue init failed: {e}")
    
    logger.info(f"📊 Startup complete. Services: {services_initialized}")
    logger.info("🚀 Application is now ready to accept requests")
    
    yield

    # Shutdown sequence
    logger.info("Shutting down Amazon Scraper System")
    
    # Stop retry queue gracefully if it exists
    if hasattr(app.state, 'retry_queue'):
        try:
            await app.state.retry_queue.stop_processing()
            logger.info("✅ Retry queue stopped gracefully")
        except Exception as e:
            logger.error(f"Failed to stop retry queue: {e}")
    
    # Close Apify service
    try:
        await apify_service.close()
        logger.info("✅ Apify service closed")
    except Exception as e:
        logger.error(f"Failed to close Apify service: {e}")


# ======================
# FastAPI app
# ======================
app = FastAPI(
    title="Amazon Scraper API",
    description="Production-grade Amazon scraping system",
    version="1.0.0",
    lifespan=lifespan
)


# ======================
# Basic endpoints
# ======================
@app.get("/")
async def root():
    return {
        "service": "Amazon Scraper System",
        "version": "1.0.0",
        "status": "operational",
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/health")
async def health_check():
    """Health check for Railway - returns 200 even if some services are degraded"""
    # Simple health check - just check if web server is running
    # Don't check all services as Railway will kill container if any fails
    return JSONResponse(
        status_code=200,
        content={
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "message": "Service is running",
            "simple_check": True  # Indicate this is a simple check
        }
    )


@app.get("/health-detailed")
async def health_detailed():
    """Detailed health check (for internal use only)"""
    services = {
        "apify": apify_service.is_available,
        "memory": memory_manager.initialized,
        "redis": memory_manager.short_term.is_available,
        "postgres": memory_manager.long_term.is_available,
        "google_sheets": google_sheets_service.is_available
    }
    
    # Determine status
    critical_services_ok = services["apify"] or services["memory"]
    status = "healthy" if critical_services_ok else "degraded"

    return {
        "status": status,
        "services": services,
        "timestamp": datetime.utcnow().isoformat(),
        "message": "All services operational" if status == "healthy" else "Some services degraded"
    }


# ======================
# Readiness/Liveness endpoints
# ======================
@app.get("/ready")
async def readiness_check():
    """Kubernetes/Platform readiness probe"""
    return {
        "status": "ready",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/live")
async def liveness_check():
    """Kubernetes/Platform liveness probe"""
    return {
        "status": "alive",
        "timestamp": datetime.utcnow().isoformat()
    }


# ======================
# REAL SEARCH ENDPOINT
# ======================
@app.post("/api/v1/search")
async def search_amazon(request: Request):
    try:
        data = await request.json()

        keyword = data.get("keyword", "").strip()
        domain = data.get("domain", "com")
        max_results = min(data.get("max_results", 10), 50)

        if not keyword:
            raise HTTPException(status_code=400, detail="Keyword is required")

        raw_results = await apify_service.scrape_amazon_search(
            keyword, domain, max_results
        )

        normalized_results = []
        for raw in raw_results:
            try:
                product = normalizer.normalize_product(raw)
                normalized_results.append(product.dict())
            except NormalizationError as e:
                logger.warning(f"Normalization failed: {e}")

        # Use config.GOOGLE_SHEETS_SPREADSHEET_ID
        if google_sheets_service.is_available and normalized_results:
            await google_sheets_service.append_to_sheet(
                spreadsheet_id=config.GOOGLE_SHEETS_SPREADSHEET_ID,
                worksheet_name="Sheet1",
                data=normalized_results
            )

        return {
            "success": True,
            "keyword": keyword,
            "count": len(normalized_results),
            "results": normalized_results,
            "timestamp": datetime.utcnow().isoformat()
        }

    except ExternalServiceError:
        raise HTTPException(status_code=503, detail="External service unavailable")
    except Exception as e:
        logger.error(f"Search failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


# ======================
# MOCK TEST ENDPOINT (STEP 1 of POINT 9)
# ======================
@app.post("/api/v1/test-mock-sheet")
async def test_mock_sheet(request: Request):
    try:
        payload = await request.json()

        keyword = payload.get("keyword")
        domain_code = payload.get("domain_code", "com")
        results = payload.get("results", [])

        if not keyword or not results:
            raise HTTPException(status_code=400, detail="keyword and results required")

        rows = []

        for item in results:
            ai_result = await memory_manager.ai_analyze_product(
                product_data=item,
                keyword=keyword
            )

            rows.append({
                "timestamp": datetime.utcnow().isoformat(),
                "asin": item.get("asin"),
                "keyword": keyword,
                "domain_code": domain_code,
                "search_result_position": item.get("search_result_position"),
                "count_review": item.get("count_review"),
                "product_rating": item.get("product_rating"),
                "price": item.get("price"),
                "retail_price": item.get("retail_price"),
                "img_url": item.get("img_url"),
                "dp_url": item.get("dp_url"),
                "sponsored": item.get("sponsored"),
                "prime": item.get("prime"),
                "product_description": item.get("product_description"),
                "sales_volume": item.get("sales_volume"),
                "manufacturer": item.get("manufacturer"),
                "page": item.get("page"),
                "sort_strategy": item.get("sort_strategy"),
                "result_count": item.get("result_count"),
                "similar_keywords": ", ".join(item.get("similar_keywords", [])),
                "categories": ", ".join(item.get("categories", [])),
                "variations": str(item.get("variations")),
                "product_details": str(item.get("product_details")),
                "availability": item.get("availability"),
                "scraped_at": item.get("scraped_at"),
                "ai_recommendation": ai_result.get("recommendation"),
                "opportunity_score": ai_result.get("opportunity_score"),
                "key_advantages": ai_result.get("key_advantages"),
            })

        await google_sheets_service.append_rows(rows)

        return {
            "success": True,
            "rows_written": len(rows),
            "message": "Mock test successful"
        }

    except Exception as e:
        logger.error(f"Mock test failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Mock sheet test failed")


# ======================
# APIFY WEBHOOK (STEP 2 of POINT 9) - UPDATED FOR ACTOR-WEBHOOK
# ======================
@app.post("/api/v1/actor-webhook")
async def apify_webhook(payload: dict):
    logger.info(f"Received Apify webhook: {payload}")
    
    # Apify webhook structure: https://docs.apify.com/platform/integrations/webhooks
    dataset_id = payload.get("datasetId")
    actor_run_id = payload.get("runId")
    keyword = payload.get("keyword", "unknown")
    
    if not dataset_id:
        logger.warning("No datasetId in webhook payload")
        return {"status": "ignored", "reason": "No datasetId"}
    
    # Fetch the actual data from Apify dataset
    results = await apify_service.fetch_dataset(dataset_id)
    
    if not results:
        logger.warning(f"No results found in dataset {dataset_id}")
        return {"status": "empty"}
    
    logger.info(f"Processing {len(results)} items from dataset {dataset_id}")
    
    # Process each item
    rows = []
    for item in results:
        ai_result = await memory_manager.ai_analyze_product(item, keyword)
        rows.append({
            "timestamp": datetime.utcnow().isoformat(),
            "asin": item.get("asin"),
            "keyword": keyword,
            "ai_recommendation": ai_result.get("recommendation"),
            "opportunity_score": ai_result.get("opportunity_score"),
            "key_advantages": ai_result.get("key_advantages"),
        })
    
    # Write to Google Sheets
    if rows and google_sheets_service.is_available:
        await google_sheets_service.append_rows(rows)
        logger.info(f"Written {len(rows)} rows to Google Sheets")
    
    return {"status": "processed", "items": len(rows)}


# ======================
# Debug endpoint to test if app stays alive
# ======================
@app.get("/debug/alive")
async def debug_alive():
    """Debug endpoint to check if app is still running"""
    return {
        "alive": True,
        "timestamp": datetime.utcnow().isoformat(),
        "uptime_seconds": time.time() - app_start_time if 'app_start_time' in globals() else 0
    }


# ======================
# Local run
# ======================
if __name__ == "__main__":
    import os
    import time
    import uvicorn
    
    # Set app start time for debugging
    app_start_time = time.time()
    
    port = int(os.environ.get("PORT", 8080))  # Default to 8080 for Railway
    uvicorn.run(app, host="0.0.0.0", port=port)
