""" Main application entry point. """
import asyncio
import signal
import sys
import time
import json
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from app.config import config
from app.logger import logger
from app.errors import ExternalServiceError, NormalizationError, RetryExhaustedError
from app.services.apify_service import ApifyService
from app.services.google_service import google_sheets_service
from app.memory_manager import memory_manager
from app.normalizers.amazon import AmazonNormalizer

# ======================
# Service initialization
# ======================
apify_service = ApifyService()
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
    
    logger.info(f"📊 Startup complete. Services: {services_initialized}")
    logger.info("🚀 Application is now ready to accept requests")
    
    yield
    
    # Shutdown sequence
    logger.info("Shutting down Amazon Scraper System")
    
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
        "service": "Amazon Scraper API",
        "version": "1.0.0",
        "status": "operational",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/health")
async def health_check():
    """
    SUPER SIMPLE HEALTH CHECK FOR RAILWAY
    Always returns 200 immediately - no service checks!
    """
    return JSONResponse(
        status_code=200,
        content={
            "status": "healthy",
            "message": "Service is running",
            "timestamp": datetime.utcnow().isoformat(),
            "simple_check": True
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
    """Kubernetes/Platform readiness probe - ALWAYS READY"""
    return JSONResponse(
        status_code=200,
        content={
            "status": "ready",
            "timestamp": datetime.utcnow().isoformat()
        }
    )

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
# APIFY WEBHOOK (IMPROVED VERSION)
# ======================
@app.post("/api/v1/actor-webhook")
async def apify_webhook(request: Request):
    """Handle Apify webhook with improved reliability."""
    try:
        # Log request metadata
        client_host = request.client.host if request.client else "unknown"
        logger.info("=" * 60)
        logger.info("🎯 WEBHOOK CALL RECEIVED")
        logger.info(f"From: {client_host}")
        
        # Get raw body for debugging
        body = await request.body()
        raw_text = body.decode('utf-8', errors='ignore') if body else ""
        
        if not raw_text.strip():
            logger.warning("⚠️ Empty request body received")
            return JSONResponse(
                status_code=400,
                content={"status": "error", "message": "Empty request body"}
            )
        
        # Parse JSON
        try:
            payload = json.loads(raw_text)
            logger.info(f"✅ JSON parsed. Keys: {list(payload.keys())}")
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON: {e}")
            logger.debug(f"Raw text: {raw_text[:500]}")
            return JSONResponse(
                status_code=400,
                content={"status": "error", "message": "Invalid JSON format"}
            )
        
        # Extract dataset ID (try multiple formats)
        dataset_id = None
        dataset_sources = [
            "datasetId", "defaultDatasetId", "dataset_id",
            "default_dataset_id", "dataSetId"
        ]
        
        for source in dataset_sources:
            if source in payload:
                dataset_id = payload[source]
                logger.info(f"🔍 Found dataset ID in '{source}': {dataset_id}")
                break
        
        # Also check nested in 'resource' if exists
        if not dataset_id and "resource" in payload:
            resource = payload["resource"]
            if isinstance(resource, dict):
                for source in dataset_sources:
                    if source in resource:
                        dataset_id = resource[source]
                        logger.info(f"🔍 Found dataset ID in resource.'{source}': {dataset_id}")
                        break
        
        if not dataset_id:
            logger.error("❌ No dataset ID found in payload")
            logger.debug(f"Full payload: {json.dumps(payload, indent=2)}")
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "message": "No dataset ID found",
                    "received_keys": list(payload.keys())
                }
            )
        
        # Extract other info
        run_id = payload.get("runId") or payload.get("actorRunId") or "unknown-run"
        keyword = payload.get("keyword", "unknown-keyword")
        status = payload.get("status", "unknown")
        results_count = payload.get("resultsCount") or payload.get("defaultDatasetItemCount") or 0
        
        logger.info(f"📊 Processing: dataset={dataset_id}, run={run_id}")
        logger.info(f"📊 Keyword: '{keyword}', Status: {status}, Expected items: {results_count}")
        
        # CRITICAL: Add delay before fetching (webhooks fire too early)
        initial_delay = 10  # Start with 10 seconds
        logger.info(f"⏳ Waiting {initial_delay} seconds for dataset to be ready...")
        await asyncio.sleep(initial_delay)
        
        # Fetch data from Apify with retry logic
        results = []
        max_retries = 6
        retry_delay = 5
        
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"🔄 Attempt {attempt}/{max_retries} to fetch dataset {dataset_id}")
                
                if attempt > 1:
                    # Exponential backoff for retries
                    wait_time = retry_delay * (2 ** (attempt - 2))
                    logger.info(f"   Waiting {wait_time} seconds before retry...")
                    await asyncio.sleep(wait_time)
                
                if apify_service.is_available:
                    results = await apify_service.fetch_dataset(dataset_id)
                    
                    if results:
                        logger.info(f"✅ Successfully fetched {len(results)} items on attempt {attempt}")
                        break
                    else:
                        logger.warning(f"Dataset {dataset_id} exists but is empty (attempt {attempt})")
                        continue
                else:
                    logger.error("❌ Apify service not available")
                    break
                    
            except Exception as e:
                logger.warning(f"Attempt {attempt} failed: {str(e)[:100]}")
                if attempt == max_retries:
                    logger.error(f"❌ All {max_retries} attempts failed for dataset {dataset_id}")
        
        # If no results after retries, try direct API call as last resort
        if not results and config.APIFY_API_KEY:
            logger.info("🆘 Trying direct API call as last resort...")
            try:
                import requests
                url = f"https://api.apify.com/v2/datasets/{dataset_id}/items"
                headers = {
                    "Authorization": f"Bearer {config.APIFY_API_KEY}",
                    "Content-Type": "application/json"
                }
                
                response = requests.get(url, headers=headers, timeout=30)
                if response.status_code == 200:
                    results = response.json()
                    logger.info(f"✅ Direct API call fetched {len(results)} items")
                else:
                    logger.error(f"Direct API failed: {response.status_code} - {response.text[:200]}")
            except Exception as e:
                logger.error(f"Direct API call also failed: {e}")
        
        if not results:
            logger.warning(f"⚠️ No results found in dataset {dataset_id} after all attempts")
            return JSONResponse(
                status_code=200,
                content={
                    "status": "no_data",
                    "message": f"Dataset {dataset_id} is empty or not accessible",
                    "dataset_id": dataset_id,
                    "attempts": max_retries,
                    "note": "Try running the actor again or check dataset in Apify Console"
                }
            )
        
        # ========== PROCESS RESULTS ==========
        logger.info(f"🔧 Processing {len(results)} items...")
        rows = []
        processed_count = 0
        
        for item in results:
            try:
                # Use simple AI analysis
                ai_result = await simple_ai_analysis(item, keyword)
                
                # Store in memory if available
                if memory_manager.initialized:
                    try:
                        # Store episodic memory
                        memory_manager.store_episodic(
                            client_id="webhook",
                            analysis_type="product_analysis",
                            input_data={"asin": item.get("asin"), "keyword": keyword},
                            output_data=ai_result,
                            insights=[ai_result.get("recommendation", "")]
                        )
                        
                        # Store long-term memory
                        await memory_manager.store_long_term(
                            client_id="webhook",
                            key=f"product_{item.get('asin', 'unknown')}_{keyword}",
                            value={
                                "asin": item.get("asin"),
                                "keyword": keyword,
                                "analysis": ai_result,
                                "timestamp": datetime.utcnow().isoformat()
                            },
                            source_analysis="webhook"
                        )
                    except Exception as e:
                        logger.error(f"Failed to store in memory: {e}")
                
                # Prepare row for Google Sheets
                rows.append({
                    "timestamp": datetime.utcnow().isoformat(),
                    "asin": item.get("asin", "unknown"),
                    "keyword": keyword,
                    "ai_recommendation": ai_result.get("recommendation", "Not analyzed"),
                    "opportunity_score": ai_result.get("opportunity_score", 0),
                    "Product_rating": ai_result.get("normalized_values", {}).get("rating", 0.0),
                    "count_review": ai_result.get("normalized_values", {}).get("reviews", 0),
                    "price": ai_result.get("normalized_values", {}).get("price", 0.0),
                    "sponsored": item.get("sponsored", False),
                    "analysis_type": ai_result.get("analysis_type", "standard"),
                    "processed_at": datetime.utcnow().isoformat()
                })
                processed_count += 1
                
            except Exception as e:
                logger.error(f"Failed to process item {item.get('asin', 'unknown')}: {e}")
                continue
        
        # Save to Google Sheets
        if rows and google_sheets_service.is_available:
            try:
                success = await google_sheets_service.append_to_sheet(
                    spreadsheet_id=config.GOOGLE_SHEETS_SPREADSHEET_ID,
                    worksheet_name="Sheet1",
                    data=rows
                )
                if success:
                    logger.info(f"✅ Written {len(rows)} rows to Google Sheets")
                else:
                    logger.error("Failed to write to Google Sheets")
            except Exception as e:
                logger.error(f"Google Sheets write failed: {e}")
        elif rows:
            logger.warning(f"⚠️ Google Sheets not available. {len(rows)} rows not saved.")
        
        logger.info(f"🎉 Webhook processing complete: {processed_count} items processed")
        
        return JSONResponse(
            status_code=200,
            content={
                "status": "success",
                "message": f"Processed {processed_count} items",
                "items_processed": processed_count,
                "rows_written": len(rows),
                "dataset_id": dataset_id,
                "keyword": keyword,
                "services": {
                    "apify": apify_service.is_available,
                    "memory": memory_manager.initialized,
                    "google_sheets": google_sheets_service.is_available
                },
                "timestamp": datetime.utcnow().isoformat()
            }
        )
    
    except Exception as e:
        logger.error(f"💥 Webhook processing failed: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": str(e)[:200],
                "timestamp": datetime.utcnow().isoformat()
            }
        )

async def simple_ai_analysis(product_data: dict, keyword: str) -> dict:
    """Simple AI analysis fallback."""
    try:
        # Log start of analysis
        logger.info(f"🔍 Starting AI analysis for ASIN: {product_data.get('asin', 'unknown')}")
        
        # Extract from your data structure
        rating_raw = product_data.get("rating") or product_data.get("product_rating") or 0
        reviews_raw = product_data.get("reviews") or product_data.get("count_review") or 0
        price_raw = product_data.get("price") or 0
        
        # Convert rating to float
        if isinstance(rating_raw, str):
            import re
            numbers = re.findall(r'\d+\.?\d*', rating_raw)
            rating = float(numbers[0]) if numbers else 0.0
        elif isinstance(rating_raw, (int, float)):
            rating = float(rating_raw)
        else:
            rating = 0.0
        
        # Convert reviews to integer
        if isinstance(reviews_raw, str):
            import re
            numbers = re.findall(r'\d+', reviews_raw.replace(',', ''))
            reviews = int(numbers[0]) if numbers else 0
        elif isinstance(reviews_raw, (int, float)):
            reviews = int(reviews_raw)
        else:
            reviews = 0
        
        # Convert price to float
        price = 0.0
        if price_raw:
            if isinstance(price_raw, str):
                import re
                numbers = re.findall(r'\d+\.?\d*', price_raw.replace(',', ''))
                if numbers:
                    price = float(numbers[0])
            elif isinstance(price_raw, (int, float)):
                price = float(price_raw)
        
        # Calculate opportunity score (0-100)
        opportunity_score = 0
        
        if rating >= 4.5:
            opportunity_score += 30
        elif rating >= 4.0:
            opportunity_score += 20
        elif rating >= 3.5:
            opportunity_score += 10
        
        if reviews >= 1000:
            opportunity_score += 30
        elif reviews >= 500:
            opportunity_score += 20
        elif reviews >= 100:
            opportunity_score += 10
        
        if price and price < 50:
            opportunity_score += 20
        elif price and price < 100:
            opportunity_score += 10
        
        # Cap at 100
        opportunity_score = min(opportunity_score, 100)
        
        # Generate recommendation
        if opportunity_score >= 70:
            recommendation = "High potential - Consider investing"
            analysis_type = "high_potential"
        elif opportunity_score >= 50:
            recommendation = "Moderate potential - Worth monitoring"
            analysis_type = "moderate_potential"
        else:
            recommendation = "Low potential - Continue research"
            analysis_type = "low_potential"
        
        # Log completion of analysis
        logger.info(f"✅ AI analysis completed. Score: {opportunity_score}/100 for ASIN: {product_data.get('asin', 'unknown')}")
        
        return {
            "recommendation": recommendation,
            "opportunity_score": opportunity_score,
            "key_advantages": f"Rating: {rating}, Reviews: {reviews}, Price: {price}",
            "analysis_type": analysis_type,
            "normalized_values": {
                "rating": rating,
                "reviews": reviews,
                "price": price
            }
        }
    
    except Exception as e:
        # Log failure
        logger.error(f"❌ AI analysis failed for {product_data.get('asin', 'unknown')}: {e}")
        return {
            "recommendation": "Analysis failed",
            "opportunity_score": 0,
            "key_advantages": "Not available",
            "analysis_type": "failed",
            "normalized_values": {
                "rating": 0,
                "reviews": 0,
                "price": 0
            }
        }

# ======================
# Debug endpoints
# ======================
@app.get("/debug/alive")
async def debug_alive():
    """Debug endpoint to check if app is still running"""
    return {
        "alive": True,
        "timestamp": datetime.utcnow().isoformat(),
        "uptime_seconds": time.time() - app_start_time if 'app_start_time' in globals() else 0
    }

@app.get("/debug/memory")
async def debug_memory():
    """Debug memory manager"""
    return {
        "initialized": memory_manager.initialized,
        "short_term_available": memory_manager.short_term.is_available,
        "long_term_available": memory_manager.long_term.is_available,
        "episodic_count": len(memory_manager.episodic.memories.get("webhook", [])),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/debug/services")
async def debug_services():
    """Check status of all services."""
    return {
        "apify_available": apify_service.is_available,
        "memory_available": memory_manager.initialized,
        "google_sheets_available": google_sheets_service.is_available,
        "spreadsheet_id": config.GOOGLE_SHEETS_SPREADSHEET_ID,
        "apify_api_key_set": bool(config.APIFY_API_KEY),
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/debug/google-sheets")
async def debug_google_sheets():
    """Debug Google Sheets connection."""
    try:
        if not google_sheets_service.is_available:
            return {"error": "Google Sheets service not available"}
        
        # Test with minimal data matching your sheet columns
        test_data = [{
            "timestamp": datetime.utcnow().isoformat(),
            "asin": "TEST123",
            "keyword": "test",
            "ai_recommendation": "Test recommendation",
            "opportunity_score": 50,
            "Product_rating": 4.5,
            "count_review": 100,
            "price": 29.99,
            "sponsored": False,
            "analysis_type": "test_analysis",
            "processed_at": datetime.utcnow().isoformat()
        }]
        
        result = await google_sheets_service.append_to_sheet(
            spreadsheet_id=config.GOOGLE_SHEETS_SPREADSHEET_ID,
            worksheet_name="Sheet1",
            data=test_data
        )
        return {"success": True, "rows_appended": result}
    except Exception as e:
        return {
            "error": str(e),
            "spreadsheet_id": config.GOOGLE_SHEETS_SPREADSHEET_ID,
            "worksheet_name": "Sheet1",
            "error_type": type(e).__name__
        }

# ======================
# Webhook Debug Endpoint
# ======================
@app.post("/api/v1/debug-webhook")
async def debug_webhook(request: Request):
    """Debug endpoint to test webhook payloads."""
    body = await request.body()
    raw_text = body.decode('utf-8', errors='ignore') if body else ""
    
    logger.info("=" * 70)
    logger.info("🐛 DEBUG WEBHOOK CALLED")
    logger.info(f"Raw body: {raw_text}")
    
    try:
        payload = json.loads(raw_text)
        return {
            "status": "debug",
            "received": True,
            "payload_keys": list(payload.keys()),
            "dataset_id": payload.get("datasetId") or payload.get("defaultDatasetId"),
            "payload_sample": {k: v for k, v in payload.items() if not isinstance(v, (dict, list)) or k == "sampleData"},
            "timestamp": datetime.utcnow().isoformat()
        }
    except:
        return {
            "status": "debug",
            "received": True,
            "raw_body": raw_text[:500],
            "timestamp": datetime.utcnow().isoformat()
        }

# ======================
# Local run
# ======================
if __name__ == "__main__":
    import os
    import uvicorn
    
    # Set app start time for debugging
    app_start_time = time.time()
    
    port = int(os.environ.get("PORT", 8080))  # Default to 8080 for Railway
    uvicorn.run(app, host="0.0.0.0", port=port)
