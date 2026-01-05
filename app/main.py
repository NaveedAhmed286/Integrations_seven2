""" Main application entry point. """
import asyncio
import signal
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from app.config import config
from app.logger import logger
from app.errors import ExternalServiceError, NormalizationError
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
# APIFY WEBHOOK
# ======================
@app.post("/api/v1/actor-webhook")
async def apify_webhook(payload: dict):
    """Handle Apify webhook."""
    try:
        logger.info(f"📬 Received Apify webhook: {payload}")
        
        dataset_id = payload.get("datasetId")
        actor_run_id = payload.get("runId")
        keyword = payload.get("keyword", "unknown")
        
        if not dataset_id:
            logger.warning("No datasetId in webhook payload")
            return {"status": "ignored", "reason": "No datasetId"}
        
        # Fetch the actual data from Apify dataset
        results = []
        if apify_service.is_available:
            try:
                results = await apify_service.fetch_dataset(dataset_id)
                logger.info(f"Fetched {len(results)} results from Apify dataset {dataset_id}")
            except Exception as e:
                logger.error(f"Failed to fetch from Apify dataset: {e}")
                results = []
        else:
            logger.warning("Apify service not available, skipping data fetch")
        
        if not results:
            logger.warning(f"No results found in dataset {dataset_id}")
            return {"status": "empty"}
        
        # Process each item with error handling
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
                
                # Prepare row for Google Sheets - FIXED COLUMNS
                rows.append({
                    "timestamp": datetime.utcnow().isoformat(),
                    "asin": item.get("asin", "unknown"),
                    "keyword": keyword,
                    "ai_recommendation": ai_result.get("recommendation", "Not analyzed"),
                    "opportunity_score": ai_result.get("opportunity_score", 0),
                    "key_advantages": ai_result.get("key_advantages", "Not available"),
                    "analysis_type": ai_result.get("analysis_type", "standard")  # FIXED: Added analysis_type
                })
                processed_count += 1
                
            except Exception as e:
                logger.error(f"Failed to process item: {e}")
                continue  # Skip this item, continue with others
        
        # FIXED: Changed from append_rows to append_to_sheet
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
        
        return {
            "status": "processed",
            "items": processed_count,
            "rows_written": len(rows),
            "services": {
                "apify": apify_service.is_available,
                "memory": memory_manager.initialized,
                "google_sheets": google_sheets_service.is_available
            }
        }
    
    except Exception as e:
        logger.error(f"Webhook processing failed: {e}", exc_info=True)
        return {"status": "error", "message": str(e)[:100]}

async def simple_ai_analysis(product_data: dict, keyword: str) -> dict:
    """Simple AI analysis fallback."""
    try:
        # ADDED: Log start of analysis
        logger.error(f"🔍 Starting AI analysis for ASIN: {product_data.get('asin', 'unknown')}")
        
        # FIXED: Convert all values to proper types before comparison
        # Convert rating to float
        rating_raw = product_data.get("product_rating", 0)
        if isinstance(rating_raw, str):
            # Try to extract number from string (e.g., "4.5 out of 5" -> 4.5)
            import re
            numbers = re.findall(r'\d+\.?\d*', rating_raw)
            rating = float(numbers[0]) if numbers else 0.0
        elif isinstance(rating_raw, (int, float)):
            rating = float(rating_raw)
        else:
            rating = 0.0
        
        # Convert reviews to integer
        reviews_raw = product_data.get("count_review", 0)
        if isinstance(reviews_raw, str):
            # Remove commas and non-numeric characters
            import re
            numbers = re.findall(r'\d+', reviews_raw.replace(',', ''))
            reviews = int(numbers[0]) if numbers else 0
        elif isinstance(reviews_raw, (int, float)):
            reviews = int(reviews_raw)
        else:
            reviews = 0
        
        # Convert price to float
        price_raw = product_data.get("price", 0)
        price = 0.0
        if price_raw:
            if isinstance(price_raw, str):
                # Remove currency symbols, commas, and convert to float
                import re
                # Extract numbers with decimal points
                numbers = re.findall(r'\d+\.?\d*', price_raw.replace(',', ''))
                if numbers:
                    price = float(numbers[0])
            elif isinstance(price_raw, (int, float)):
                price = float(price_raw)
        
        # FIXED: Now safe to compare numbers with numbers
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
        
        # ADDED: Log completion of analysis
        logger.error(f"✅ AI analysis completed. Score: {opportunity_score}/100 for ASIN: {product_data.get('asin', 'unknown')}")
        
        return {
            "recommendation": recommendation,
            "opportunity_score": opportunity_score,
            "key_advantages": f"Rating: {rating}, Reviews: {reviews}, Price: {price}",
            "analysis_type": analysis_type,  # FIXED: Changed from "simple_analysis" to dynamic value
            "normalized_values": {
                "rating": rating,
                "reviews": reviews,
                "price": price
            }
        }
    
    except Exception as e:
        # ADDED: Log failure
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

# ADD THIS NEW ENDPOINT HERE:
@app.get("/debug/google-sheets")
async def debug_google_sheets():
    """Debug Google Sheets connection."""
    try:
        if not google_sheets_service.is_available:
            return {"error": "Google Sheets service not available"}
        
        # Test with minimal data
        test_data = [{
            "timestamp": datetime.utcnow().isoformat(),
            "asin": "TEST123",
            "keyword": "test",
            "ai_recommendation": "Test recommendation",
            "opportunity_score": 50,
            "key_advantages": "Test advantages",
            "analysis_type": "test_analysis"  # FIXED: Added analysis_type
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
# Local run
# ======================
if __name__ == "__main__":
    import os
    import uvicorn
    
    # Set app start time for debugging
    app_start_time = time.time()
    
    port = int(os.environ.get("PORT", 8080))  # Default to 8080 for Railway
    uvicorn.run(app, host="0.0.0.0", port=port)
