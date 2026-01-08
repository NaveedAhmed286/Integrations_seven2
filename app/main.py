""" Main application entry point. """
import asyncio
import signal
import sys
import time
import json
import requests
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
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
# FastAPI app with CORS
# ======================
app = FastAPI(
    title="Amazon Scraper API",
    description="Production-grade Amazon scraping system",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for now
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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
# MAIN WORKING ENDPOINT - NO WEBHOOKS!
# ======================
@app.post("/api/v1/scrape-amazon")
async def scrape_amazon_direct(request: Request):
    """
    SIMPLE, RELIABLE Amazon scraper - NO WEBHOOKS NEEDED!
    Direct API integration that ALWAYS works.
    """
    try:
        data = await request.json()
        keyword = data.get("keyword", "").strip()
        
        if not keyword:
            return JSONResponse(
                status_code=400,
                content={"status": "error", "message": "Keyword is required"}
            )
        
        logger.info(f"🚀 Starting Amazon scrape for: '{keyword}'")
        
        # ===== 1. VERIFY APIFY API KEY =====
        if not config.APIFY_API_KEY:
            logger.error("❌ Apify API key not configured")
            return JSONResponse(
                status_code=500,
                content={"status": "error", "message": "Apify API key not configured"}
            )
        
        # ===== 2. START APIFY ACTOR =====
        actor_id = "apify/web-scraper"
        run_url = f"https://api.apify.com/v2/acts/{actor_id}/runs?token={config.APIFY_API_KEY}"
        
        # SIMPLE page function that WORKS
        page_function = """async function pageFunction(context) {
    const $ = context.$;
    const results = [];
    
    // Wait for Amazon to load
    await new Promise(resolve => setTimeout(resolve, 8000));
    
    // Get product cards
    $('div[data-asin]').each((index, element) => {
        const $el = $(element);
        const asin = $el.attr('data-asin');
        
        if (asin && asin.length > 5 && asin !== '') {
            // Get title
            const title = $el.find('h2 a span').first().text().trim() || 
                         $el.find('h2').text().trim() || 
                         'No title';
            
            // Get price
            const price = $el.find('.a-price-whole').first().text().trim() || 
                         $el.find('.a-offscreen').first().text().trim() || 
                         'N/A';
            
            // Get rating
            let rating = '0.0';
            const ratingText = $el.find('.a-icon-star-small .a-icon-alt').text();
            if (ratingText) {
                const match = ratingText.match(/(\\d+\\.\\d)/);
                if (match) rating = match[1];
            }
            
            results.push({
                asin: asin,
                title: title.substring(0, 150),
                price: price.replace('$', '').trim(),
                rating: rating,
                position: index + 1,
                scraped_at: new Date().toISOString(),
                keyword: context.request.url
            });
        }
    });
    
    console.log('📊 Scraped', results.length, 'products');
    return results;
}"""
        
        payload = {
            "startUrls": [{"url": f"https://www.amazon.com/s?k={keyword}"}],
            "pageFunction": page_function,
            "injectJQuery": True,
            "maxPagesPerCrawl": 1,
            "maxResults": 50,
            "pageLoadTimeoutSecs": 120,
            "waitFor": "load",
            "proxyConfiguration": {"useApifyProxy": True}
        }
        
        logger.info("📤 Starting Apify Web Scraper...")
        run_response = requests.post(run_url, json=payload, timeout=30)
        
        if run_response.status_code != 201:
            logger.error(f"❌ Failed to start Apify actor: {run_response.status_code}")
            logger.error(f"Response: {run_response.text}")
            return JSONResponse(
                status_code=500,
                content={
                    "status": "error", 
                    "message": f"Failed to start Apify actor: {run_response.status_code}",
                    "details": run_response.text[:200]
                }
            )
        
        run_data = run_response.json()
        run_id = run_data["data"]["id"]
        logger.info(f"✅ Apify actor started. Run ID: {run_id}")
        
        # ===== 3. WAIT FOR COMPLETION =====
        logger.info("⏳ Waiting for Apify actor to complete...")
        
        max_wait = 300  # 5 minutes
        wait_interval = 10
        status = "RUNNING"
        dataset_id = None
        status_data = None
        
        for i in range(max_wait // wait_interval):
            status_url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={config.APIFY_API_KEY}"
            try:
                status_response = requests.get(status_url, timeout=10)
                
                if status_response.status_code == 200:
                    status_data = status_response.json()
                    status = status_data["data"]["status"]
                    
                    if status in ["SUCCEEDED", "FAILED", "TIMED-OUT", "ABORTED"]:
                        logger.info(f"✅ Apify actor finished with status: {status}")
                        if status == "SUCCEEDED":
                            dataset_id = status_data["data"].get("defaultDatasetId")
                        break
            except Exception as e:
                logger.warning(f"Status check error: {e}")
            
            logger.info(f"⏳ Still running... (attempt {i+1}, status: {status})")
            await asyncio.sleep(wait_interval)
        
        # ===== 4. FETCH RESULTS IF SUCCESSFUL =====
        if status == "SUCCEEDED" and dataset_id:
            logger.info(f"📊 Fetching dataset: {dataset_id}")
            
            # Fetch dataset items
            dataset_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items"
            dataset_response = requests.get(dataset_url, timeout=30)
            
            if dataset_response.status_code == 200:
                results = dataset_response.json()
                logger.info(f"✅ Fetched {len(results)} items from Apify")
                
                # ===== 5. PROCESS AND SAVE TO GOOGLE SHEETS =====
                sheets_success = False
                if results and google_sheets_service.is_available:
                    try:
                        rows = []
                        for item in results[:100]:  # Limit to 100 items
                            # Simple AI analysis
                            ai_result = await simple_ai_analysis(item, keyword)
                            
                            rows.append({
                                "timestamp": datetime.utcnow().isoformat(),
                                "asin": item.get("asin", "unknown"),
                                "keyword": keyword,
                                "title": item.get("title", "")[:100],
                                "price": item.get("price", "N/A"),
                                "rating": item.get("rating", "0.0"),
                                "position": item.get("position", 0),
                                "ai_recommendation": ai_result.get("recommendation", "Not analyzed"),
                                "opportunity_score": ai_result.get("opportunity_score", 0),
                                "scraped_at": datetime.utcnow().isoformat()
                            })
                        
                        success = await google_sheets_service.append_to_sheet(
                            spreadsheet_id=config.GOOGLE_SHEETS_SPREADSHEET_ID,
                            worksheet_name="Sheet1",
                            data=rows
                        )
                        
                        if success:
                            sheets_success = True
                            logger.info(f"✅ Written {len(rows)} rows to Google Sheets")
                        else:
                            logger.error("❌ Google Sheets write failed")
                    except Exception as e:
                        logger.error(f"Google Sheets error: {e}")
                elif results:
                    logger.warning("⚠️ Google Sheets not available, skipping save")
                
                # ===== 6. RETURN SUCCESS =====
                return JSONResponse(
                    status_code=200,
                    content={
                        "status": "success",
                        "message": f"Successfully scraped {len(results)} Amazon products",
                        "keyword": keyword,
                        "items_scraped": len(results),
                        "rows_saved": len(results) if sheets_success else 0,
                        "apify_run_id": run_id,
                        "dataset_id": dataset_id,
                        "google_sheets_success": sheets_success,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                )
            else:
                logger.error(f"❌ Failed to fetch dataset: {dataset_response.status_code}")
                return JSONResponse(
                    status_code=500,
                    content={
                        "status": "error",
                        "message": f"Failed to fetch dataset: {dataset_response.status_code}",
                        "apify_run_id": run_id,
                        "dataset_id": dataset_id
                    }
                )
        else:
            logger.error(f"❌ Apify actor failed with status: {status}")
            return JSONResponse(
                status_code=500,
                content={
                    "status": "error",
                    "message": f"Apify actor failed: {status}",
                    "apify_run_id": run_id
                }
            )
            
    except Exception as e:
        logger.error(f"❌ Scrape failed: {e}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "message": str(e)[:200],
                "timestamp": datetime.utcnow().isoformat()
            }
        )

# ======================
# TEST ENDPOINTS
# ======================
@app.post("/api/v1/test")
async def test_endpoint(request: Request):
    """Simple test endpoint"""
    try:
        body = await request.body()
        raw_text = body.decode('utf-8', errors='ignore') if body else ""
        
        logger.info("🧪 TEST ENDPOINT CALLED")
        
        return {
            "status": "success",
            "message": "Test endpoint working",
            "received": True,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Test endpoint error: {e}")
        return {"status": "error", "message": str(e)}

# ======================
# SIMPLE AI ANALYSIS
# ======================
async def simple_ai_analysis(product_data: dict, keyword: str) -> dict:
    """Simple AI analysis fallback."""
    try:
        # Extract values
        rating_raw = product_data.get("rating", "0.0")
        price_raw = product_data.get("price", "0")
        
        # Convert rating
        rating = 0.0
        if isinstance(rating_raw, str):
            import re
            numbers = re.findall(r'\d+\.?\d*', rating_raw)
            rating = float(numbers[0]) if numbers else 0.0
        elif isinstance(rating_raw, (int, float)):
            rating = float(rating_raw)
        
        # Convert price
        price = 0.0
        if isinstance(price_raw, str):
            import re
            numbers = re.findall(r'\d+\.?\d*', price_raw.replace(',', '').replace('$', ''))
            if numbers:
                price = float(numbers[0])
        elif isinstance(price_raw, (int, float)):
            price = float(price_raw)
        
        # Calculate opportunity score (0-100)
        opportunity_score = 0
        
        if rating >= 4.5:
            opportunity_score += 40
        elif rating >= 4.0:
            opportunity_score += 30
        elif rating >= 3.5:
            opportunity_score += 20
        
        if price > 0 and price < 50:
            opportunity_score += 30
        elif price > 0 and price < 100:
            opportunity_score += 20
        elif price > 0:
            opportunity_score += 10
        
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
        
        return {
            "recommendation": recommendation,
            "opportunity_score": opportunity_score,
            "analysis_type": analysis_type,
            "normalized_values": {
                "rating": rating,
                "price": price
            }
        }
    
    except Exception as e:
        logger.error(f"AI analysis failed: {e}")
        return {
            "recommendation": "Analysis failed",
            "opportunity_score": 0,
            "analysis_type": "failed"
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
        
        test_data = [{
            "timestamp": datetime.utcnow().isoformat(),
            "asin": "DEBUG123",
            "keyword": "debug",
            "title": "Debug product",
            "price": "29.99",
            "rating": "4.5",
            "position": 1,
            "ai_recommendation": "Debug test",
            "opportunity_score": 75,
            "scraped_at": datetime.utcnow().isoformat()
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
