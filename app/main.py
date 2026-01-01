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

# Initialize services
apify_service = ApifyService()
memory_manager = MemoryManager()
normalizer = AmazonNormalizer()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle."""
    # Startup
    logger.info("Starting Amazon Scraper System")
    
    # Initialize services
    await apify_service.initialize()
    await memory_manager.initialize()
    await google_sheets_service.initialize()
    
    yield
    
    # Shutdown
    logger.info("Shutting down Amazon Scraper System")
    await apify_service.close()
    await memory_manager.close()

# Create FastAPI app
app = FastAPI(
    title="Amazon Scraper API",
    description="Production-grade Amazon scraping system with memory and retry mechanisms",
    version="1.0.0",
    lifespan=lifespan
)

@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "Amazon Scraper System",
        "version": "1.0.0",
        "status": "operational",
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/health")
async def health_check():
    """Health check endpoint with logging."""
    redis_available = False
    if memory_manager.short_term.redis and memory_manager.short_term.is_available:
        try:
            await memory_manager.short_term.redis.ping()
            redis_available = True
            logger.info("Redis ping successful")
        except Exception as e:
            logger.warning(f"Redis ping failed: {e}")
            redis_available = False
    
    services = {
        "apify": apify_service.is_available,
        "memory": memory_manager.initialized,
        "redis": redis_available,
        "postgres": memory_manager.long_term.is_available,
        "google_sheets": google_sheets_service.is_available
    }
    
    status = "healthy" if all(services.values()) else "degraded"
    
    logger.info(f"Health check: {services}")
    
    return {
        "status": status,
        "services": services,
        "timestamp": datetime.utcnow().isoformat()
    }

@app.post("/api/v1/search")
async def search_amazon(request: Request):
    """Search Amazon for products using junglee/free-amazon-product-scraper."""
    try:
        data = await request.json()
        keyword = data.get("keyword", "").strip()
        domain = data.get("domain", "com")
        max_results = data.get("max_results", 10)
        
        if not keyword:
            raise HTTPException(status_code=400, detail="Keyword is required")
        
        if len(keyword) < 2:
            raise HTTPException(status_code=400, detail="Keyword must be at least 2 characters")
        
        if max_results > 50:
            max_results = 50
            logger.info("Limiting max_results to 50")

        logger.info(f"Searching Amazon for: '{keyword}'")

        raw_results = await apify_service.scrape_amazon_search(
            keyword, domain, max_results
        )

        normalized_results = []
        normalization_errors = 0
        
        for raw_product in raw_results:
            try:
                product = normalizer.normalize_product(raw_product)
                normalized_results.append(product.dict())
            except NormalizationError as e:
                logger.warning(f"Failed to normalize product: {e}")
                normalization_errors += 1

        memory_manager.store_episodic(
            client_id="api",
            analysis_type="search",
            input_data={"keyword": keyword, "domain": domain, "max_results": max_results},
            output_data={"results_count": len(normalized_results)},
            insights=[
                f"Found {len(raw_results)} raw products",
                f"Normalized {len(normalized_results)} products"
            ]
        )

        # ===== GOOGLE SHEET WRITE =====
        if google_sheets_service.is_available and normalized_results:
            await google_sheets_service.append_to_sheet(
                spreadsheet_id=config.GOOGLE_SHEET_ID,
                worksheet_name="Sheet1",
                data=normalized_results
            )

        return {
            "success": True,
            "keyword": keyword,
            "results": normalized_results,
            "count": len(normalized_results),
            "normalization_errors": normalization_errors,
            "timestamp": datetime.utcnow().isoformat()
        }

    except ExternalServiceError as e:
        logger.error(f"External service error: {e}")
        raise HTTPException(status_code=503, detail="Service unavailable")

    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/v1/memory/{client_id}")
async def get_memory_context(client_id: str):
    context = await memory_manager.get_ai_context(client_id)
    return {"client_id": client_id, "context": context}

@app.post("/api/v1/test-mock-sheet")
async def test_mock_sheet(request: Request):
    """
    Test endpoint:
    - No Apify
    - Uses mock Amazon data
    - Calls DeepSeek
    - Writes to Google Sheet
    """
    try:
        payload = await request.json()
        keyword = payload.get("keyword")
        domain_code = payload.get("domain_code", "com")
        results = payload.get("results", [])

        if not keyword or not results:
            raise HTTPException(status_code=400, detail="keyword and results are required")

        sheet_rows = []

        for item in results:
            # --- AI ANALYSIS (DeepSeek) ---
            ai_result = await memory_manager.ai_analyze_product(
                product_data=item,
                keyword=keyword
            )

            # --- Map EXACT Google Sheet columns ---
            sheet_rows.append({
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
                # --- AI OUTPUT ---
                "ai_recommendation": ai_result.get("recommendation"),
                "opportunity_score": ai_result.get("opportunity_score"),
                "key_advantages": ai_result.get("key_advantages"),
            })

        # --- WRITE TO GOOGLE SHEET ---
        await memory_manager.google_sheet.append_rows(sheet_rows)

        return {
            "success": True,
            "rows_written": len(sheet_rows),
            "message": "Mock data + AI analysis written to Google Sheet"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Mock sheet test failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Mock sheet test failed")


if __name__ == "__main__":
    import os
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
