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
from app.services.google_service import google_sheets_service  # <<< ADDED >>>
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
    await google_sheets_service.initialize()  # <<< ADDED >>>
    
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
    """Health check endpoint."""
    redis_available = False
    if memory_manager.short_term.redis and memory_manager.short_term.is_available:
        try:
            await memory_manager.short_term.redis.ping()
            redis_available = True
        except Exception as e:
            logger.warning(f"Redis ping failed: {e}")
            redis_available = False
    
    services = {
        "apify": apify_service.is_available,
        "memory": memory_manager.initialized,
        "redis": redis_available,
        "postgres": memory_manager.long_term.is_available,
        "google_sheets": google_sheets_service.is_available  # <<< ADDED >>>
    }
    
    status = "healthy" if all(services.values()) else "degraded"
    
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

        # ===== GOOGLE SHEET WRITE (ONLY REAL ADDITION) =====
        if google_sheets_service.is_available and normalized_results:
            await google_sheets_service.append_to_sheet(
                spreadsheet_id=config.GOOGLE_SHEET_ID,
                worksheet_name="Sheet1",
                data=normalized_results
            )
        # ==================================================

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

if __name__ == "__main__":
    import os
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
