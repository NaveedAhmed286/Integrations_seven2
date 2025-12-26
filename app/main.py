"""
Main application entry point.
"""
import asyncio
import signal
import sys
from contextlib import asynccontextmanager

import redis
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse

from app.config import config
from app.logger import logger
from app.errors import ExternalServiceError, NormalizationError
from app.services.apify_service import ApifyService
from app.memory_manager import MemoryManager
from app.normalizers.amazon import AmazonNormalizer
from app.models.product import AmazonProduct

# Initialize services
apify_service = ApifyService()
memory_manager = MemoryManager()
normalizer = AmazonNormalizer()

# Initialize Redis connection for queues
redis_client = redis.Redis.from_url(config.REDIS_URL, decode_responses=True)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle."""
    # Startup
    logger.info("Starting Amazon Scraper System")
    
    # Initialize services
    await apify_service.initialize()
    await memory_manager.initialize()
    
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
        "status": "operational"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    services = {
        "apify": apify_service.is_available,
        "memory": memory_manager.initialized,
        "redis": redis_client.ping() if redis_client else False
    }
    
    return {
        "status": "healthy" if all(services.values()) else "degraded",
        "services": services
    }

@app.post("/api/v1/search")
async def search_amazon(request: Request):
    """Search Amazon for products."""
    try:
        data = await request.json()
        keyword = data.get("keyword", "").strip()
        domain = data.get("domain", "com")
        max_results = data.get("max_results", 10)
        
        if not keyword:
            raise HTTPException(status_code=400, detail="Keyword is required")
        
        # Search Amazon
        raw_results = await apify_service.scrape_amazon_search(
            keyword, domain, max_results
        )
        
        # Normalize results
        normalized_results = []
        for raw_product in raw_results:
            try:
                product = normalizer.normalize_product(raw_product)
                normalized_results.append(product.dict())
            except NormalizationError as e:
                logger.warning(f"Failed to normalize product: {e}")
                continue
        
        # Store in memory
        await memory_manager.store_episodic_memory(
            client_id="api",
            action="search",
            details={"keyword": keyword, "domain": domain, "results_count": len(normalized_results)}
        )
        
        return {
            "success": True,
            "keyword": keyword,
            "domain": domain,
            "results": normalized_results,
            "count": len(normalized_results)
        }
        
    except ExternalServiceError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error in search: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/api/v1/memory/{client_id}")
async def get_memory_context(client_id: str):
    """Get memory context for a client."""
    context = await memory_manager.get_ai_context(client_id)
    
    return {
        "client_id": client_id,
        "context": context
    }

if __name__ == "__main__":
    import os
    import uvicorn
    # Read the PORT from the environment, default to 8000
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
