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
    # Check Redis connection asynchronously
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
        "postgres": memory_manager.long_term.is_available
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
        
        # Input validation
        if not keyword:
            raise HTTPException(status_code=400, detail="Keyword is required")
        
        if len(keyword) < 2:
            raise HTTPException(status_code=400, detail="Keyword must be at least 2 characters")
        
        # Limit max results for performance
        if max_results > 50:
            max_results = 50
            logger.info(f"Limiting max_results to 50 for performance")
        
        logger.info(f"Searching Amazon for: '{keyword}' (domain: {domain}, max: {max_results})")
        
        # Search Amazon using junglee actor
        raw_results = await apify_service.scrape_amazon_search(
            keyword, domain, max_results
        )
        
        # Normalize results (handles junglee actor format)
        normalized_results = []
        normalization_errors = 0
        
        for raw_product in raw_results:
            try:
                product = normalizer.normalize_product(raw_product)
                normalized_results.append(product.dict())
            except NormalizationError as e:
                logger.warning(f"Failed to normalize product: {e}")
                normalization_errors += 1
                continue
        
        # Log summary
        if normalization_errors > 0:
            logger.warning(f"Skipped {normalization_errors} products due to normalization errors")
        
        # Store in episodic memory
        memory_manager.store_episodic(
            client_id="api",
            analysis_type="search",
            input_data={"keyword": keyword, "domain": domain, "max_results": max_results},
            output_data={"results_count": len(normalized_results), "raw_count": len(raw_results)},
            insights=[
                f"Found {len(raw_results)} raw products for '{keyword}'",
                f"Successfully normalized {len(normalized_results)} products",
                f"Domain: amazon.{domain}"
            ]
        )
        
        return {
            "success": True,
            "keyword": keyword,
            "domain": domain,
            "results": normalized_results,
            "count": len(normalized_results),
            "raw_count": len(raw_results),
            "normalization_errors": normalization_errors,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except ExternalServiceError as e:
        logger.error(f"Apify service error: {e}")
        raise HTTPException(
            status_code=503, 
            detail=f"Amazon scraping service temporarily unavailable. Please try again in a moment."
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in search: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, 
            detail="Internal server error. Please check logs for details."
        )

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
