"""
Single entrypoint for the application.
FastAPI for API endpoints, background worker for processing.
"""
import asyncio
from typing import Dict, Any
import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse

from app.config import config
from app.logger import logger
from app.readiness import readiness_manager
from app.sentry import initialize_sentry
from app.queue.workflow_queue import workflow_queue
from app.queue.retry_queue import retry_queue
from app.agents.amazon_agent import AmazonAgent


class Application:
    """Main application controller."""
    
    def __init__(self):
        self.app = FastAPI(
            title="Amazon Scraper System",
            description="Production-grade scraper with memory and retry systems",
            version="1.0.0"
        )
        self.agent = AmazonAgent()
        self.setup_routes()
    
    def setup_routes(self):
        """Setup API routes."""
        
        @self.app.get("/")
        async def root():
            return {"status": "running", "system": "amazon-scraper"}
        
        @self.app.get("/health")
        async def health():
            """Health check endpoint."""
            status = readiness_manager.get_status()
            return {
                "status": "healthy" if status["ready"] else "initializing",
                "services": status["services"],
                "uptime": status["uptime"]
            }
        
        @self.app.post("/scrape")
        async def scrape(keyword: str, max_results: int = 10, background_tasks: BackgroundTasks = None):
            """
            Trigger Amazon search scrape.
            
            Args:
                keyword: Search keyword
                max_results: Maximum products to return
                background_tasks: FastAPI background tasks
            """
            try:
                # Enqueue scrape task
                task_id = await self.agent.scrape_amazon_search(
                    keyword=keyword,
                    max_results=max_results
                )
                
                # Process in background if requested
                if background_tasks:
                    background_tasks.add_task(
                        self.agent.process_pending_tasks
                    )
                
                return {
                    "task_id": task_id,
                    "message": f"Scrape enqueued for '{keyword}'",
                    "status": "pending"
                }
                
            except Exception as e:
                logger.error(f"Failed to enqueue scrape: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/task/{task_id}")
        async def get_task_status(task_id: str):
            """Get task status."""
            task = workflow_queue.get_task_status(task_id)
            if not task:
                raise HTTPException(status_code=404, detail="Task not found")
            
            return task
        
        @self.app.get("/queue/stats")
        async def get_queue_stats():
            """Get queue statistics."""
            return {
                "workflow": {
                    "pending": len(workflow_queue.task_order),
                    "tasks": list(workflow_queue.callbacks.keys())
                },
                "retry": retry_queue.get_stats()
            }
        
        @self.app.post("/test/json")
        async def test_with_json(payload: Dict[str, Any]):
            """
            Test endpoint for manual JSON input.
            Accepts the scraper JSON you provided.
            """
            try:
                # Normalize and process the test data
                results = await self.agent.process_scraper_json(payload)
                
                return {
                    "processed": len(results),
                    "products": [p.to_dict() for p in results],
                    "message": "Test data processed successfully"
                }
                
            except Exception as e:
                logger.error(f"Test processing failed: {e}")
                raise HTTPException(status_code=400, detail=str(e))
        
        @self.app.post("/analyze/ai")
        async def analyze_with_ai(payload: Dict[str, Any], client_id: str = "default"):
            """
            Direct AI analysis endpoint.
            
            Args:
                payload: Product data for analysis
                client_id: Client identifier
            
            Example payload:
            {
                "asin": "B0F3PT1VBL",
                "keyword": "wireless headphones",
                "product_rating": 4.5,
                "count_review": 100,
                "price": 29.99,
                "sponsored": true,
                "prime": true,
                "search_result_position": 2
            }
            """
            try:
                analysis = await self.agent.analyze_with_ai(
                    product_data=payload,
                    client_id=client_id
                )
                
                return {
                    "analysis": analysis,
                    "has_ai": config.has_ai,
                    "is_fallback": analysis.get("is_fallback", False),
                    "client_id": client_id
                }
                
            except Exception as e:
                logger.error(f"AI analysis failed: {e}")
                raise HTTPException(status_code=500, detail=str(e))
        
        @self.app.get("/services/status")
        async def get_services_status():
            """Get detailed status of all external services."""
            try:
                from app.services.apify_service import apify_service
                from app.services.google_service import google_sheets_service
                from app.services.ai_service import ai_service
                
                status = {
                    "readiness": readiness_manager.get_status(),
                    "services": {
                        "apify": await apify_service.get_actor_status("apify~amazon-search-scraper"),
                        "google_sheets": {
                            "configured": config.has_google_sheets,
                            "available": google_sheets_service.is_available
                        },
                        "ai": await ai_service.get_service_status()
                    }
                }
                
                return status
                
            except Exception as e:
                logger.error(f"Failed to get service status: {e}")
                return {"error": str(e)}
    
    async def startup(self):
        """Application startup sequence."""
        logger.info("Starting Amazon Scraper System...")
        
        # Initialize Sentry (observability only)
        initialize_sentry()
        
        # Initialize queues
        self.agent.register_queues()
        
        # Initialize services (non-blocking, continues without network)
        asyncio.create_task(readiness_manager.initialize_services())
        
        logger.info("Application startup completed")
    
    async def shutdown(self):
        """Application shutdown sequence."""
        logger.info("Shutting down...")
        
        # Close services
        from app.services.apify_service import apify_service
        from app.services.ai_service import ai_service
        
        await apify_service.close()
        await ai_service.close()
        
        logger.info("Shutdown complete")


# Create application instance
application = Application()
app = application.app


@app.on_event("startup")
async def on_startup():
    """FastAPI startup event."""
    await application.startup()


@app.on_event("shutdown")
async def on_shutdown():
    """FastAPI shutdown event."""
    await application.shutdown()


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=False  # Disable in production
    )