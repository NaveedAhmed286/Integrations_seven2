"""
Startup readiness and health checks.
Application starts without network, recovers after.
"""
import asyncio
from typing import Dict, Any

from app.config import config
from app.logger import logger


class ReadinessManager:
    """
    Manages application readiness state.
    Startup succeeds even with no internet.
    """
    
    def __init__(self):
        self.is_ready = False
        self.services: Dict[str, bool] = {
            "config": True,  # Config validation happens at import
            "memory": False,
            "apify": False,
            "google_sheets": False,
            "ai": False,  # Added AI service
            "queues": True  # Queues work offline
        }
        self.startup_time = None
    
    async def initialize_services(self):
        """
        Initialize all services after startup.
        Non-blocking, failures don't prevent startup.
        """
        logger.info("Starting service initialization...")
        
        # Initialize memory system
        try:
            from app.memory.memory_manager import memory_manager
            await memory_manager.initialize()
            self.services["memory"] = memory_manager.short_term.is_available or \
                                     memory_manager.long_term.is_available
        except Exception as e:
            logger.warning(f"Memory initialization failed: {e}")
        
        # Initialize Apify service
        try:
            from app.services.apify_service import apify_service
            await apify_service.initialize()
            self.services["apify"] = apify_service.is_available
        except Exception as e:
            logger.warning(f"Apify service initialization failed: {e}")
        
        # Initialize Google Sheets service
        try:
            from app.services.google_service import google_sheets_service
            await google_sheets_service.initialize()
            self.services["google_sheets"] = google_sheets_service.is_available
        except Exception as e:
            logger.warning(f"Google Sheets initialization failed: {e}")
        
        # Initialize AI service (DEEPSEEK)
        try:
            from app.services.ai_service import ai_service
            await ai_service.initialize()
            self.services["ai"] = ai_service.is_available
        except Exception as e:
            logger.warning(f"AI service initialization failed: {e}")
        
        # Mark as ready
        self.is_ready = True
        self.startup_time = asyncio.get_event_loop().time()
        
        logger.info(f"Services initialized. Ready: {self.is_ready}")
        logger.info(f"Service status: {self.services}")
    
    def get_status(self) -> Dict[str, Any]:
        """Get readiness status."""
        return {
            "ready": self.is_ready,
            "services": self.services,
            "uptime": asyncio.get_event_loop().time() - self.startup_time if self.startup_time else 0
        }
    
    def is_service_available(self, service_name: str) -> bool:
        """Check if a specific service is available."""
        return self.services.get(service_name, False)


# Global readiness manager
readiness_manager = ReadinessManager()