"""
Wrapper for Apify service.
Includes timeout, retry, response validation, error translation.
All network logic is isolated here.
"""
import aiohttp
import asyncio
from typing import List, Dict, Any, Optional
import json

from app.errors import ExternalServiceError, NetworkError
from app.utils.retry import async_retry
from app.config import config
from app.logger import logger


class ApifyService:
    """
    Wrapper for Apify API calls.
    Business logic never calls Apify directly.
    """
    
    def __init__(self):
        self.api_key = config.APIFY_API_KEY
        self.base_url = "https://api.apify.com/v2"
        self.session: Optional[aiohttp.ClientSession] = None
        self.is_available = bool(self.api_key)
    
    async def initialize(self):
        """Initialize HTTP session (called after startup)."""
        if not self.is_available:
            logger.warning("Apify service not configured")
            return
        
        self.session = aiohttp.ClientSession(
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            },
            timeout=aiohttp.ClientTimeout(total=config.REQUEST_TIMEOUT)
        )
        logger.info("Apify service initialized")
    
    async def close(self):
        """Close HTTP session."""
        if self.session:
            await self.session.close()
    
    @async_retry(exceptions=(aiohttp.ClientError, asyncio.TimeoutError))
    async def scrape_amazon_search(self, keyword: str, domain: str = "com", 
                                   max_results: int = 10) -> List[Dict[str, Any]]:
        """
        Scrape Amazon search results via Apify.
        
        Args:
            keyword: Search keyword
            domain: Amazon domain (com, co.uk, etc.)
            max_results: Maximum results to return
            
        Returns:
            List of raw product data
            
        Raises:
            ExternalServiceError: If Apify API fails
            NetworkError: If network connection fails
        """
        if not self.is_available:
            raise ExternalServiceError("Apify service not configured")
        
        try:
            # Prepare Apify actor input
            actor_input = {
                "keyword": keyword,
                "domainCode": domain,
                "maxPages": 1,
                "maxItems": max_results,
                "proxy": {
                    "useApifyProxy": True
                }
            }
            
            # Start actor run
            start_response = await self.session.post(
                f"{self.base_url}/acts/apify~amazon-search-scraper/run-sync-get-dataset-items",
                json={"input": actor_input}
            )
            
            if start_response.status != 200:
                error_text = await start_response.text()
                raise ExternalServiceError(
                    f"Apify API error {start_response.status}: {error_text}"
                )
            
            # Parse response
            results = await start_response.json()
            
            # Validate response structure
            if not isinstance(results, list):
                raise ExternalServiceError("Invalid response format from Apify")
            
            logger.info(f"Scraped {len(results)} products for keyword: {keyword}")
            return results
            
        except aiohttp.ClientError as e:
            raise NetworkError(f"Network error calling Apify: {str(e)}") from e
        except asyncio.TimeoutError as e:
            raise NetworkError(f"Timeout calling Apify: {str(e)}") from e
        except json.JSONDecodeError as e:
            raise ExternalServiceError(f"Invalid JSON response from Apify: {str(e)}") from e
    
    @async_retry(exceptions=(aiohttp.ClientError, asyncio.TimeoutError))
    async def get_actor_status(self, actor_id: str) -> Dict[str, Any]:
        """
        Get Apify actor status.
        
        Args:
            actor_id: Apify actor ID
            
        Returns:
            Actor status information
        """
        if not self.is_available:
            return {"status": "not_configured"}
        
        try:
            response = await self.session.get(f"{self.base_url}/acts/{actor_id}")
            
            if response.status == 200:
                return await response.json()
            else:
                return {"status": "error", "code": response.status}
                
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return {"status": "network_error"}


# Global service instance
apify_service = ApifyService()