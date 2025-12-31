"""
Wrapper for Apify service.
Includes timeout, retry, response validation, error translation.
All network logic is isolated here.
"""
import aiohttp
import asyncio
import os
from typing import List, Dict, Any, Optional
import json
import time

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
        self.actor_name = "apify/web-scraper"  # Fixed actor name
        self.session: Optional[aiohttp.ClientSession] = None
        self.is_available = bool(self.api_key)
    
    async def initialize(self):
        """Initialize HTTP session (called after startup)."""
        if not self.is_available:
            logger.warning("Apify service not configured - set APIFY_API_KEY")
            logger.warning(f"Current APIFY_API_KEY: {'Set' if config.APIFY_API_KEY else 'Not set'}")
            return
        
        self.session = aiohttp.ClientSession(
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            },
            timeout=aiohttp.ClientTimeout(total=config.REQUEST_TIMEOUT)
        )
        
        # Test the connection
        try:
            status = await self.get_account_status()
            logger.info(f"Apify service initialized. Account: {status.get('username', 'Unknown')}")
        except Exception as e:
            logger.error(f"Apify initialization test failed: {e}")
    
    async def close(self):
        """Close HTTP session."""
        if self.session:
            await self.session.close()
    
    async def get_account_status(self) -> Dict[str, Any]:
        """Get Apify account information."""
        if not self.is_available:
            return {"error": "Apify not configured"}
        
        try:
            response = await self.session.get(f"{self.base_url}/users/me")
            if response.status == 200:
                return await response.json()
            else:
                return {"error": f"API returned {response.status}"}
        except Exception as e:
            logger.error(f"Failed to get Apify account status: {e}")
            return {"error": str(e)}
    @async_retry(exceptions=(aiohttp.ClientError, asyncio.TimeoutError))    
    async def scrape_amazon_search(self, keyword: str, domain: str = "com", 
                                   max_results: int = 10) -> List[Dict[str, Any]]:
        """
        Scrape Amazon using apify/web-scraper.
        
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
            raise ExternalServiceError("Apify service not configured. Set APIFY_API_KEY environment variable.")
        
        logger.info(f"Starting Amazon scrape for: '{keyword}' (domain: {domain}, max: {max_results})")
        
        try:
            # CORRECT input format for apify/web-scraper
          page_function = f"""
async function pageFunction(context) {{
    const $ = context.jQuery;
    const results = [];
    
    // NO waitForSelector! The page is already loaded
    
    // Amazon product containers
    $('[data-component-type="s-search-result"]').each((index, element) => {{
        const $el = $(element);
        
        // Extract basic information
        const title = $el.find('h2 a span').first().text().trim();
        const priceText = $el.find('.a-price .a-offscreen').text();
        const url = 'https://amazon.{domain}' + ($el.find('h2 a').attr('href') || '');
        const ratingText = $el.find('.a-icon-alt').text();
        const reviewsText = $el.find('span.a-size-base.s-underline-text').text();
        const image = $el.find('img.s-image').attr('src') || '';
        
        // Parse price - handle "3 sizes" issue
        let price = 0;
        if (priceText && !priceText.includes('size')) {{
            const priceMatch = priceText.match(/\\$?([\\d.,]+)/);
            if (priceMatch) {{
                price = parseFloat(priceMatch[1].replace(',', ''));
            }}
        }}
        
        // Parse rating
        let rating = 0;
        if (ratingText) {{
            const ratingMatch = ratingText.match(/([\\d.]+)/);
            if (ratingMatch) {{
                rating = parseFloat(ratingMatch[1]);
            }}
        }}
        
        // Parse reviews
        let reviews = 0;
        if (reviewsText) {{
            const reviewsMatch = reviewsText.match(/([\\d,]+)/);
            if (reviewsMatch) {{
                reviews = parseInt(reviewsMatch[1].replace(',', ''));
            }}
        }}
        
        // Check if sponsored
        const sponsored = $el.text().includes('Sponsored') || 
                         $el.find('span:contains("Sponsored")').length > 0;
        
        // Check if Prime
        const prime = $el.find('.a-icon-prime, i.a-icon-prime').length > 0;
        
        if (title && price > 0) {{
            results.push({{
                title: title,
                price: price,
                url: url,
                rating: rating,
                reviews: reviews,
                image: image,
                sponsored: sponsored,
                prime: prime,
                position: index + 1,
                keyword: '{keyword}',
                scraped_at: new Date().toISOString()
            }});
        }}
    }});
    
    // If no results with new layout, try alternative selectors
    if (results.length === 0) {{
        $('.s-result-item').each((index, element) => {{
            const $el = $(element);
            const title = $el.find('.a-text-normal').first().text().trim();
            const priceText = $el.find('.a-price .a-offscreen').text();
            
            if (title && priceText) {{
                const price = parseFloat(priceText.replace(/[^\\d.]/g, ''));
                results.push({{
                    title: title,
                    price: price,
                    keyword: '{keyword}',
                    scraped_at: new Date().toISOString()
                }});
            }}
        }});
    }}
    
    return results.slice(0, {max_results});
}}
"""            
            actor_input = {
                "startUrls": [{
                    "url": f"https://www.amazon.{domain}/s?k={keyword.replace(' ', '+')}"
                }],
                "maxRequestsPerCrawl": 50,
                "maxConcurrency": 3,
                "pageFunction": page_function,
                "injectJQuery": True,
                "proxyConfiguration": {
                    "useApifyProxy": True
                },
                "maxItems": max_results,
                "waitUntil": ["networkidle2"],
                "dynamicContentWaitSecs": 10,
                "debugLog": False,
                "saveHtml": False
            }
            
            logger.debug(f"Using actor: {self.actor_name}")
            logger.debug(f"Input keys: {list(actor_input.keys())}")
            
            # Method 1: Try run-sync-get-dataset-items first (simpler)
            try:
                logger.info("Trying run-sync-get-dataset-items method...")
                response = await self.session.post(
                    f"{self.base_url}/acts/{self.actor_name}/run-sync-get-dataset-items",
                    json=actor_input,
                    timeout=aiohttp.ClientTimeout(total=300)  # 5 minutes
                )
                
                logger.debug(f"Response status: {response.status}")
                
                if response.status == 200:
                    results = await response.json()
                    logger.info(f"Method 1 succeeded: Got {len(results) if isinstance(results, list) else 'unknown'} items")
                    
                    if isinstance(results, list):
                        items = results[:max_results]
                    elif isinstance(results, dict):
                        items = results.get("items", [])[:max_results]
                    else:
                        items = []
                    
                    # Add keyword and domain to each result for tracking
                    for item in items:
                        item["keyword"] = keyword
                        item["domain"] = domain
                    
                    logger.info(f"Successfully scraped {len(items)} products")
                    return items
                else:
                    error_text = await response.text()
                    logger.warning(f"Method 1 failed ({response.status}): {error_text[:200]}")
                    
            except Exception as method1_error:
                logger.warning(f"Method 1 error: {method1_error}")
            
            # Method 2: Use regular run and wait for completion
            logger.info("Trying regular run method...")
            return await self._scrape_with_regular_run(actor_input, keyword, domain, max_results)
            
        except aiohttp.ClientError as e:
            logger.error(f"Network error calling Apify: {str(e)}")
            raise NetworkError(f"Network error calling Apify: {str(e)}") from e
        except asyncio.TimeoutError as e:
            logger.error(f"Timeout calling Apify: {str(e)}")
            raise NetworkError(f"Timeout calling Apify: {str(e)}") from e
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response from Apify: {str(e)}")
            raise ExternalServiceError(f"Invalid JSON response from Apify: {str(e)}") from e
        except Exception as e:
            logger.error(f"Unexpected error in scrape_amazon_search: {e}", exc_info=True)
            raise ExternalServiceError(f"Failed to scrape Amazon: {str(e)}") from e
    
    async def _scrape_with_regular_run(self, actor_input: Dict[str, Any], 
                                     keyword: str, domain: str, max_results: int) -> List[Dict[str, Any]]:
        """Alternative method using regular run."""
        try:
            # Start the run
            start_response = await self.session.post(
                f"{self.base_url}/acts/{self.actor_name}/runs",
                json={"input": actor_input},
                timeout=aiohttp.ClientTimeout(total=120)
            )
            
            if start_response.status != 201:
                error_text = await start_response.text()
                raise ExternalServiceError(f"Failed to start actor: {start_response.status} - {error_text}")
            
            run_data = await start_response.json()
            run_id = run_data["data"]["id"]
            
            logger.info(f"Actor run started: {run_id}")
            
            # Wait for completion
            items = await self._wait_for_run_completion(run_id, timeout=300)
            
            # Add keyword and domain
            for item in items[:max_results]:
                item["keyword"] = keyword
                item["domain"] = domain
            
            logger.info(f"Scraped {len(items[:max_results])} products using regular run method")
            return items[:max_results]
            
        except Exception as e:
            logger.error(f"Regular run method failed: {e}")
            raise
    
    async def _wait_for_run_completion(self, run_id: str, timeout: int = 300) -> List[Dict[str, Any]]:
        """Wait for actor run to complete and get results."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            try:
                # Check run status
                status_response = await self.session.get(
                    f"{self.base_url}/actor-runs/{run_id}",
                    timeout=aiohttp.ClientTimeout(total=30)
                )
                
                if status_response.status == 200:
                    status_data = await status_response.json()
                    run_status = status_data["data"]["status"]
                    
                    if run_status == "SUCCEEDED":
                        # Get dataset items
                        dataset_id = status_data["data"].get("defaultDatasetId")
                        if dataset_id:
                            dataset_response = await self.session.get(
                                f"{self.base_url}/datasets/{dataset_id}/items",
                                timeout=aiohttp.ClientTimeout(total=30)
                            )
                            
                            if dataset_response.status == 200:
                                return await dataset_response.json()
                    
                    elif run_status in ["FAILED", "TIMED-OUT", "ABORTED"]:
                        error_msg = status_data["data"].get("errorMessage", "Unknown error")
                        raise ExternalServiceError(f"Actor run {run_status}: {error_msg}")
                
                await asyncio.sleep(5)  # Wait before polling again
                
            except Exception as e:
                logger.warning(f"Error checking run status: {e}")
                await asyncio.sleep(5)
        
        raise ExternalServiceError(f"Actor run timed out after {timeout} seconds")
    
    @async_retry(exceptions=(aiohttp.ClientError, asyncio.TimeoutError))
    async def get_actor_status(self, actor_id: str = None) -> Dict[str, Any]:
        """Get Apify actor status."""
        if not self.is_available:
            return {"status": "not_configured"}
        
        actor_to_check = actor_id or self.actor_name
        
        try:
            response = await self.session.get(f"{self.base_url}/acts/{actor_to_check}")
            
            if response.status == 200:
                return await response.json()
            else:
                return {"status": "error", "code": response.status}
                
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error(f"Network error getting actor status: {e}")
            return {"status": "network_error"}
    
    async def test_actor_connection(self) -> bool:
        """Test if the configured actor is accessible."""
        try:
            status = await self.get_actor_status()
            return status.get("status") != "error"
        except Exception as e:
            logger.error(f"Actor test failed: {e}")
            return False


# Global service instance
apify_service = ApifyService()
