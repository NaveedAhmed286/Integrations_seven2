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
        self.actor_name = "apify/web-scraper"
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
        logger.info(f"Apify service initialized with actor: {self.actor_name}")
    
    async def close(self):
        """Close HTTP session."""
        if self.session:
            await self.session.close()
    
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
            raise ExternalServiceError("Apify service not configured")
        
        logger.info(f"Starting Amazon scrape for: '{keyword}' (domain: {domain}, max: {max_results})")
        
        try:
            # Optimized page function based on working actor test
            page_function = f"""
            async function pageFunction(context) {{
                const $ = context.jQuery;
                const results = [];
                
                // Extract products using selector that worked in tests
                $('[data-component-type="s-search-result"]').each((index, element) => {{
                    const $el = $(element);
                    
                    // Extract ASIN (unique Amazon ID)
                    const asin = $el.attr('data-asin') || $el.data('asin') || '';
                    
                    // Extract product title
                    const title = $el.find('h2 a span').first().text().trim();
                    
                    // Extract price - handle size variants issue
                    let price = '';
                    const priceSelectors = [
                        '.a-price .a-offscreen',
                        '.a-price-whole',
                        '.a-color-price'
                    ];
                    
                    for (const selector of priceSelectors) {{
                        const priceText = $el.find(selector).first().text().trim();
                        if (priceText && !priceText.includes('size')) {{
                            price = priceText;
                            break;
                        }}
                    }}
                    
                    // Extract rating
                    const ratingText = $el.find('.a-icon-alt').first().text();
                    const rating = ratingText ? ratingText.split(' ')[0] : '0';
                    
                    // Extract reviews
                    const reviewsText = $el.find('span.a-size-base.s-underline-text').first().text();
                    const reviews = reviewsText ? reviewsText.replace(/,/g, '') : '0';
                    
                    // Get URL
                    const urlPath = $el.find('h2 a').first().attr('href');
                    const url = urlPath ? 'https://www.amazon.com' + urlPath.split('?')[0] : '';
                    
                    // Check if sponsored
                    const sponsored = $el.find('span:contains("Sponsored")').length > 0;
                    
                    if (title) {{
                        results.push({{
                            asin: asin,
                            title: title,
                            price: price || 'Price not found',
                            rating: rating,
                            reviews: reviews,
                            url: url,
                            sponsored: sponsored,
                            position: index + 1,
                            keyword: '{keyword}',
                            scraped_at: new Date().toISOString()
                        }});
                    }}
                }});
                
                // Return limited results
                return results.slice(0, {max_results});
            }}
            """
            
            # Actor input based on working configuration
            actor_input = {
                "startUrls": [{
                    "url": f"https://www.amazon.{domain}/s?k={keyword.replace(' ', '+')}"
                }],
                "maxRequestsPerCrawl": 1,
                "pageFunction": page_function,
                "injectJQuery": True,
                "proxyConfiguration": {
                    "useApifyProxy": True
                },
                "maxItems": max_results,
                "waitUntil": "networkidle2",
                "dynamicContentWaitSecs": 10
            }
            
            logger.debug(f"Sending request to Apify actor: {self.actor_name}")
            
            # Use run-sync-get-dataset-items (simpler API)
            response = await self.session.post(
                f"{self.base_url}/acts/{self.actor_name}/run-sync-get-dataset-items",
                json=actor_input,
                timeout=aiohttp.ClientTimeout(total=180)  # 3 minutes timeout
            )
            
            if response.status != 200:
                error_text = await response.text()
                logger.error(f"Apify API error {response.status}: {error_text[:200]}")
                raise ExternalServiceError(
                    f"Apify API error {response.status}: {error_text[:200]}"
                )
            
            # Parse response
            results = await response.json()
            
            # Handle different response formats
            if isinstance(results, list):
                items = results
            elif isinstance(results, dict) and "items" in results:
                items = results["items"]
            else:
                logger.warning(f"Unexpected response format: {type(results)}")
                items = []
            
            if not isinstance(items, list):
                raise ExternalServiceError("Invalid response format from Apify")
            
            # Add keyword and domain to each result
            for item in items:
                item["keyword"] = keyword
                item["domain"] = domain
            
            logger.info(f"Successfully scraped {len(items)} products for keyword: {keyword}")
            return items
            
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
