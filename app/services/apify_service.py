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
        self.actor_name = "apify~web-scraper"        
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
            # FINAL WORKING PAGE FUNCTION
            page_function = f"""async function pageFunction(context) {{
                const $ = context.jQuery;
                const results = [];
                
                $('div[data-asin]:not([data-asin=""])').each((index, element) => {{
                    const $el = $(element);
                    const asin = $el.attr('data-asin');
                    
                    // Title extraction
                    let title = '';
                    const titleSources = [
                        $el.find('h2 span'),
                        $el.find('.a-text-normal'),
                        $el.find('span.a-text-normal')
                    ];
                    
                    for (const source of titleSources) {{
                        const titleText = $(source).first().text().trim();
                        if (titleText && titleText.length > 10) {{
                            title = titleText;
                            break;
                        }}
                    }}
                    
                    // Price extraction - with fallback
                    let price = 'Price not found';
                    const allText = $el.text();
                    const priceMatch = allText.match(/\\$[\\d,]+\\.\\d{{2}}/);
                    if (priceMatch) {{
                        price = priceMatch[0];
                    }}
                    
                    // URL
                    const urlPath = $el.find('a[href*="/dp/"]').first().attr('href');
                    const url = urlPath ? 'https://www.amazon.{domain}' + urlPath.split('?')[0] : '';
                    
                    if (title && asin) {{
                        results.push({{
                            asin: asin,
                            title: title,
                            price: price,
                            url: url,
                            keyword: '{keyword}',
                            position: index + 1,
                            scraped_at: new Date().toISOString()
                        }});
                    }}
                }});
                
                return results.slice(0, {max_results});
            }}"""
            
            # Actor input - PROPERLY INDENTED!
            actor_input = {
                "startUrls": [{
                    "url": f"https://www.amazon.{domain}/s?k={keyword.replace(' ', '+')}"
                }],
                "maxRequestsPerCrawl": 1,
                "pageFunction": page_function,
                "injectJQuery": True,
                "proxyConfiguration": {
                    "useApifyProxy": True,
                    "apifyProxyGroups": ["RESIDENTIAL"]
                },
                "maxItems": max_results,
                "waitUntil": ["networkidle2"],
                "dynamicContentWaitSecs": 10
            }
            
            logger.debug(f"Sending request to Apify actor: {self.actor_name}")
            
            # Use run-sync-get-dataset-items
            response = await self.session.post(
                f"{self.base_url}/acts/{self.actor_name}/run-sync-get-dataset-items",
                json=actor_input,
                timeout=aiohttp.ClientTimeout(total=180)
            )
            
            # FIXED: Accept both 200 and 201 status codes
            if response.status not in [200, 201]:
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
    async def fetch_dataset(self, dataset_id: str) -> List[Dict[str, Any]]:
        """
        Fetch dataset items from Apify.
        
        Args:
            dataset_id: Apify dataset ID
            
        Returns:
            List of dataset items
            
        Raises:
            ExternalServiceError: If Apify API fails
        """
        if not self.is_available:
            raise ExternalServiceError("Apify service not configured")
        
        logger.info(f"Fetching dataset: {dataset_id}")
        
        try:
            response = await self.session.get(
                f"{self.base_url}/datasets/{dataset_id}/items",
                timeout=aiohttp.ClientTimeout(total=60)
            )
            
            # FIXED: Accept both 200 and 201 status codes
            if response.status not in [200, 201]:
                error_text = await response.text()
                logger.error(f"Failed to fetch dataset {dataset_id}: {error_text[:200]}")
                raise ExternalServiceError(f"Failed to fetch dataset: {response.status}")
            
            # Parse response
            items = await response.json()
            
            if not isinstance(items, list):
                logger.warning(f"Unexpected dataset format: {type(items)}")
                return []
            
            logger.info(f"Successfully fetched {len(items)} items from dataset {dataset_id}")
            return items
            
        except aiohttp.ClientError as e:
            logger.error(f"Network error fetching dataset: {str(e)}")
            raise NetworkError(f"Network error fetching dataset: {str(e)}") from e
        except asyncio.TimeoutError as e:
            logger.error(f"Timeout fetching dataset: {str(e)}")
            raise NetworkError(f"Timeout fetching dataset: {str(e)}") from e
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response from dataset: {str(e)}")
            raise ExternalServiceError(f"Invalid JSON response from dataset: {str(e)}") from e
        except Exception as e:
            logger.error(f"Unexpected error in fetch_dataset: {e}", exc_info=True)
            raise ExternalServiceError(f"Failed to fetch dataset: {str(e)}") from e
    
    @async_retry(exceptions=(aiohttp.ClientError, asyncio.TimeoutError))
    async def get_actor_status(self, actor_id: str = None) -> Dict[str, Any]:
        """Get Apify actor status."""
        if not self.is_available:
            return {"status": "not_configured"}
        
        actor_to_check = actor_id or self.actor_name
        
        try:
            response = await self.session.get(f"{self.base_url}/acts/{actor_to_check}")
            
            # FIXED: Accept both 200 and 201 status codes
            if response.status in [200, 201]:
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
