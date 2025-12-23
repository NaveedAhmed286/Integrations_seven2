"""
Business orchestration logic.
Coordinates between services, normalizers, memory, and queues.
"""
from typing import List, Dict, Any, Optional
import asyncio

from app.errors import ExternalServiceError, NormalizationError
from app.config import config
from app.logger import logger
from app.models.product import AmazonProduct
from app.normalizers.amazon import AmazonNormalizer
from app.memory.memory_manager import memory_manager
from app.queue.workflow_queue import workflow_queue
from app.queue.retry_queue import retry_queue
from app.services.apify_service import apify_service
from app.services.google_service import google_sheets_service
from app.services import ai_service


class AmazonAgent:
    """
    Main business logic orchestrator.
    Never calls external APIs directly - uses service wrappers.
    """
    
    def __init__(self):
        self.client_id = "default"  # In production, this would come from auth
    
    def register_queues(self):
        """Register task handlers with queues."""
        
        # Workflow queue tasks
        workflow_queue.register_task(
            "scrape_amazon",
            self._task_scrape_amazon
        )
        workflow_queue.register_task(
            "normalize_products",
            self._task_normalize_products
        )
        workflow_queue.register_task(
            "analyze_products",
            self._task_analyze_products
        )
        workflow_queue.register_task(
            "persist_results",
            self._task_persist_results
        )
        
        # Retry queue operations
        retry_queue.register_operation(
            "apify_scrape",
            self._retry_apify_scrape
        )
        retry_queue.register_operation(
            "google_sheets_append",
            self._retry_google_sheets_append
        )
        retry_queue.register_operation(
            "ai_analysis",
            self._retry_ai_analysis
        )
        
        logger.info("Queue handlers registered")
    
    async def scrape_amazon_search(self, keyword: str, max_results: int = 10) -> str:
        """
        Start Amazon search scrape workflow.
        
        Args:
            keyword: Search keyword
            max_results: Maximum products to return
            
        Returns:
            Task ID
        """
        task_id = await workflow_queue.enqueue(
            "scrape_amazon",
            {
                "keyword": keyword,
                "max_results": max_results,
                "client_id": self.client_id
            }
        )
        
        logger.info(f"Started scrape workflow for '{keyword}': {task_id}")
        return task_id
    
    async def _task_scrape_amazon(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Workflow task: Scrape Amazon search.
        
        Args:
            data: Task data with keyword and max_results
            
        Returns:
            Raw scraped data
        """
        keyword = data["keyword"]
        max_results = data["max_results"]
        client_id = data["client_id"]
        
        try:
            # Store in episodic memory
            memory_manager.store_episodic(
                client_id=client_id,
                analysis_type="scrape_initiated",
                input_data={"keyword": keyword, "max_results": max_results},
                output_data={},
                insights=[f"Scraping Amazon for: {keyword}"]
            )
            
            # Check if we have recent results in memory
            cache_key = f"scrape_{keyword}_{max_results}"
            cached = await memory_manager.retrieve_short_term(client_id, cache_key)
            
            if cached:
                logger.info(f"Using cached results for: {keyword}")
                return {
                    "keyword": keyword,
                    "raw_products": cached["products"],
                    "source": "cache",
                    "cached_at": cached["timestamp"]
                }
            
            # Call Apify service (with retry logic inside wrapper)
            raw_products = await apify_service.scrape_amazon_search(
                keyword=keyword,
                max_results=max_results
            )
            
            # Cache results
            await memory_manager.store_short_term(
                client_id,
                cache_key,
                {
                    "products": raw_products,
                    "timestamp": asyncio.get_event_loop().time(),
                    "keyword": keyword
                },
                ttl=3600  # 1 hour cache
            )
            
            # Store in episodic memory
            memory_manager.store_episodic(
                client_id=client_id,
                analysis_type="scrape_completed",
                input_data={"keyword": keyword, "max_results": max_results},
                output_data={"product_count": len(raw_products)},
                insights=[
                    f"Found {len(raw_products)} products for '{keyword}'",
                    f"First ASIN: {raw_products[0].get('asin', 'unknown') if raw_products else 'none'}"
                ]
            )
            
            return {
                "keyword": keyword,
                "raw_products": raw_products,
                "source": "apify",
                "count": len(raw_products)
            }
            
        except ExternalServiceError as e:
            logger.error(f"Apify scrape failed for '{keyword}': {e}")
            
            # Enqueue for retry
            await retry_queue.enqueue_failed_operation(
                "apify_scrape",
                {
                    "keyword": keyword,
                    "max_results": max_results,
                    "client_id": client_id,
                    "_attempt": 0
                },
                error=str(e)
            )
            
            raise  # Re-raise to mark task as failed
    
    async def _retry_apify_scrape(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Retry handler for failed Apify scrapes."""
        return await self._task_scrape_amazon(data)
    
    async def _task_normalize_products(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Workflow task: Normalize raw products.
        
        Args:
            data: Task data with raw_products
            
        Returns:
            Normalized products
        """
        raw_products = data["raw_products"]
        keyword = data.get("keyword", "unknown")
        client_id = data.get("client_id", self.client_id)
        
        try:
            # Normalize using explicit normalization layer
            normalized_products = AmazonNormalizer.normalize_batch(raw_products)
            
            # Filter valid products
            valid_products = [p for p in normalized_products if p.is_valid]
            invalid_count = len(raw_products) - len(valid_products)
            
            if invalid_count > 0:
                logger.warning(
                    f"Filtered out {invalid_count} invalid products for '{keyword}'"
                )
            
            # Store normalization insights in long-term memory
            for product in valid_products[:5]:  # Store insights for first 5 products
                insight = {
                    "asin": product.asin,
                    "keyword": keyword,
                    "rating": product.product_rating,
                    "has_price": product.has_price,
                    "position": product.search_result_position,
                    "normalization_timestamp": asyncio.get_event_loop().time()
                }
                
                await memory_manager.store_long_term(
                    client_id,
                    f"product_insight_{product.asin}_{keyword}",
                    insight,
                    source_analysis="normalization"
                )
            
            # Store in episodic memory
            memory_manager.store_episodic(
                client_id=client_id,
                analysis_type="normalization_completed",
                input_data={"raw_count": len(raw_products), "keyword": keyword},
                output_data={"valid_count": len(valid_products), "invalid_count": invalid_count},
                insights=[
                    f"Normalized {len(valid_products)} valid products",
                    f"Average rating: {sum(p.product_rating for p in valid_products) / len(valid_products) if valid_products else 0:.2f}",
                    f"Sponsored products: {sum(1 for p in valid_products if p.sponsored)}"
                ]
            )
            
            return {
                "normalized_products": valid_products,
                "raw_count": len(raw_products),
                "valid_count": len(valid_products),
                "invalid_count": invalid_count
            }
            
        except Exception as e:
            logger.error(f"Normalization failed: {e}")
            raise NormalizationError(f"Failed to normalize products: {str(e)}")
    
    async def _task_analyze_products(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Workflow task: Analyze normalized products with AI.
        
        Args:
            data: Task data with normalized_products
            
        Returns:
            Analysis results with AI insights
        """
        products = data["normalized_products"]
        keyword = data.get("keyword", "unknown")
        client_id = data.get("client_id", self.client_id)
        
        if not products:
            return {"analysis": "no_products", "insights": []}
        
        try:
            # Get AI context from memory
            ai_context = await memory_manager.get_ai_context(client_id)
            
            # Basic statistical analysis
            analysis = {
                "total_products": len(products),
                "average_rating": sum(p.product_rating for p in products) / len(products),
                "products_with_price": sum(1 for p in products if p.has_price),
                "sponsored_count": sum(1 for p in products if p.sponsored),
                "prime_count": sum(1 for p in products if p.prime),
                "top_positions": [p.search_result_position for p in products[:5]],
                "unique_asins": len(set(p.asin for p in products)),
                "price_range": self._calculate_price_range(products)
            }
            
            # AI-powered analysis for top products
            ai_insights = []
            if config.has_ai and products:
                # Analyze top 3 products with AI
                top_products = products[:3]
                for product in top_products:
                    try:
                        product_dict = product.to_dict()
                        ai_analysis = await self._analyze_product_with_ai(
                            product_dict, client_id
                        )
                        
                        if ai_analysis and not ai_analysis.get("is_fallback", False):
                            ai_insights.append({
                                "asin": product.asin,
                                "competitiveness_score": ai_analysis.get("competitiveness_score", 0),
                                "strengths": ai_analysis.get("key_strengths", []),
                                "opportunities": ai_analysis.get("opportunities", [])
                            })
                            
                    except Exception as e:
                        logger.error(f"AI analysis failed for {product.asin}: {e}")
                        # Continue with other products
            
            # Generate insights
            insights = self._generate_insights(analysis, ai_insights)
            
            # Add AI insights to analysis
            if ai_insights:
                analysis["ai_insights"] = ai_insights
                analysis["has_ai_analysis"] = True
            else:
                analysis["has_ai_analysis"] = False
            
            # Store analysis in long-term memory
            analysis_insight = {
                "keyword": keyword,
                "timestamp": asyncio.get_event_loop().time(),
                "metrics": analysis,
                "insights": insights,
                "product_count": len(products),
                "ai_context_used": bool(ai_context.get("episodic_summary")),
                "ai_analysis_performed": bool(ai_insights)
            }
            
            await memory_manager.store_long_term(
                client_id,
                f"analysis_{keyword}_{asyncio.get_event_loop().time()}",
                analysis_insight,
                source_analysis="product_analysis"
            )
            
            # Store in episodic memory
            memory_manager.store_episodic(
                client_id=client_id,
                analysis_type="analysis_completed",
                input_data={"product_count": len(products), "keyword": keyword},
                output_data=analysis,
                insights=insights[:5]  # Top 5 insights
            )
            
            return {
                "analysis": analysis,
                "insights": insights,
                "ai_insights": ai_insights,
                "products_analyzed": len(products)
            }
            
        except Exception as e:
            logger.error(f"Analysis failed: {e}")
            return {
                "analysis": "failed",
                "error": str(e),
                "insights": []
            }
    
    def _calculate_price_range(self, products: List[AmazonProduct]) -> Dict[str, Any]:
        """Calculate price range statistics."""
        prices = [p.price for p in products if p.price is not None]
        
        if not prices:
            return {"min": None, "max": None, "average": None, "count": 0}
        
        return {
            "min": min(prices),
            "max": max(prices),
            "average": sum(prices) / len(prices),
            "count": len(prices)
        }
    
    def _generate_insights(self, analysis: Dict[str, Any], 
                          ai_insights: List[Dict[str, Any]]) -> List[str]:
        """Generate insights from analysis and AI results."""
        insights = []
        
        # Basic insights
        if analysis["average_rating"] > 4.0:
            insights.append(f"High average rating ({analysis['average_rating']:.2f})")
        
        if analysis["sponsored_count"] > analysis["total_products"] * 0.5:
            insights.append(f"High sponsored content ({analysis['sponsored_count']}/{analysis['total_products']})")
        
        if analysis["unique_asins"] < analysis["total_products"]:
            insights.append(f"Duplicate ASINs found: {analysis['total_products'] - analysis['unique_asins']}")
        
        # Price insights
        if analysis["price_range"]["count"] > 0:
            avg_price = analysis["price_range"]["average"]
            if avg_price < 30:
                insights.append(f"Low average price (${avg_price:.2f}) - competitive market")
            elif avg_price > 100:
                insights.append(f"High average price (${avg_price:.2f}) - premium market")
        
        # Add AI insights
        for ai_insight in ai_insights:
            score = ai_insight.get("competitiveness_score", 0)
            asin = ai_insight.get("asin", "Unknown")
            
            if score > 70:
                insights.append(f"AI: {asin} has high competitiveness ({score}/100)")
            elif score < 40:
                insights.append(f"AI: {asin} has low competitiveness ({score}/100)")
            
            # Add first strength if available
            strengths = ai_insight.get("strengths", [])
            if strengths:
                insights.append(f"AI: {asin} strength - {strengths[0]}")
        
        return insights
    
    async def _analyze_product_with_ai(self, product_data: Dict[str, Any], 
                                      client_id: str) -> Dict[str, Any]:
        """
        Analyze a single product with AI.
        
        Args:
            product_data: Product data dictionary
            client_id: Client identifier
            
        Returns:
            AI analysis results
        """
        try:
            return await ai_service.analyze_product_competitiveness(
                product_data=product_data,
                client_id=client_id
            )
        except ExternalServiceError as e:
            logger.error(f"AI service error for {product_data.get('asin', 'unknown')}: {e}")
            
            # Enqueue for retry
            await retry_queue.enqueue_failed_operation(
                "ai_analysis",
                {
                    "product_data": product_data,
                    "client_id": client_id,
                    "_attempt": 0
                },
                error=str(e)
            )
            
            # Return fallback analysis immediately
            return ai_service._get_fallback_analysis(product_data)
        except Exception as e:
            logger.error(f"Unexpected AI analysis error: {e}")
            return ai_service._get_fallback_analysis(product_data)
    
    async def _retry_ai_analysis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Retry handler for failed AI analysis."""
        return await self._analyze_product_with_ai(
            data["product_data"], 
            data["client_id"]
        )
    
    async def _task_persist_results(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Workflow task: Persist results to Google Sheets.
        
        Args:
            data: Task data with analysis results
            
        Returns:
            Persistence results
        """
        analysis = data.get("analysis", {})
        products = data.get("normalized_products", [])
        keyword = data.get("keyword", "unknown")
        client_id = data.get("client_id", self.client_id)
        
        try:
            if not config.has_google_sheets:
                logger.warning("Google Sheets not configured, skipping persistence")
                return {"persisted": False, "reason": "not_configured"}
            
            # Prepare data for Google Sheets
            sheet_data = []
            
            # Add analysis summary
            sheet_data.append({
                "keyword": keyword,
                "analysis_timestamp": asyncio.get_event_loop().time(),
                "total_products": analysis.get("total_products", 0),
                "average_rating": analysis.get("average_rating", 0),
                "products_with_price": analysis.get("products_with_price", 0),
                "sponsored_count": analysis.get("sponsored_count", 0),
                "unique_asins": analysis.get("unique_asins", 0)
            })
            
            # Add product data (first 50 products max)
            for product in products[:50]:
                sheet_data.append({
                    "asin": product.asin,
                    "keyword": keyword,
                    "rating": product.product_rating,
                    "price": product.price,
                    "position": product.search_result_position,
                    "reviews": product.count_review,
                    "sponsored": product.sponsored,
                    "prime": product.prime,
                    "has_price": product.has_price,
                    "image_url": product.img_url,
                    "product_url": product.dp_url
                })
            
            # Append to Google Sheets
            # In production, spreadsheet_id would come from config
            spreadsheet_id = config.GOOGLE_SHEETS_SPREADSHEET_ID if hasattr(config, 'GOOGLE_SHEETS_SPREADSHEET_ID') else "test_sheet"
            
            rows_appended = await google_sheets_service.append_to_sheet(
                spreadsheet_id=spreadsheet_id,
                worksheet_name=keyword[:30],  # Worksheet name max 30 chars
                data=sheet_data
            )
            
            # Store persistence in memory
            persistence_insight = {
                "keyword": keyword,
                "timestamp": asyncio.get_event_loop().time(),
                "rows_appended": rows_appended,
                "product_count": len(products),
                "spreadsheet": spreadsheet_id
            }
            
            await memory_manager.store_long_term(
                client_id,
                f"persistence_{keyword}_{asyncio.get_event_loop().time()}",
                persistence_insight,
                source_analysis="google_sheets_persistence"
            )
            
            # Store in episodic memory
            memory_manager.store_episodic(
                client_id=client_id,
                analysis_type="persistence_completed",
                input_data={"product_count": len(products), "keyword": keyword},
                output_data={"rows_appended": rows_appended},
                insights=[f"Persisted {rows_appended} rows to Google Sheets for '{keyword}'"]
            )
            
            return {
                "persisted": True,
                "rows_appended": rows_appended,
                "spreadsheet": spreadsheet_id,
                "worksheet": keyword[:30]
            }
            
        except ExternalServiceError as e:
            logger.error(f"Google Sheets persistence failed: {e}")
            
            # Enqueue for retry
            await retry_queue.enqueue_failed_operation(
                "google_sheets_append",
                {
                    "analysis": analysis,
                    "products": [p.to_dict() for p in products],
                    "keyword": keyword,
                    "client_id": client_id,
                    "_attempt": 0
                },
                error=str(e)
            )
            
            raise  # Re-raise to mark task as failed
    
    async def _retry_google_sheets_append(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Retry handler for failed Google Sheets operations."""
        return await self._task_persist_results(data)
    
    async def process_scraper_json(self, payload: Dict[str, Any]) -> List[AmazonProduct]:
        """
        Process scraper JSON directly (for testing).
        
        Args:
            payload: Raw scraper JSON
            
        Returns:
            List of normalized products
        """
        # Handle both single object and array
        if isinstance(payload, dict):
            raw_products = [payload]
        elif isinstance(payload, list):
            raw_products = payload
        else:
            raise ValueError("Payload must be dict or list")
        
        # Normalize
        normalized = AmazonNormalizer.normalize_batch(raw_products)
        
        # Store in memory for context
        if normalized:
            client_id = self.client_id
            keyword = normalized[0].keyword if normalized else "test"
            
            memory_manager.store_episodic(
                client_id=client_id,
                analysis_type="manual_json_processing",
                input_data={"raw_count": len(raw_products), "source": "manual"},
                output_data={"normalized_count": len(normalized)},
                insights=[f"Manually processed {len(normalized)} products for '{keyword}'"]
            )
        
        return normalized
    
    async def process_pending_tasks(self):
        """Process any pending workflow tasks."""
        # Workflow queue processes automatically
        # This method exists for manual triggering if needed
        logger.info("Processing pending tasks...")
        return True
    
    async def analyze_with_ai(self, product_data: Dict[str, Any], 
                             client_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Public method to analyze product with AI.
        
        Args:
            product_data: Product data (can be raw or normalized)
            client_id: Optional client identifier
            
        Returns:
            AI analysis results
        """
        client_id = client_id or self.client_id
        
        try:
            # Ensure we have normalized data
            if "asin" in product_data and "product_rating" in product_data:
                # Already normalized-ish
                normalized_data = product_data
            else:
                # Normalize first
                normalized = AmazonNormalizer.normalize_batch([product_data])
                if not normalized:
                    return {"error": "Failed to normalize product data"}
                normalized_data = normalized[0].to_dict()
            
            # Analyze with AI
            analysis = await self._analyze_product_with_ai(normalized_data, client_id)
            
            # Store in episodic memory
            memory_manager.store_episodic(
                client_id=client_id,
                analysis_type="direct_ai_analysis",
                input_data={"asin": normalized_data.get("asin", "unknown")},
                output_data={"competitiveness_score": analysis.get("competitiveness_score", 0)},
                insights=analysis.get("key_strengths", [])[:2] + analysis.get("opportunities", [])[:2]
            )
            
            return analysis
            
        except Exception as e:
            logger.error(f"Direct AI analysis failed: {e}")
            return ai_service._get_fallback_analysis(product_data)