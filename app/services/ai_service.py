"""
Wrapper for DeepSeek/AI services.
Includes timeout, retry, response validation, error translation.
All AI/LLM calls are isolated here.
"""
import aiohttp
import asyncio
import json
from typing import Dict, Any, List, Optional
import hashlib

from app.errors import ExternalServiceError, NetworkError
from app.utils.retry import async_retry, idempotent_operation
from app.config import config
from app.logger import logger
from app.memory.memory_manager import memory_manager


class AIService:
    """
    Wrapper for AI/LLM API calls (DeepSeek, OpenAI, Anthropic, etc.).
    Business logic never calls AI APIs directly.
    """
    
    def __init__(self):
        self.api_key = config.DEEPSEEK_API_KEY
        self.base_url = "https://api.deepseek.com/v1"
        self.session: Optional[aiohttp.ClientSession] = None
        self.is_available = bool(self.api_key)
        self.model = "deepseek-chat"  # Default model
        
        # Cache for similar analysis requests
        self.analysis_cache: Dict[str, Dict[str, Any]] = {}
    
    async def initialize(self):
        """Initialize HTTP session (called after startup)."""
        if not self.is_available:
            logger.warning("AI service not configured")
            return
        
        self.session = aiohttp.ClientSession(
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            },
            timeout=aiohttp.ClientTimeout(total=config.REQUEST_TIMEOUT)
        )
        logger.info(f"AI service initialized with model: {self.model}")
    
    async def close(self):
        """Close HTTP session."""
        if self.session:
            await self.session.close()
    
    @async_retry(exceptions=(aiohttp.ClientError, asyncio.TimeoutError))
    async def chat_completion(self, messages: List[Dict[str, str]], 
                            temperature: float = 0.7,
                            max_tokens: int = 1000,
                            client_id: str = "default") -> Dict[str, Any]:
        """
        Send chat completion request to DeepSeek.
        
        Args:
            messages: List of message dicts [{"role": "user", "content": "..."}]
            temperature: Creativity (0.0 to 1.0)
            max_tokens: Maximum response length
            client_id: Client identifier for memory/caching
            
        Returns:
            AI response with content and metadata
            
        Raises:
            ExternalServiceError: If AI API fails
            NetworkError: If network connection fails
        """
        if not self.is_available:
            raise ExternalServiceError("AI service not configured")
        
        try:
            # Get AI context from memory
            memory_context = await memory_manager.get_ai_context(client_id)
            
            # Prepare payload with memory context
            payload = {
                "model": self.model,
                "messages": self._prepare_messages_with_context(messages, memory_context),
                "temperature": max(0.0, min(1.0, temperature)),
                "max_tokens": max_tokens,
                "stream": False
            }
            
            # Create cache key for idempotency
            cache_key = self._create_cache_key(payload, client_id)
            
            # Check cache
            cached_response = await memory_manager.retrieve_short_term(
                client_id, f"ai_cache_{cache_key}"
            )
            
            if cached_response:
                logger.info(f"Using cached AI response for: {cache_key[:32]}...")
                return cached_response["response"]
            
            # Make API request
            response = await self.session.post(
                f"{self.base_url}/chat/completions",
                json=payload
            )
            
            if response.status != 200:
                error_text = await response.text()
                raise ExternalServiceError(
                    f"AI API error {response.status}: {error_text}"
                )
            
            # Parse response
            result = await response.json()
            
            # Validate response structure
            if not isinstance(result, dict) or "choices" not in result:
                raise ExternalServiceError("Invalid response format from AI API")
            
            # Extract content
            ai_response = {
                "content": result["choices"][0]["message"]["content"],
                "model": result.get("model", self.model),
                "usage": result.get("usage", {}),
                "finish_reason": result["choices"][0].get("finish_reason", "stop"),
                "id": result.get("id", ""),
                "created": result.get("created", 0)
            }
            
            # Cache the response (24 hours)
            await memory_manager.store_short_term(
                client_id,
                f"ai_cache_{cache_key}",
                {
                    "response": ai_response,
                    "timestamp": asyncio.get_event_loop().time(),
                    "messages_hash": cache_key
                },
                ttl=86400  # 24 hours
            )
            
            # Store in episodic memory (summarized)
            self._store_ai_interaction(client_id, messages, ai_response)
            
            logger.info(f"AI response generated (tokens: {ai_response['usage'].get('total_tokens', 0)})")
            return ai_response
            
        except aiohttp.ClientError as e:
            raise NetworkError(f"Network error calling AI API: {str(e)}") from e
        except asyncio.TimeoutError as e:
            raise NetworkError(f"Timeout calling AI API: {str(e)}") from e
        except json.JSONDecodeError as e:
            raise ExternalServiceError(f"Invalid JSON response from AI API: {str(e)}") from e
    
    def _prepare_messages_with_context(self, messages: List[Dict[str, str]], 
                                      memory_context: Dict[str, Any]) -> List[Dict[str, str]]:
        """
        Prepare messages with memory context for AI.
        
        Rules:
        - Memory is read-only for AI
        - Only summarized context is provided
        - No raw prompts or full payloads
        """
        system_message = {
            "role": "system",
            "content": self._create_system_prompt(memory_context)
        }
        
        # Add system message at the beginning
        return [system_message] + messages
    
    def _create_system_prompt(self, memory_context: Dict[str, Any]) -> str:
        """Create system prompt with memory context."""
        base_prompt = """You are an expert Amazon product analyst. Analyze products based on the provided data.

Guidelines:
1. Be concise and factual
2. Focus on actionable insights
3. Consider pricing, ratings, and competition
4. Highlight opportunities and risks

"""
        
        # Add memory context if available
        if memory_context.get("episodic_summary"):
            base_prompt += "\nPrevious analyses:\n"
            for memory in memory_context["episodic_summary"][-3:]:  # Last 3 analyses
                base_prompt += f"- {memory['when']}: {memory['analysis']} - Key insights: {', '.join(memory['key_insights'][:2])}\n"
        
        if memory_context.get("recent_insights"):
            base_prompt += "\nRecent insights:\n"
            for insight in memory_context["recent_insights"][-5:]:  # Last 5 insights
                if "value" in insight and "insights" in insight["value"]:
                    for item in insight["value"]["insights"][:2]:
                        base_prompt += f"- {item}\n"
        
        return base_prompt
    
    def _create_cache_key(self, payload: Dict[str, Any], client_id: str) -> str:
        """Create cache key for AI request."""
        # Remove variable fields
        cache_payload = payload.copy()
        if "messages" in cache_payload:
            # Only use the last user message for cache key
            user_messages = [m for m in cache_payload["messages"] if m["role"] == "user"]
            if user_messages:
                cache_payload["messages"] = [user_messages[-1]]
        
        # Create hash
        payload_str = json.dumps(cache_payload, sort_keys=True)
        return hashlib.md5(f"{client_id}_{payload_str}".encode()).hexdigest()
    
    def _store_ai_interaction(self, client_id: str, messages: List[Dict[str, str]], 
                             response: Dict[str, Any]):
        """Store AI interaction in episodic memory."""
        try:
            # Extract user message (last user message)
            user_messages = [m for m in messages if m["role"] == "user"]
            last_user_message = user_messages[-1]["content"] if user_messages else ""
            
            # Summarize for memory
            input_summary = {
                "message_count": len(messages),
                "last_user_message_preview": last_user_message[:100] + "..." if len(last_user_message) > 100 else last_user_message
            }
            
            output_summary = {
                "response_preview": response["content"][:150] + "..." if len(response["content"]) > 150 else response["content"],
                "tokens_used": response["usage"].get("total_tokens", 0),
                "model": response["model"]
            }
            
            # Extract insights from response
            insights = self._extract_insights_from_response(response["content"])
            
            memory_manager.store_episodic(
                client_id=client_id,
                analysis_type="ai_analysis",
                input_data=input_summary,
                output_data=output_summary,
                insights=insights[:3]  # Top 3 insights
            )
            
        except Exception as e:
            logger.error(f"Failed to store AI interaction in memory: {e}")
    
    def _extract_insights_from_response(self, content: str) -> List[str]:
        """Extract key insights from AI response."""
        insights = []
        
        # Simple extraction logic (could be enhanced)
        lines = content.split('\n')
        for line in lines:
            line = line.strip()
            if line.startswith(('- ', '* ', '• ', '> ')):
                insights.append(line[2:].strip())
            elif ':' in line and len(line) < 100:
                insights.append(line)
            elif len(line) < 50 and line and not line.startswith('#'):
                insights.append(line)
        
        return insights[:5]  # Limit to 5 insights
    
    @async_retry(exceptions=(aiohttp.ClientError, asyncio.TimeoutError))
    @idempotent_operation("analyze_product_competitiveness")
    async def analyze_product_competitiveness(self, product_data: Dict[str, Any], 
                                            client_id: str = "default") -> Dict[str, Any]:
        """
        Analyze product competitiveness using AI.
        
        Args:
            product_data: Product information (from normalized model)
            client_id: Client identifier
            
        Returns:
            Analysis results with scores and insights
        """
        if not self.is_available:
            return self._get_fallback_analysis(product_data)
        
        try:
            # Prepare analysis prompt
            messages = [
                {
                    "role": "user",
                    "content": self._create_competitiveness_prompt(product_data)
                }
            ]
            
            # Get AI analysis
            response = await self.chat_completion(
                messages=messages,
                temperature=0.3,  # Lower temperature for analytical tasks
                max_tokens=500,
                client_id=client_id
            )
            
            # Parse and structure the response
            analysis = self._parse_competitiveness_response(response["content"])
            
            # Store analysis in long-term memory
            await memory_manager.store_long_term(
                client_id,
                f"competitiveness_analysis_{product_data.get('asin', 'unknown')}",
                {
                    "asin": product_data.get("asin", ""),
                    "analysis": analysis,
                    "ai_model": response["model"],
                    "tokens_used": response["usage"].get("total_tokens", 0),
                    "timestamp": asyncio.get_event_loop().time()
                },
                source_analysis="ai_competitiveness"
            )
            
            return analysis
            
        except Exception as e:
            logger.error(f"AI competitiveness analysis failed: {e}")
            return self._get_fallback_analysis(product_data)
    
    def _create_competitiveness_prompt(self, product_data: Dict[str, Any]) -> str:
        """Create prompt for product competitiveness analysis."""
        prompt = f"""Analyze this Amazon product's competitiveness:

Product Details:
- ASIN: {product_data.get('asin', 'Unknown')}
- Title/Keyword: {product_data.get('keyword', 'Unknown')}
- Rating: {product_data.get('product_rating', 0)}/5
- Reviews: {product_data.get('count_review', 0)}
- Price: ${product_data.get('price', 'Not available')}
- Retail Price: ${product_data.get('retail_price', 'Not available')}
- Sponsored: {product_data.get('sponsored', False)}
- Prime: {product_data.get('prime', False)}
- Search Position: {product_data.get('search_result_position', 999)}

Provide analysis in this JSON format:
{{
  "competitiveness_score": 0-100,
  "price_competitiveness": "high/medium/low",
  "rating_competitiveness": "high/medium/low",
  "review_competitiveness": "high/medium/low",
  "key_strengths": ["strength1", "strength2"],
  "key_weaknesses": ["weakness1", "weakness2"],
  "opportunities": ["opportunity1", "opportunity2"],
  "recommendations": ["recommendation1", "recommendation2"]
}}

Be concise and data-driven."""
        
        return prompt
    
    def _parse_competitiveness_response(self, content: str) -> Dict[str, Any]:
        """Parse AI response for competitiveness analysis."""
        try:
            # Try to extract JSON from response
            import re
            
            # Find JSON in the response
            json_match = re.search(r'\{.*\}', content, re.DOTALL)
            if json_match:
                return json.loads(json_match.group())
            
            # Fallback: create structured response from text
            return {
                "competitiveness_score": 50,
                "price_competitiveness": "unknown",
                "rating_competitiveness": "unknown",
                "review_competitiveness": "unknown",
                "key_strengths": [],
                "key_weaknesses": [],
                "opportunities": [],
                "recommendations": [],
                "raw_response": content[:500]  # Store first 500 chars
            }
            
        except (json.JSONDecodeError, AttributeError) as e:
            logger.error(f"Failed to parse AI response: {e}")
            return self._get_fallback_analysis({})
    
    def _get_fallback_analysis(self, product_data: Dict[str, Any]) -> Dict[str, Any]:
        """Provide fallback analysis when AI is unavailable."""
        rating = product_data.get("product_rating", 0)
        reviews = product_data.get("count_review", 0)
        price = product_data.get("price", 0)
        
        # Simple rule-based analysis
        score = 50
        
        if rating >= 4.0:
            score += 20
        elif rating >= 3.0:
            score += 10
        
        if reviews >= 100:
            score += 15
        elif reviews >= 10:
            score += 5
        
        if price and price < 50:
            score += 10
        
        return {
            "competitiveness_score": min(100, max(0, score)),
            "price_competitiveness": "high" if (price and price < 30) else "medium" if (price and price < 100) else "low",
            "rating_competitiveness": "high" if rating >= 4.0 else "medium" if rating >= 3.0 else "low",
            "review_competitiveness": "high" if reviews >= 100 else "medium" if reviews >= 10 else "low",
            "key_strengths": ["Fallback analysis - AI service unavailable"],
            "key_weaknesses": ["Using rule-based fallback"],
            "opportunities": ["Enable AI service for detailed analysis"],
            "recommendations": ["Configure DEEPSEEK_API_KEY environment variable"],
            "is_fallback": True
        }
    
    @async_retry(exceptions=(aiohttp.ClientError, asyncio.TimeoutError))
    async def analyze_market_trends(self, products: List[Dict[str, Any]], 
                                  client_id: str = "default") -> Dict[str, Any]:
        """
        Analyze market trends from multiple products.
        
        Args:
            products: List of product data
            client_id: Client identifier
            
        Returns:
            Market trend analysis
        """
        if not self.is_available or len(products) < 3:
            return self._get_fallback_trend_analysis(products)
        
        try:
            # Prepare trend analysis prompt
            products_summary = []
            for i, product in enumerate(products[:10]):  # Limit to 10 products
                products_summary.append(
                    f"Product {i+1}: {product.get('asin', 'Unknown')} - "
                    f"Rating: {product.get('product_rating', 0)} - "
                    f"Price: ${product.get('price', 'N/A')} - "
                    f"Reviews: {product.get('count_review', 0)} - "
                    f"Position: {product.get('search_result_position', 999)}"
                )
            
            messages = [
                {
                    "role": "user",
                    "content": f"""Analyze these Amazon products for market trends:

{chr(10).join(products_summary)}

Provide analysis of:
1. Average price range and competitiveness
2. Rating distribution
3. Review volume patterns
4. Sponsored vs organic presence
5. Market saturation indicators
6. Opportunities for new products

Format as concise bullet points."""
                }
            ]
            
            response = await self.chat_completion(
                messages=messages,
                temperature=0.4,
                max_tokens=800,
                client_id=client_id
            )
            
            return {
                "trend_analysis": response["content"],
                "products_analyzed": len(products),
                "model": response["model"],
                "tokens_used": response["usage"].get("total_tokens", 0)
            }
            
        except Exception as e:
            logger.error(f"AI market trend analysis failed: {e}")
            return self._get_fallback_trend_analysis(products)
    
    def _get_fallback_trend_analysis(self, products: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Fallback trend analysis."""
        if not products:
            return {"error": "No products to analyze", "is_fallback": True}
        
        # Simple calculations
        ratings = [p.get("product_rating", 0) for p in products]
        prices = [p.get("price", 0) for p in products if p.get("price")]
        reviews = [p.get("count_review", 0) for p in products]
        sponsored = sum(1 for p in products if p.get("sponsored", False))
        
        avg_rating = sum(ratings) / len(ratings) if ratings else 0
        avg_price = sum(prices) / len(prices) if prices else 0
        avg_reviews = sum(reviews) / len(reviews) if reviews else 0
        
        return {
            "trend_analysis": f"""
Fallback Analysis (AI unavailable):
- Products Analyzed: {len(products)}
- Average Rating: {avg_rating:.2f}/5
- Average Price: ${avg_price:.2f}
- Average Reviews: {avg_reviews:.1f}
- Sponsored Products: {sponsored}/{len(products)} ({(sponsored/len(products)*100):.1f}%)
- Configure AI service for detailed trend analysis.
""",
            "products_analyzed": len(products),
            "average_rating": avg_rating,
            "average_price": avg_price,
            "sponsored_percentage": (sponsored / len(products) * 100) if products else 0,
            "is_fallback": True
        }
    
    async def get_service_status(self) -> Dict[str, Any]:
        """
        Get AI service status.
        
        Returns:
            Service status information
        """
        if not self.is_available:
            return {"status": "not_configured", "model": "none"}
        
        try:
            # Try a simple request to check service health
            if self.session:
                response = await self.session.get(f"{self.base_url}/models")
                if response.status == 200:
                    return {"status": "available", "model": self.model}
                else:
                    return {"status": "error", "code": response.status}
            else:
                return {"status": "not_initialized"}
                
        except Exception:
            return {"status": "unavailable"}


# Global service instance
ai_service = AIService()