"""
Explicit normalization layer.
Converts raw scraper output into safe internal model.
No external data bypasses this layer.
"""
import re
from typing import List, Dict, Any, Optional
from datetime import datetime

from app.errors import NormalizationError
from app.models.product import AmazonProduct, SimilarKeyword


class AmazonNormalizer:
    """
    Normalizes raw Amazon scraper data into internal model.
    Fixes types, applies defaults, removes malformed fields.
    """
    
    @staticmethod
    def normalize_product(raw_product: Dict[str, Any]) -> AmazonProduct:
        """
        Convert raw scraper product to internal model.
        
        Args:
            raw_product: Raw JSON from Amazon scraper
            
        Returns:
            Normalized AmazonProduct
            
        Raises:
            NormalizationError: If data cannot be normalized
        """
        try:
            # Extract and normalize ASIN
            asin = AmazonNormalizer._normalize_asin(raw_product.get("asin"))
            
            # Normalize similar keywords
            similar_keywords = AmazonNormalizer._normalize_similar_keywords(
                raw_product.get("similarKeywords", [])
            )
            
            # Normalize price fields
            price = AmazonNormalizer._normalize_price(raw_product.get("price"))
            retail_price = AmazonNormalizer._normalize_price(raw_product.get("retailPrice"))
            
            # Normalize rating (ensure 0-5 range)
            rating = AmazonNormalizer._normalize_rating(raw_product.get("productRating"))
            
            # Normalize review count (ensure non-negative)
            review_count = AmazonNormalizer._normalize_review_count(raw_product.get("countReview"))
            
            # Create internal model
            return AmazonProduct(
                asin=asin,
                keyword=raw_product.get("keyword", "").strip(),
                domain_code=raw_product.get("domainCode", "com"),
                search_result_position=raw_product.get("searchResultPosition", 999),
                count_review=review_count,
                product_rating=rating,
                price=price,
                retail_price=retail_price,
                img_url=raw_product.get("imgUrl", "").strip(),
                dp_url=raw_product.get("dpUrl", "").strip(),
                sponsored=bool(raw_product.get("sponsored", False)),
                prime=bool(raw_product.get("prime", False)),
                product_description=AmazonNormalizer._normalize_description(
                    raw_product.get("productDescription")
                ),
                sales_volume=raw_product.get("salesVolume", "").strip() or None,
                manufacturer=raw_product.get("manufacturer", "").strip() or None,
                page=raw_product.get("page", 1),
                sort_strategy=raw_product.get("sortStrategy", "relevanceblender"),
                result_count=raw_product.get("resultCount", 0),
                similar_keywords=similar_keywords,
                categories=raw_product.get("categories", []),
                variations=raw_product.get("variations", []),
                product_details=raw_product.get("productDetails", []),
                scraped_at=datetime.utcnow()
            )
            
        except (KeyError, ValueError, TypeError) as e:
            raise NormalizationError(
                f"Failed to normalize product data: {str(e)}. "
                f"Raw data: {raw_product.get('asin', 'UNKNOWN_ASIN')}"
            )
    
    @staticmethod
    def _normalize_asin(raw_asin: Any) -> str:
        """Normalize ASIN (10 characters, uppercase)."""
        if not raw_asin:
            raise ValueError("ASIN is required")
        
        asin_str = str(raw_asin).strip().upper()
        # Remove any non-alphanumeric characters
        asin_str = re.sub(r'[^A-Z0-9]', '', asin_str)
        
        if len(asin_str) != 10:
            raise ValueError(f"ASIN must be 10 characters, got: {asin_str}")
        
        return asin_str
    
    @staticmethod
    def _normalize_price(raw_price: Any) -> Optional[float]:
        """Normalize price to float or None."""
        if raw_price is None or raw_price == "":
            return None
        
        try:
            # Handle string prices like "$29.99" or "29.99"
            if isinstance(raw_price, str):
                # Remove currency symbols and commas
                clean_price = re.sub(r'[^\d.]', '', raw_price)
                if not clean_price:
                    return None
                return float(clean_price)
            
            return float(raw_price)
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def _normalize_rating(raw_rating: Any) -> float:
        """Normalize rating to 0-5 range."""
        try:
            rating = float(raw_rating or 0)
            # Ensure rating is between 0 and 5
            return max(0.0, min(5.0, rating))
        except (ValueError, TypeError):
            return 0.0
    
    @staticmethod
    def _normalize_review_count(raw_count: Any) -> int:
        """Normalize review count to non-negative integer."""
        try:
            count = int(raw_count or 0)
            return max(0, count)
        except (ValueError, TypeError):
            return 0
    
    @staticmethod
    def _normalize_description(raw_description: Any) -> Optional[str]:
        """Normalize product description."""
        if not raw_description:
            return None
        
        desc = str(raw_description).strip()
        # Remove "No Product Description Found" placeholder
        if desc.lower() == "no product description found":
            return None
        
        return desc
    
    @staticmethod
    def _normalize_similar_keywords(raw_keywords: List[Dict]) -> List[SimilarKeyword]:
        """Normalize similar keywords list."""
        normalized = []
        
        for kw in raw_keywords or []:
            try:
                keyword = kw.get("keyword", "").strip()
                url = kw.get("url", "").strip()
                
                if keyword and url:
                    normalized.append(SimilarKeyword(keyword=keyword, url=url))
            except (KeyError, AttributeError):
                continue  # Skip malformed entries
        
        return normalized
    
    @staticmethod
    def normalize_batch(raw_products: List[Dict[str, Any]]) -> List[AmazonProduct]:
        """
        Normalize a batch of products.
        
        Args:
            raw_products: List of raw scraper products
            
        Returns:
            List of normalized products (skipping failures)
        """
        normalized = []
        
        for raw in raw_products:
            try:
                product = AmazonNormalizer.normalize_product(raw)
                if product.is_valid:
                    normalized.append(product)
            except NormalizationError as e:
                # Log but continue processing other products
                # Sentry will capture this in production
                continue
        
        return normalized