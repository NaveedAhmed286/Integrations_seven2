"""
Explicit normalization layer.
Converts junglee/free-amazon-product-scraper output into safe internal model.
"""
import re
from typing import List, Dict, Any, Optional
from datetime import datetime

from app.errors import NormalizationError
from app.models.product import AmazonProduct


class AmazonNormalizer:
    """
    Normalizes raw Amazon scraper data into internal model.
    Handles junglee/free-amazon-product-scraper output format.
    """
    
    @staticmethod
    def normalize_product(raw_product: Dict[str, Any]) -> AmazonProduct:
        """
        Convert junglee scraper product to internal model.
        
        Expected fields from junglee:
        - title, price, rating, url, asin, availability, image, etc.
        
        Returns:
            Normalized AmazonProduct
            
        Raises:
            NormalizationError: If data cannot be normalized
        """
        try:
            # Extract and normalize ASIN from URL or asin field
            asin = AmazonNormalizer._extract_asin(
                raw_product.get("asin"),
                raw_product.get("url", "")
            )
            
            # Normalize price
            price = AmazonNormalizer._normalize_price(raw_product.get("price"))
            
            # Normalize rating (extract numeric from string like "4.4 out of 5 stars")
            rating = AmazonNormalizer._extract_rating(raw_product.get("rating"))
            
            # Normalize review count if available
            review_count = AmazonNormalizer._normalize_review_count(
                raw_product.get("reviewsCount") or raw_product.get("totalReviews")
            )
            
            # Normalize availability
            availability = AmazonNormalizer._normalize_availability(
                raw_product.get("availability")
            )
            
            # Create internal model - simplified for junglee output
            return AmazonProduct(
                asin=asin or "UNKNOWN",
                keyword=raw_product.get("keyword", "").strip(),
                domain_code=raw_product.get("domain", "com"),
                search_result_position=raw_product.get("position", 999),
                count_review=review_count,
                product_rating=rating,
                price=price,
                retail_price=price,  # Use same price if no retail price
                img_url=raw_product.get("image", "").strip() or raw_product.get("imgUrl", ""),
                dp_url=raw_product.get("url", "").strip(),
                sponsored=bool(raw_product.get("sponsored", False)),
                prime=bool(raw_product.get("prime", False)) or "prime" in str(raw_product.get("delivery", "")).lower(),
                product_description=raw_product.get("description", "").strip() or None,
                sales_volume=raw_product.get("salesVolume", "").strip() or None,
                manufacturer=raw_product.get("manufacturer", "").strip() or raw_product.get("brand", "").strip() or None,
                page=1,
                sort_strategy="relevance",
                result_count=0,
                similar_keywords=[],
                categories=raw_product.get("categories", []),
                variations=raw_product.get("variations", []),
                product_details=raw_product.get("productDetails", []),
                availability=availability,
                scraped_at=datetime.utcnow()
            )
            
        except (KeyError, ValueError, TypeError) as e:
            raise NormalizationError(
                f"Failed to normalize product: {str(e)}. "
                f"Data keys: {list(raw_product.keys())}"
            )
    
    @staticmethod
    def _extract_asin(asin_field: Any, url: str) -> Optional[str]:
        """Extract ASIN from field or URL."""
        # Try from asin field first
        if asin_field:
            asin_str = str(asin_field).strip().upper()
            asin_str = re.sub(r'[^A-Z0-9]', '', asin_str)
            if len(asin_str) == 10:
                return asin_str
        
        # Try to extract from URL (e.g., /dp/B0DWK6GBB8)
        if url:
            match = re.search(r'/dp/([A-Z0-9]{10})', url.upper())
            if match:
                return match.group(1)
        
        return None
    
    @staticmethod
    def _normalize_price(raw_price: Any) -> Optional[float]:
        """Normalize price to float."""
        if raw_price is None or raw_price == "":
            return None
        
        try:
            if isinstance(raw_price, str):
                # Remove currency symbols, commas, spaces
                clean_price = re.sub(r'[^\d.]', '', raw_price)
                if clean_price and clean_price != ".":
                    return float(clean_price)
            elif isinstance(raw_price, (int, float)):
                return float(raw_price)
        except (ValueError, TypeError):
            pass
        
        return None
    
    @staticmethod
    def _extract_rating(rating_str: Any) -> float:
        """Extract numeric rating from string."""
        if not rating_str:
            return 0.0
        
        try:
            if isinstance(rating_str, (int, float)):
                return max(0.0, min(5.0, float(rating_str)))
            
            # Handle "4.4 out of 5 stars" format
            str_rating = str(rating_str)
            match = re.search(r'(\d+\.?\d*)', str_rating)
            if match:
                rating = float(match.group(1))
                return max(0.0, min(5.0, rating))
        except (ValueError, TypeError):
            pass
        
        return 0.0
    
    @staticmethod
    def _normalize_review_count(raw_count: Any) -> int:
        """Normalize review count."""
        if not raw_count:
            return 0
        
        try:
            if isinstance(raw_count, str):
                # Remove commas and non-digits
                clean = re.sub(r'[^\d]', '', raw_count)
                if clean:
                    return int(clean)
            else:
                return int(raw_count)
        except (ValueError, TypeError):
            pass
        
        return 0
    
    @staticmethod
    def _normalize_availability(availability: Any) -> str:
        """Normalize availability string."""
        if not availability:
            return "Unknown"
        
        avail_str = str(availability).strip()
        
        # Standardize common availability strings
        if "in stock" in avail_str.lower():
            return "In Stock"
        elif "out of stock" in avail_str.lower():
            return "Out of Stock"
        elif "pre-order" in avail_str.lower():
            return "Pre-order"
        elif "temporarily" in avail_str.lower():
            return "Temporarily Unavailable"
        
        return avail_str
    
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
                normalized.append(product)
            except NormalizationError:
                # Skip products that can't be normalized
                continue
        
        return normalized
