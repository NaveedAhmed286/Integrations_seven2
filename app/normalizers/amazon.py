"""
Explicit normalization layer.
Converts raw Amazon scraper output into safe internal model.
Supports multiple actors: Junglee/free-amazon-product-scraper and apify/web-scraper.
"""
import re
from typing import List, Dict, Any, Optional
from datetime import datetime

from app.errors import NormalizationError
from app.models.product import AmazonProduct


class AmazonNormalizer:
    """
    Normalizes raw Amazon scraper data into internal model.
    Handles multiple actor outputs.
    """

    @staticmethod
    def normalize_product(raw_product: Dict[str, Any]) -> AmazonProduct:
        """
        Convert raw scraper product to internal model.

        Returns:
            Normalized AmazonProduct
        Raises:
            NormalizationError: If data cannot be normalized
        """
        try:
            # Extract ASIN
            asin = AmazonNormalizer._extract_asin(
                raw_product.get("asin") or raw_product.get("productId"),
                raw_product.get("url") or raw_product.get("dpUrl")
            )

            # Normalize price
            price = AmazonNormalizer._normalize_price(
                raw_product.get("price") or raw_product.get("currentPrice")
            )

            # Normalize rating
            rating = AmazonNormalizer._extract_rating(
                raw_product.get("rating") or raw_product.get("productRating")
            )

            # Normalize review count
            review_count = AmazonNormalizer._normalize_review_count(
                raw_product.get("reviewsCount") or raw_product.get("totalReviews") or raw_product.get("reviewCount")
            )

            # Normalize availability
            availability = AmazonNormalizer._normalize_availability(
                raw_product.get("availability") or raw_product.get("stockStatus")
            )

            # Normalize manufacturer/brand
            manufacturer = (raw_product.get("manufacturer") or raw_product.get("brand") or "").strip() or None

            # Normalize image URL
            img_url = raw_product.get("image") or raw_product.get("img_url") or raw_product.get("imgUrl") or ""

            # Normalize product description
            product_description = raw_product.get("description") or raw_product.get("productDescription") or None

            # Normalize sales volume safely
            sales_volume_raw = raw_product.get("salesVolume") or raw_product.get("sales_volume")
            sales_volume = 0
            if sales_volume_raw:
                if isinstance(sales_volume_raw, str):
                    clean = re.sub(r"[^\d]", "", sales_volume_raw)
                    sales_volume = int(clean) if clean else 0
                elif isinstance(sales_volume_raw, (int, float)):
                    sales_volume = int(sales_volume_raw)

            # Create internal model
            return AmazonProduct(
                asin=asin or "UNKNOWN",
                keyword=(raw_product.get("keyword") or "").strip(),
                domain_code=raw_product.get("domain") or "com",
                search_result_position=raw_product.get("position") or raw_product.get("searchResultPosition") or 999,
                count_review=review_count,
                product_rating=rating,
                price=price,
                retail_price=price,
                img_url=img_url.strip(),
                dp_url=(raw_product.get("url") or raw_product.get("dpUrl") or "").strip(),
                sponsored=bool(raw_product.get("sponsored", False)),
                prime=bool(raw_product.get("prime", False)) or "prime" in str(raw_product.get("delivery", "")).lower(),
                product_description=product_description,
                sales_volume=sales_volume,
                manufacturer=manufacturer,
                page=raw_product.get("page") or 1,
                sort_strategy=raw_product.get("sortStrategy") or "relevance",
                result_count=raw_product.get("resultCount") or 0,
                similar_keywords=raw_product.get("similarKeywords") or raw_product.get("similar_keywords") or [],
                categories=raw_product.get("categories") or [],
                variations=raw_product.get("variations") or [],
                product_details=raw_product.get("productDetails") or [],
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
        if asin_field:
            asin_str = str(asin_field).strip().upper()
            asin_str = re.sub(r"[^A-Z0-9]", "", asin_str)
            if len(asin_str) == 10:
                return asin_str

        if url:
            match = re.search(r"/dp/([A-Z0-9]{10})", url.upper())
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
                clean_price = re.sub(r"[^\d.]", "", raw_price)
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
            str_rating = str(rating_str)
            match = re.search(r"(\d+\.?\d*)", str_rating)
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
                clean = re.sub(r"[^\d]", "", raw_count)
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
        Skips failures to prevent pipeline crashes.
        """
        normalized = []
        for raw in raw_products:
            try:
                product = AmazonNormalizer.normalize_product(raw)
                normalized.append(product)
            except NormalizationError:
                continue
        return normalized
