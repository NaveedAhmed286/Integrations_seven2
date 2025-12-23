"""
Canonical internal data contract.
Everything depends on this shape.
"""
from dataclasses import dataclass, field
from typing import Optional, List, Dict
from datetime import datetime


@dataclass(frozen=True)
class SimilarKeyword:
    """Similar keyword data contract."""
    keyword: str
    url: str


@dataclass(frozen=True)
class AmazonProduct:
    """
    Canonical internal model for Amazon products.
    Used by Forms, Sheets, AI, queues, persistence, and memory layers.
    """
    # Required fields (from scraper)
    asin: str
    keyword: str
    domain_code: str
    search_result_position: int
    count_review: int
    product_rating: float
    img_url: str
    dp_url: str
    sponsored: bool
    prime: bool
    
    # Optional fields (may be null/empty in scraper output)
    price: Optional[float] = None
    retail_price: Optional[float] = None
    product_description: Optional[str] = None
    sales_volume: Optional[str] = None
    manufacturer: Optional[str] = None
    
    # Search metadata
    page: int = 1
    sort_strategy: str = "relevanceblender"
    result_count: int = 0
    
    # Lists (empty by default, not null)
    similar_keywords: List[SimilarKeyword] = field(default_factory=list)
    categories: List[str] = field(default_factory=list)
    variations: List[Dict] = field(default_factory=list)
    product_details: List[Dict] = field(default_factory=list)
    
    # System fields
    scraped_at: datetime = field(default_factory=datetime.utcnow)
    normalized_at: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def is_valid(self) -> bool:
        """Basic validation rules."""
        if not self.asin or len(self.asin) != 10:
            return False
        if self.product_rating < 0 or self.product_rating > 5:
            return False
        if self.count_review < 0:
            return False
        return True
    
    @property
    def has_price(self) -> bool:
        """Check if product has price information."""
        return self.price is not None and self.price > 0
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization."""
        return {
            "asin": self.asin,
            "keyword": self.keyword,
            "domain_code": self.domain_code,
            "search_result_position": self.search_result_position,
            "count_review": self.count_review,
            "product_rating": self.product_rating,
            "price": self.price,
            "retail_price": self.retail_price,
            "img_url": self.img_url,
            "dp_url": self.dp_url,
            "sponsored": self.sponsored,
            "prime": self.prime,
            "product_description": self.product_description,
            "sales_volume": self.sales_volume,
            "manufacturer": self.manufacturer,
            "scraped_at": self.scraped_at.isoformat(),
            "normalized_at": self.normalized_at.isoformat()
        }