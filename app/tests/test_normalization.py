"""
Test normalization behavior.
Ensures raw scraper data is safely normalized.
"""
import pytest
from datetime import datetime

from app.normalizers.amazon import AmazonNormalizer
from app.errors import NormalizationError


def test_normalize_valid_product():
    """Test normalization of valid product data."""
    raw_product = {
        "asin": "B0F3PT1VBL",
        "keyword": "wireless headphones",
        "domainCode": "com",
        "searchResultPosition": 2,
        "countReview": 4,
        "productRating": 0.8,
        "imgUrl": "https://m.media-amazon.com/images/I/21x6dsVdzhL._AC_SR250,250_QL65_.jpg",
        "dpUrl": "https://aax-us-east-retail-direct.amazon.com/dp/B0F3PT1VBL",
        "sponsored": True,
        "prime": True,
        "price": None,
        "retailPrice": "",
        "productDescription": "No Product Description Found",
        "salesVolume": "",
        "manufacturer": "",
        "page": 1,
        "sortStrategy": "relevanceblender",
        "resultCount": 60000,
        "similarKeywords": [
            {"keyword": "wireless earbuds", "url": "https://www.amazon.com/s?k=wireless+earbuds"}
        ],
        "categories": [],
        "variations": [],
        "productDetails": []
    }
    
    product = AmazonNormalizer.normalize_product(raw_product)
    
    assert product.asin == "B0F3PT1VBL"
    assert product.keyword == "wireless headphones"
    assert product.domain_code == "com"
    assert product.search_result_position == 2
    assert product.count_review == 4
    assert product.product_rating == 0.8
    assert product.sponsored == True
    assert product.prime == True
    assert product.price is None
    assert product.retail_price is None
    assert product.product_description is None  # "No Product Description Found" becomes None
    assert len(product.similar_keywords) == 1
    assert product.similar_keywords[0].keyword == "wireless earbuds"
    assert product.is_valid


def test_normalize_price_strings():
    """Test normalization of price strings."""
    raw_product = {
        "asin": "TESTASIN12",
        "keyword": "test",
        "domainCode": "com",
        "searchResultPosition": 1,
        "countReview": 100,
        "productRating": 4.5,
        "imgUrl": "https://example.com/image.jpg",
        "dpUrl": "https://example.com/product",
        "sponsored": False,
        "prime": True,
        "price": "$29.99",
        "retailPrice": "$39.99",
        "productDescription": "Test",
        "salesVolume": "",
        "manufacturer": "",
        "page": 1,
        "sortStrategy": "relevanceblender",
        "resultCount": 1000,
        "similarKeywords": [],
        "categories": [],
        "variations": [],
        "productDetails": []
    }
    
    product = AmazonNormalizer.normalize_product(raw_product)
    
    assert product.price == 29.99
    assert product.retail_price == 39.99


def test_normalize_invalid_asin():
    """Test normalization with invalid ASIN."""
    raw_product = {
        "asin": "TOOSHORT",
        "keyword": "test",
        "domainCode": "com",
        "searchResultPosition": 1,
        "countReview": 100,
        "productRating": 4.5,
        "imgUrl": "https://example.com/image.jpg",
        "dpUrl": "https://example.com/product",
        "sponsored": False,
        "prime": True
    }
    
    with pytest.raises(NormalizationError):
        AmazonNormalizer.normalize_product(raw_product)


def test_normalize_batch():
    """Test batch normalization."""
    raw_products = [
        {
            "asin": "B0F3PT1VBL",
            "keyword": "wireless headphones",
            "domainCode": "com",
            "searchResultPosition": 2,
            "countReview": 4,
            "productRating": 0.8,
            "imgUrl": "https://example.com/image.jpg",
            "dpUrl": "https://example.com/product",
            "sponsored": True,
            "prime": True
        },
        {
            "asin": "INVALIDASIN",  # Invalid
            "keyword": "test",
            "domainCode": "com",
            "searchResultPosition": 1,
            "countReview": 100,
            "productRating": 4.5,
            "imgUrl": "https://example.com/image.jpg",
            "dpUrl": "https://example.com/product",
            "sponsored": False,
            "prime": True
        },
        {
            "asin": "B0F3QJLD3B",
            "keyword": "wireless headphones",
            "domainCode": "com",
            "searchResultPosition": 3,
            "countReview": 4,
            "productRating": 0.8,
            "imgUrl": "https://example.com/image2.jpg",
            "dpUrl": "https://example.com/product2",
            "sponsored": True,
            "prime": True
        }
    ]
    
    normalized = AmazonNormalizer.normalize_batch(raw_products)
    
    # Should normalize 2 valid products, skip 1 invalid
    assert len(normalized) == 2
    assert normalized[0].asin == "B0F3PT1VBL"
    assert normalized[1].asin == "B0F3QJLD3B"


def test_normalize_similar_keywords():
    """Test normalization of similar keywords."""
    raw_product = {
        "asin": "TESTASIN12",
        "keyword": "test",
        "domainCode": "com",
        "searchResultPosition": 1,
        "countReview": 100,
        "productRating": 4.5,
        "imgUrl": "https://example.com/image.jpg",
        "dpUrl": "https://example.com/product",
        "sponsored": False,
        "prime": True,
        "similarKeywords": [
            {"keyword": "test 1", "url": "https://example.com/1"},
            {"keyword": "", "url": "https://example.com/2"},  # Empty keyword
            {"url": "https://example.com/3"},  # Missing keyword
            {"keyword": "test 4", "url": ""},  # Empty URL
            {"keyword": "test 5", "url": "https://example.com/5"}
        ]
    }
    
    product = AmazonNormalizer.normalize_product(raw_product)
    
    # Should only normalize valid keywords
    assert len(product.similar_keywords) == 2
    assert product.similar_keywords[0].keyword == "test 1"
    assert product.similar_keywords[1].keyword == "test 5"


if __name__ == "__main__":
    test_normalize_valid_product()
    test_normalize_price_strings()
    test_normalize_invalid_asin()
    test_normalize_batch()
    test_normalize_similar_keywords()
    print("\nAll normalization tests passed!")