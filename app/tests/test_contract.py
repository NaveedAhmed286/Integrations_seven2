"""
Test data contract stability.
Ensures internal model doesn't break.
"""
import pytest
from datetime import datetime

from app.models.product import AmazonProduct, SimilarKeyword


def test_product_contract():
    """Test basic product contract."""
    product = AmazonProduct(
        asin="TESTASIN12",
        keyword="test product",
        domain_code="com",
        search_result_position=1,
        count_review=100,
        product_rating=4.5,
        img_url="https://example.com/image.jpg",
        dp_url="https://example.com/product",
        sponsored=False,
        prime=True,
        price=29.99,
        product_description="Test description"
    )
    
    assert product.is_valid
    assert product.has_price
    assert product.asin == "TESTASIN12"
    assert product.keyword == "test product"
    assert product.product_rating == 4.5
    
    # Test to_dict serialization
    product_dict = product.to_dict()
    assert "asin" in product_dict
    assert "keyword" in product_dict
    assert "product_rating" in product_dict
    assert "scraped_at" in product_dict
    assert "normalized_at" in product_dict


def test_similar_keyword_contract():
    """Test similar keyword contract."""
    keyword = SimilarKeyword(
        keyword="similar product",
        url="https://example.com/similar"
    )
    
    assert keyword.keyword == "similar product"
    assert keyword.url == "https://example.com/similar"


def test_product_validation():
    """Test product validation rules."""
    # Invalid ASIN length
    product = AmazonProduct(
        asin="SHORT",
        keyword="test",
        domain_code="com",
        search_result_position=1,
        count_review=100,
        product_rating=4.5,
        img_url="https://example.com/image.jpg",
        dp_url="https://example.com/product",
        sponsored=False,
        prime=True
    )
    
    assert not product.is_valid
    
    # Invalid rating
    product = AmazonProduct(
        asin="TESTASIN12",
        keyword="test",
        domain_code="com",
        search_result_position=1,
        count_review=100,
        product_rating=6.0,  # > 5.0
        img_url="https://example.com/image.jpg",
        dp_url="https://example.com/product",
        sponsored=False,
        prime=True
    )
    
    assert not product.is_valid
    
    # Negative review count
    product = AmazonProduct(
        asin="TESTASIN12",
        keyword="test",
        domain_code="com",
        search_result_position=1,
        count_review=-10,  # Negative
        product_rating=4.5,
        img_url="https://example.com/image.jpg",
        dp_url="https://example.com/product",
        sponsored=False,
        prime=True
    )
    
    assert not product.is_valid


if __name__ == "__main__":
    test_product_contract()
    test_similar_keyword_contract()
    test_product_validation()
    print("\nAll contract tests passed!")