"""
Consolidated ApifyService tests with proper mocking.
"""
import pytest
import os
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
import aiohttp
from dotenv import load_dotenv

# Load test environment
load_dotenv('.env.test')

from app.services.apify_service import ApifyService
from app.errors import ExternalServiceError, NetworkError


class MockConfig:
    """Mock configuration for testing."""
    APIFY_API_KEY = "test-mock-api-key-for-testing-only"
    REQUEST_TIMEOUT = 30
    MAX_RETRIES = 3


@pytest.fixture
def mock_config():
    """Mock the config module."""
    # Since app.config is an instance, we need to patch its attributes
    with patch('app.config.config.APIFY_API_KEY', MockConfig.APIFY_API_KEY), \
         patch('app.config.config.REQUEST_TIMEOUT', MockConfig.REQUEST_TIMEOUT):
        yield


def test_config_access():
    """Test that config is accessed correctly."""
    import app.config
    # app.config IS the Config instance
    print(f'Config loaded, type: {type(app.config)}')
    print(f'APIFY_API_KEY: {"SET" if app.config.APIFY_API_KEY else "NOT SET"}')
    assert hasattr(app.config, 'APIFY_API_KEY')


def test_apify_service_simple():
    """Test creating ApifyService instance with mocked config."""
    from app.services.apify_service import ApifyService
    
    # Test with mock config
    with patch('app.services.apify_service.config.APIFY_API_KEY', 'test_key_123'):
        service = ApifyService()
        print(f'ApifyService api_key: {service.api_key}')
        assert service.api_key == 'test_key_123'
        print(f'✅ ApifyService created with mock key')


@pytest.mark.asyncio
async def test_apify_service_initialization(mock_config):
    """Test ApifyService initialization."""
    service = ApifyService()
    
    # Since we patched config.APIFY_API_KEY, it should use the mock value
    assert service.api_key == MockConfig.APIFY_API_KEY
    assert service.is_available is True
    assert service.session is None


@pytest.mark.asyncio
async def test_initialize_session(mock_config):
    """Test session initialization."""
    service = ApifyService()
    
    # Mock the aiohttp.ClientSession
    with patch('aiohttp.ClientSession') as mock_session_class:
        mock_session = AsyncMock()
        mock_session_class.return_value = mock_session
        
        await service.initialize()
        
        assert service.session is not None
        mock_session_class.assert_called_once()


def create_mock_response(status=200, json_data=None, text_data=""):
    """Helper to create a properly mocked async response."""
    mock_response = AsyncMock()
    mock_response.status = status
    mock_response.text = AsyncMock(return_value=text_data)
    
    if json_data is not None:
        mock_response.json = AsyncMock(return_value=json_data)
    
    # Mock the async context manager methods
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=None)
    
    return mock_response


@pytest.mark.asyncio
async def test_scrape_amazon_search_success(mock_config):
    """Test successful Amazon search scrape."""
    service = ApifyService()
    
    # Create a mock session
    mock_session = AsyncMock()
    
    # Create mock response with test data
    mock_response = create_mock_response(
        status=200,
        json_data=[{"name": "Test Product", "price": "$19.99"}]
    )
    
    # Setup the post method to return the mock response
    mock_session.post.return_value = mock_response
    service.session = mock_session
    service.is_available = True
    
    results = await service.scrape_amazon_search("test", "com", 1)
    
    assert len(results) == 1
    assert results[0]["name"] == "Test Product"
    assert results[0]["price"] == "$19.99"
    mock_session.post.assert_called_once()


@pytest.mark.asyncio
async def test_scrape_amazon_search_api_error(mock_config):
    """Test Amazon search with API error."""
    service = ApifyService()
    
    # Create a mock session with error response
    mock_session = AsyncMock()
    mock_response = create_mock_response(
        status=500,
        text_data="Internal Server Error"
    )
    
    mock_session.post.return_value = mock_response
    service.session = mock_session
    service.is_available = True
    
    # Should raise ExternalServiceError
    with pytest.raises(ExternalServiceError) as exc_info:
        await service.scrape_amazon_search("test", "com", 1)
    
    assert "Apify API error" in str(exc_info.value)


@pytest.mark.asyncio
async def test_scrape_amazon_search_not_configured():
    """Test Amazon search when service is not configured."""
    service = ApifyService()
    
    # Simulate no API key
    with patch('app.config.config.APIFY_API_KEY', ''):
        service = ApifyService()  # Recreate with empty API key
        
        with pytest.raises(ExternalServiceError) as exc_info:
            await service.scrape_amazon_search("test", "com", 1)
        
        assert "not configured" in str(exc_info.value)


@pytest.mark.asyncio
async def test_get_actor_status_success(mock_config):
    """Test successful actor status check."""
    service = ApifyService()
    
    mock_session = AsyncMock()
    mock_response = create_mock_response(
        status=200,
        json_data={"status": "READY", "actorId": "apify~amazon-search-scraper"}
    )
    
    mock_session.get.return_value = mock_response
    service.session = mock_session
    service.is_available = True
    
    status = await service.get_actor_status("apify~amazon-search-scraper")
    
    assert status == {"status": "READY", "actorId": "apify~amazon-search-scraper"}
    mock_session.get.assert_called_once()


@pytest.mark.asyncio
async def test_get_actor_status_error(mock_config):
    """Test actor status check with error response."""
    service = ApifyService()
    
    mock_session = AsyncMock()
    mock_response = create_mock_response(
        status=404,
        text_data="Actor not found"
    )
    
    mock_session.get.return_value = mock_response
    service.session = mock_session
    service.is_available = True
    
    status = await service.get_actor_status("apify~amazon-search-scraper")
    
    # Should return error dict
    assert "status" in status
    assert status["status"] == "error"
    assert "code" in status


def test_real_apify_connection():
    """Test real connection to Apify if configured."""
    service = ApifyService()
    
    # This test checks if the service is properly configured
    # It will pass if APIFY_API_KEY is set in environment
    if service.api_key:
        assert service.is_available is True
        print("✅ Apify service is configured")
    else:
        assert service.is_available is False
        print("ℹ️ Apify service is not configured (expected for tests)")


@pytest.mark.skipif(
    not os.getenv("APIFY_API_KEY"),
    reason="No APIFY_API_KEY configured for real test"
)
def test_real_apify_if_configured():
    """Test real ApifyService if API key is configured."""
    from app.services.apify_service import ApifyService
    
    # Get real API key from environment
    api_key = os.getenv("APIFY_API_KEY", "")
    
    if not api_key:
        pytest.skip("No APIFY_API_KEY configured")
    
    # Create service with the real API key from environment
    # We need to patch the config to use the real API key
    with patch('app.config.config.APIFY_API_KEY', api_key):
        service = ApifyService()
        print(f'✅ Real ApifyService created with API key')
        
        # Note: We don't actually initialize or make real API calls in unit tests
        # This just verifies the service can be created with a real config
        assert service.api_key == api_key
        assert service.is_available is True


def test_close_method(mock_config):
    """Test close method."""
    service = ApifyService()
    
    # Create a mock session
    mock_session = AsyncMock()
    service.session = mock_session
    
    # Test close method
    asyncio.run(service.close())
    
    # Verify close was called on session
    mock_session.close.assert_called_once()
