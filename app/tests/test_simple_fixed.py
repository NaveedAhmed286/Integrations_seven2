import pytest

def test_config_direct():
    """Test config directly - app.config IS the Config instance"""
    import app.config
    
    print(f"Type of app.config: {type(app.config)}")
    print(f"Is Config instance: {isinstance(app.config, app.config.__class__)}")
    
    # Direct access works
    assert hasattr(app.config, 'APIFY_API_KEY')
    assert hasattr(app.config, 'RETRY_BACKOFF')
    
    print(f"APIFY_API_KEY: {'SET' if app.config.APIFY_API_KEY else 'NOT SET'}")
    print(f"RETRY_BACKOFF: {app.config.RETRY_BACKOFF}")
    print(f"MAX_RETRIES: {app.config.MAX_RETRIES}")
    
    # Test values
    assert app.config.RETRY_BACKOFF == 1.5
    assert app.config.MAX_RETRIES == 3
    assert app.config.LOG_LEVEL == "INFO"

def test_imports():
    """Test basic imports"""
    from app.queue.workflow_queue import WorkflowQueue
    from app.services.apify_service import ApifyService
    
    print("✅ All imports work correctly")
