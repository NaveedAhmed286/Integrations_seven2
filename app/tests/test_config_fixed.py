import pytest
import os

def test_config_structure():
    """Test config structure - app.config IS the Config instance"""
    import app.config
    
    print(f"Type of app.config: {type(app.config)}")
    
    # app.config should be the Config instance
    assert hasattr(app.config, 'APIFY_API_KEY')
    assert hasattr(app.config, 'RETRY_BACKOFF')
    
    print(f"✅ Config instance check passed")
    print(f"APIFY_API_KEY: {'SET' if app.config.APIFY_API_KEY else 'NOT SET'}")
    print(f"RETRY_BACKOFF: {app.config.RETRY_BACKOFF}")

def test_config_environment():
    """Test that environment variables are loaded"""
    import app.config
    
    # Test default values
    print(f"DEBUG mode: {app.config.DEBUG}")
    print(f"LOG_LEVEL: {app.config.LOG_LEVEL}")
    
    assert app.config.DEBUG == False  # Default should be False
    assert app.config.LOG_LEVEL == "INFO"  # Default should be INFO
    
    # Test that RETRY_BACKOFF has the correct default
    assert app.config.RETRY_BACKOFF == 1.5
    
    print("✅ Config environment test passed")
