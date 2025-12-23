"""
Test application startup without network.
Ensures app starts even with no internet.
"""
import os
import sys
import asyncio
from unittest.mock import patch, MagicMock


def test_startup_without_network():
    """Test that application starts without network dependencies."""
    
    # Mock environment variables
    with patch.dict(os.environ, {
        "REDIS_URL": "redis://localhost:6379",
        "DATABASE_URL": "postgresql://user:pass@localhost/db",
        "ENVIRONMENT": "test"
    }):
        
        # Mock external services to simulate network failure
        with patch('app.services.apify_service.aiohttp.ClientSession') as mock_session, \
             patch('app.memory.memory_manager.asyncpg.create_pool') as mock_pool, \
             patch('app.services.google_service.gspread.authorize') as mock_google:
            
            # Configure mocks to raise connection errors
            mock_session.side_effect = ConnectionError("No network")
            mock_pool.side_effect = ConnectionRefusedError("DB not available")
            mock_google.side_effect = Exception("Google auth failed")
            
            # Import should still succeed
            from app.config import config
            from app.readiness import readiness_manager
            
            # Config should be loaded
            assert config.REDIS_URL == "redis://localhost:6379"
            assert config.DATABASE_URL == "postgresql://user:pass@localhost/db"
            assert config.ENVIRONMENT == "test"
            
            print("✓ Configuration loads without network")
            
            # Test async initialization
            async def test_async():
                await readiness_manager.initialize_services()
                
                # Services should report as unavailable but app should be ready
                status = readiness_manager.get_status()
                assert status["ready"] == True
                assert status["services"]["apify"] == False
                assert status["services"]["google_sheets"] == False
                assert status["services"]["memory"] == False
                
                print("✓ Application starts in degraded mode without network")
            
            # Run async test
            asyncio.run(test_async())


def test_config_fail_fast():
    """Test that missing required config fails fast."""
    
    # Clear environment variables
    with patch.dict(os.environ, {}, clear=True):
        try:
            # This should raise ConfigError
            from app.config import config
            print("✗ Should have raised ConfigError")
            sys.exit(1)
        except Exception as e:
            if "ConfigError" in str(type(e).__name__):
                print("✓ Missing config fails fast")
            else:
                print(f"✗ Wrong exception: {e}")
                sys.exit(1)


if __name__ == "__main__":
    test_startup_without_network()
    test_config_fail_fast()
    print("\nAll startup tests passed!")