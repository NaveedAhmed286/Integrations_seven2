"""
Test that all modules import correctly.
Catches circular imports early.
"""
import sys
import importlib


def test_imports():
    """Test importing all application modules."""
    modules = [
        "app.config",
        "app.errors",
        "app.logger",
        "app.readiness",
        "app.sentry",
        "app.models.product",
        "app.normalizers.amazon",
        "app.services.apify_service",
        "app.services.google_service",
        "app.memory_manager",
        "app.queue.workflow_queue",
        "app.queue.retry_queue",
        "app.utils.retry",
        "app.agents.amazon_agent",
        "app.main"
    ]
    
    for module_name in modules:
        try:
            importlib.import_module(module_name)
            print(f"✓ {module_name} imports correctly")
        except Exception as e:
            print(f"✗ {module_name} failed to import: {e}")
            sys.exit(1)


if __name__ == "__main__":
    test_imports()
    print("\nAll imports successful!")