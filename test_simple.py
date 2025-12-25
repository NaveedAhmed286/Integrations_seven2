def test_basic():
    assert 1 + 1 == 2

def test_import():
    import app.config
    assert hasattr(app.config.config, 'APIFY_API_KEY')
    print(f"✅ Config has APIFY_API_KEY: {app.config.config.APIFY_API_KEY[:8]}..." if app.config.config.APIFY_API_KEY else "✅ Config has APIFY_API_KEY: NOT SET")
