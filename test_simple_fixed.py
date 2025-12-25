def test_basic():
    assert 1 + 1 == 2

def test_import():
    import app.config
    # Now it should work: app.config.config (instance) not app.config.Config (class)
    assert hasattr(app.config.config, 'APIFY_API_KEY')
    key = app.config.config.APIFY_API_KEY
    print(f'✅ Config has APIFY_API_KEY: {\"SET (\" + key[:8] + \"...)\" if key else \"NOT SET\"}')
