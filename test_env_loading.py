import os
from dotenv import load_dotenv

# Load .env.test
load_dotenv('.env.test')

print("✅ .env.test loaded")
print(f"APIFY_API_KEY from env: {os.getenv('APIFY_API_KEY')[:8]}..." if os.getenv('APIFY_API_KEY') else "NOT SET")

# Now test config
import app.config
print(f"APIFY_API_KEY in config: {app.config.config.APIFY_API_KEY[:8]}..." if app.config.config.APIFY_API_KEY else "NOT SET")
