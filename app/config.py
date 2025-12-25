import os

class Config:
    def __init__(self):
        # Core infrastructure
        self.REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        self.DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:pass@localhost:5432/dbname")
        self.REDIS_TTL = int(os.environ.get("REDIS_TTL", "86400"))
        self.MAX_MEMORIES_PER_CLIENT = int(os.environ.get("MAX_MEMORIES_PER_CLIENT", "100"))
        self.MAX_EPISODIC_MEMORIES = int(os.environ.get("MAX_EPISODIC_MEMORIES", "50"))
        
        # Retry configuration
        self.MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
        self.REQUEST_TIMEOUT = int(os.environ.get("REQUEST_TIMEOUT", "30"))
        self.RETRY_BACKOFF = float(os.environ.get("RETRY_BACKOFF", "1.5"))
        self.RETRY_BACKOFF_MAX = float(os.environ.get("RETRY_BACKOFF_MAX", "60"))
        
        # External services
        self.APIFY_API_KEY = os.environ.get("APIFY_API_KEY", "")
        self.DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
        
        # Google Sheets
        self.GOOGLE_SHEETS_CREDENTIALS_JSON = os.environ.get("GOOGLE_SHEETS_CREDENTIALS_JSON", "")
        self.GOOGLE_SHEETS_SPREADSHEET_ID = os.environ.get("GOOGLE_SHEETS_SPREADSHEET_ID", "")
        
        # Application settings
        self.DEBUG = os.environ.get("DEBUG", "false").lower() == "true"
        self.LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

# Create an instance
config = Config()
