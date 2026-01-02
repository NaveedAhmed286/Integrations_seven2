import os
import logging

# Set up logger
logger = logging.getLogger(__name__)

class Config:
    def __init__(self):
        # Core infrastructure
        self.REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        self.DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://user:pass@localhost:5432/dbname")
        self.REDIS_TTL = int(os.environ.get("REDIS_TTL", "86400"))
        self.MAX_MEMORIES_PER_CLIENT = int(os.environ.get("MAX_MEMORIES_PER_CLIENT", "100"))
        self.MAX_EPISODIC_MEMORIES = int(os.environ.get("MAX_EPISODIC_MEMORIES", "50"))
        
        # Fix for Railway PostgreSQL SSL requirement
        if self.DATABASE_URL and "railway" in self.DATABASE_URL and "?sslmode=" not in self.DATABASE_URL:
            self.DATABASE_URL = self.DATABASE_URL + "?sslmode=require"
            logger.info("Added SSL mode to PostgreSQL URL for Railway")
        
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
        
        # FIX: Add missing variable that your main.py references
        self.GOOGLE_SHEET_ID = self.GOOGLE_SHEETS_SPREADSHEET_ID  # Alias for backward compatibility
        
        # Application settings
        self.DEBUG = os.environ.get("DEBUG", "false").lower() == "true"
        self.LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
        
        # Sentry Configuration (for your sentry.py to work)
        self.SENTRY_DSN = os.environ.get("SENTRY_DSN", "")
        self.ENVIRONMENT = os.environ.get("ENVIRONMENT", "production")
        
        # Connection pool settings for Railway
        self.DB_POOL_SIZE = int(os.environ.get("DB_POOL_SIZE", "5"))
        self.DB_POOL_RECYCLE = int(os.environ.get("DB_POOL_RECYCLE", "300"))
        self.REDIS_CONNECT_TIMEOUT = int(os.environ.get("REDIS_CONNECT_TIMEOUT", "5"))
        
        logger.info(f"Config initialized. Environment: {self.ENVIRONMENT}")
    
    # Property to check if Sentry is configured
    @property
    def has_sentry(self):
        return bool(self.SENTRY_DSN)

# Create an instance
config = Config()
