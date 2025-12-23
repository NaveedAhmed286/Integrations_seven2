"""
Centralized configuration.
Fail fast ONLY on missing configuration, not network availability.
"""
import os
from typing import Optional
from app.errors import ConfigError


class Config:
    """Configuration manager with validation."""
    
    # Redis (short-term memory)
    REDIS_URL: str
    REDIS_TTL: int = 86400  # 24 hours in seconds
    
    # PostgreSQL (long-term memory)
    DATABASE_URL: str
    
    # External Services
    APIFY_API_KEY: Optional[str] = None
    GOOGLE_SHEETS_CREDENTIALS_JSON: Optional[str] = None
    DEEPSEEK_API_KEY: Optional[str] = None
    
    # Sentry
    SENTRY_DSN: Optional[str] = None
    ENVIRONMENT: str = "production"
    
    # Application
    MAX_RETRIES: int = 3
    RETRY_BACKOFF: float = 1.5
    REQUEST_TIMEOUT: int = 30
    
    # Memory limits
    MAX_EPISODIC_MEMORIES: int = 100
    MAX_MEMORIES_PER_CLIENT: int = 1000
    
    def __init__(self):
        """Validate and load configuration on import."""
        self._validate()
    
    def _validate(self):
        """Validate required environment variables."""
        # Required for memory system
        self.REDIS_URL = self._get_required("REDIS_URL")
        self.DATABASE_URL = self._get_required("DATABASE_URL")
        
        # Optional external services (system works in degraded mode)
        self.APIFY_API_KEY = os.getenv("APIFY_API_KEY")
        self.GOOGLE_SHEETS_CREDENTIALS_JSON = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON")
        self.DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
        
        # Optional observability
        self.SENTRY_DSN = os.getenv("SENTRY_DSN")
        self.ENVIRONMENT = os.getenv("ENVIRONMENT", "production")
        
        # Application settings with defaults
        self.MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
        self.RETRY_BACKOFF = float(os.getenv("RETRY_BACKOFF", "1.5"))
        self.REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
        
        # Memory limits
        self.MAX_EPISODIC_MEMORIES = int(os.getenv("MAX_EPISODIC_MEMORIES", "100"))
        self.MAX_MEMORIES_PER_CLIENT = int(os.getenv("MAX_MEMORIES_PER_CLIENT", "1000"))
    
    def _get_required(self, var_name: str) -> str:
        """Get required environment variable or raise ConfigError."""
        value = os.getenv(var_name)
        if not value:
            raise ConfigError(f"Missing required environment variable: {var_name}")
        return value
    
    @property
    def has_apify(self) -> bool:
        """Check if Apify is configured."""
        return bool(self.APIFY_API_KEY)
    
    @property
    def has_google_sheets(self) -> bool:
        """Check if Google Sheets is configured."""
        return bool(self.GOOGLE_SHEETS_CREDENTIALS_JSON)
    
    @property
    def has_ai(self) -> bool:
        """Check if AI service is configured."""
        return bool(self.DEEPSEEK_API_KEY)
    
    @property
    def has_sentry(self) -> bool:
        """Check if Sentry is configured."""
        return bool(self.SENTRY_DSN)


# Global configuration instance
config = Config()