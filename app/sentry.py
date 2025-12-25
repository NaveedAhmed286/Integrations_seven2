"""
Sentry initialization for centralized error tracking.
Observes reality, never controls logic.
"""
import logging
from typing import Dict, Any

import sentry_sdk
from typing import Dict, Any
from sentry_sdk.integrations.asyncio import AsyncioIntegration
from sentry_sdk.integrations.logging import LoggingIntegration

from app.config import config
from app.logger import logger


def initialize_sentry():
    """Initialize Sentry SDK if DSN is configured."""
    if not config.has_sentry:
        logger.info("Sentry not configured, skipping initialization")
        return
    
    try:
        sentry_sdk.init(
            dsn=config.SENTRY_DSN,
            environment=config.ENVIRONMENT,
            integrations=[
                AsyncioIntegration(),
                LoggingIntegration(level=logging.INFO, event_level=logging.ERROR)
            ],
            traces_sample_rate=0.1,  # 10% of transactions
            profiles_sample_rate=0.05,  # 5% of profiles
            send_default_pii=False,
            
            # Custom tags for our system
            default_integrations=False,
            debug=False,
            
            before_send=lambda event, hint: _enrich_sentry_event(event, hint)
        )
        
        logger.info("Sentry initialized for error tracking")
        
    except Exception as e:
        logger.error(f"Failed to initialize Sentry: {e}")


def _enrich_sentry_event(event: Dict[str, Any], hint: Dict[str, Any]) -> Dict[str, Any]:
    """Enrich Sentry events with system context."""
    try:
        # Add system tags
        event.setdefault("tags", {})
        event["tags"]["system"] = "amazon-scraper"
        event["tags"]["environment"] = config.ENVIRONMENT
        
        # Add fingerprint for grouping
        if "exception" in event:
            exceptions = event["exception"].get("values", [])
            if exceptions:
                # Group by exception type and module
                exc = exceptions[0]
                event["fingerprint"] = [
                    "{{ default }}",
                    exc.get("type", "Unknown"),
                    exc.get("module", "unknown")
                ]
        
        # Add retry context if available
        if hint and "exc_info" in hint:
            exc = hint["exc_info"][1]
            if hasattr(exc, "_retry_context"):
                event["extra"] = event.get("extra", {})
                event["extra"]["retry_context"] = exc._retry_context
        
    except Exception as e:
        logger.error(f"Failed to enrich Sentry event: {e}")
    
    return event


def capture_retry_exhaustion(operation: str, attempts: int, error: str, context: Dict[str, Any]):
    """Capture retry exhaustion in Sentry."""
    if not config.has_sentry:
        return
    
    with sentry_sdk.push_scope() as scope:
        scope.set_tag("operation", operation)
        scope.set_tag("retry_exhausted", "true")
        scope.set_extra("attempts", attempts)
        scope.set_extra("context", context)
        scope.set_level("error")
        
        sentry_sdk.capture_message(
            f"Retry exhausted for {operation} after {attempts} attempts",
            "error"
        )


def capture_normalization_error(raw_data: Dict[str, Any], error: str):
    """Capture normalization errors in Sentry."""
    if not config.has_sentry:
        return
    
    with sentry_sdk.push_scope() as scope:
        scope.set_tag("error_type", "normalization")
        scope.set_extra("asin", raw_data.get("asin", "unknown"))
        scope.set_extra("raw_data_keys", list(raw_data.keys()))
        scope.set_level("warning")
        
        sentry_sdk.capture_message(
            f"Normalization failed for ASIN: {raw_data.get('asin', 'unknown')}",
            "warning"
        )
