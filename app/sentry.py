"""
Sentry initialization for centralized error tracking.
Observes reality, never controls logic.
"""
import logging
import os
import time
from typing import Dict, Any

import sentry_sdk
from sentry_sdk.integrations.asyncio import AsyncioIntegration
from sentry_sdk.integrations.logging import LoggingIntegration

from app.logger import logger

# Project name constant
PROJECT_NAME = "integrations_seven2"


def initialize_sentry():
    """Initialize Sentry SDK if DSN is configured."""
    # Get config directly from environment variables
    sentry_dsn = os.environ.get("SENTRY_DSN")
    
    if not sentry_dsn:
        logger.info("Sentry not configured, skipping initialization")
        return
    
    # Use Railway environment or default
    environment = os.environ.get("RAILWAY_ENVIRONMENT", 
                                os.environ.get("ENVIRONMENT", "production"))
    
    try:
        # Create unique release ID with timestamp
        release = os.environ.get("SENTRY_RELEASE", 
                                f"{PROJECT_NAME}@{int(time.time())}")
        
        # Get service name
        service_name = os.environ.get("RAILWAY_SERVICE_NAME", PROJECT_NAME)
        
        sentry_sdk.init(
            dsn=sentry_dsn,
            environment=environment,
            release=release,
            server_name=service_name,
            
            integrations=[
                AsyncioIntegration(),
                LoggingIntegration(level=logging.INFO, event_level=logging.ERROR)
            ],
            
            # CRITICAL: This prevents old cached errors from being sent
            before_send=_filter_historical_events,
            before_send_transaction=_filter_historical_transactions,
            
            # Sampling rates
            traces_sample_rate=0.1,
            profiles_sample_rate=0.05,
            sample_rate=1.0,  # 100% of errors
            
            # Settings
            send_default_pii=False,
            default_integrations=False,
            debug=False,
            
            # Add startup time as tag
            _experiments={
                "record_qps": True,
            }
        )
        
        # Set global tags
        sentry_sdk.set_tag("project", PROJECT_NAME)
        sentry_sdk.set_tag("service", service_name)
        sentry_sdk.set_tag("railway_service", service_name)
        sentry_sdk.set_tag("port", "8080")  # Your fixed port
        
        logger.info(f"Sentry initialized - Release: {release}, Env: {environment}")
        
    except Exception as e:
        logger.error(f"Failed to initialize Sentry: {e}")
        # Don't crash the app if Sentry fails


def _filter_historical_events(event: Dict[str, Any], hint: Dict[str, Any]) -> Dict[str, Any]:
    """Filter out ALL historical events - this is the key fix."""
    if hint and hint.get('historical'):
        logger.debug("Skipping historical Sentry event")
        return None  # COMPLETELY SKIP OLD EVENTS
    
    # Only process current events
    try:
        # Add current timestamp
        event.setdefault("tags", {})
        event["tags"]["current_startup"] = time.strftime("%Y-%m-%d %H:%M:%S")
        
        # Add port info (since you fixed it)
        event["tags"]["port"] = "8080"
        event["tags"]["railway_fixed"] = "true"
        
    except Exception:
        pass
    
    return event


def _filter_historical_transactions(event: Dict[str, Any], hint: Dict[str, Any]) -> Dict[str, Any]:
    """Filter historical transactions."""
    if hint and hint.get('historical'):
        return None
    return event


# Keep your existing capture functions but add startup check
def capture_startup_error(error: str, context: Dict[str, Any] = None):
    """Capture startup errors separately."""
    sentry_dsn = os.environ.get("SENTRY_DSN")
    if not sentry_dsn:
        return
    
    with sentry_sdk.push_scope() as scope:
        scope.set_tag("error_type", "startup")
        scope.set_tag("phase", "initialization")
        scope.set_tag("port", "8080")
        scope.set_extra("context", context or {})
        scope.set_level("error")
        
        sentry_sdk.capture_message(
            f"[{PROJECT_NAME}] Startup error: {error}",
            "error"
        )
