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

from app.logger import logger  # Only import what you need

# Project name constant
PROJECT_NAME = "integrations_seven2"


def initialize_sentry():
    """Initialize Sentry SDK if DSN is configured."""
    # Get config directly from environment variables
    sentry_dsn = os.environ.get("SENTRY_DSN")
    
    # Use project-specific environment variable or default
    environment = os.environ.get("ENVIRONMENT", 
                                os.environ.get("RAILWAY_ENVIRONMENT", "production"))
    
    if not sentry_dsn:
        logger.info("Sentry not configured, skipping initialization")
        return
    
    try:
        # Generate release ID specific to your project
        release = os.environ.get("SENTRY_RELEASE")
        if not release:
            # Create unique release with project name and timestamp
            release = f"{PROJECT_NAME}@{int(time.time())}"
        
        # Get service name from Railway if available
        service_name = os.environ.get("RAILWAY_SERVICE_NAME", PROJECT_NAME)
        
        sentry_sdk.init(
            dsn=sentry_dsn,
            environment=environment,
            release=release,
            
            # Set service name for better tracking
            server_name=service_name,
            
            integrations=[
                AsyncioIntegration(),
                LoggingIntegration(level=logging.INFO, event_level=logging.ERROR)
            ],
            
            # Performance monitoring
            traces_sample_rate=0.1,  # 10% of transactions
            profiles_sample_rate=0.05,  # 5% of profiles
            
            # Error sampling (100% of errors)
            sample_rate=1.0,
            
            # Filter and enrich events
            before_send=lambda event, hint: _filter_and_enrich_event(
                event, hint, environment, service_name
            ),
            
            # Optimization
            send_default_pii=False,
            default_integrations=False,
            debug=False,
        )
        
        logger.info(f"Sentry initialized for {service_name} ({environment})")
        logger.info(f"Release: {release}")
        
    except Exception as e:
        logger.error(f"Failed to initialize Sentry: {e}")


def _filter_and_enrich_event(
    event: Dict[str, Any], 
    hint: Dict[str, Any], 
    environment: str,
    service_name: str
) -> Dict[str, Any]:
    """Filter out historical events and enrich with system context."""
    # CRITICAL: Skip historical/old cached events
    if hint and hint.get('historical'):
        logger.debug("Filtering out historical Sentry event")
        return None
    
    try:
        # Add project-specific tags
        event.setdefault("tags", {})
        event["tags"]["project"] = PROJECT_NAME
        event["tags"]["service"] = service_name
        event["tags"]["environment"] = environment
        
        # Add Railway-specific tags if available
        railway_tags = [
            "RAILWAY_SERVICE_NAME",
            "RAILWAY_DEPLOYMENT_ID", 
            "RAILWAY_ENVIRONMENT",
            "RAILWAY_SERVICE_ID"
        ]
        
        for tag in railway_tags:
            value = os.environ.get(tag)
            if value:
                event["tags"][tag.lower()] = value
        
        # Add fingerprint for better grouping
        if "exception" in event:
            exceptions = event["exception"].get("values", [])
            if exceptions:
                exc = exceptions[0]
                # Group by project + exception type + environment
                event["fingerprint"] = [
                    PROJECT_NAME,
                    exc.get("type", "Unknown"),
                    environment,
                    service_name
                ]
        
        # Add context from existing enrichment logic
        if hint and "exc_info" in hint:
            exc = hint["exc_info"][1]
            if hasattr(exc, "_retry_context"):
                event["extra"] = event.get("extra", {})
                event["extra"]["retry_context"] = exc._retry_context
                
        # Add project context
        event["extra"] = event.get("extra", {})
        event["extra"]["project_name"] = PROJECT_NAME
        event["extra"]["initialized_at"] = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
        
    except Exception as e:
        logger.error(f"Failed to enrich Sentry event: {e}")
    
    return event


# Your existing functions remain the same
def capture_retry_exhaustion(operation: str, attempts: int, error: str, context: Dict[str, Any]):
    """Capture retry exhaustion in Sentry."""
    sentry_dsn = os.environ.get("SENTRY_DSN")
    if not sentry_dsn:
        return
    
    with sentry_sdk.push_scope() as scope:
        scope.set_tag("project", PROJECT_NAME)
        scope.set_tag("operation", operation)
        scope.set_tag("retry_exhausted", "true")
        scope.set_extra("attempts", attempts)
        scope.set_extra("context", context)
        scope.set_level("error")
        
        sentry_sdk.capture_message(
            f"[{PROJECT_NAME}] Retry exhausted for {operation} after {attempts} attempts",
            "error"
        )


def capture_normalization_error(raw_data: Dict[str, Any], error: str):
    """Capture normalization errors in Sentry."""
    sentry_dsn = os.environ.get("SENTRY_DSN")
    if not sentry_dsn:
        return
    
    with sentry_sdk.push_scope() as scope:
        scope.set_tag("project", PROJECT_NAME)
        scope.set_tag("error_type", "normalization")
        scope.set_extra("asin", raw_data.get("asin", "unknown"))
        scope.set_extra("raw_data_keys", list(raw_data.keys()))
        scope.set_level("warning")
        
        sentry_sdk.capture_message(
            f"[{PROJECT_NAME}] Normalization failed for ASIN: {raw_data.get('asin', 'unknown')}",
            "warning"
        )
