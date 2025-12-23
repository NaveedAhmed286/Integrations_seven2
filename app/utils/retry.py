"""
Consistent retry behavior across all external services.
Bounded retries, exponential backoff, idempotent operations.
"""
import asyncio
import functools
from typing import Callable, Any, Optional
from datetime import datetime

from app.errors import RetryExhaustedError, ExternalServiceError
from app.config import config
from app.logger import logger


def async_retry(
    max_retries: Optional[int] = None,
    backoff_factor: Optional[float] = None,
    exceptions: tuple = (Exception,)
):
    """
    Retry decorator for async functions.
    
    Args:
        max_retries: Maximum retry attempts (default from config)
        backoff_factor: Exponential backoff factor (default from config)
        exceptions: Exceptions to catch and retry
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            max_tries = max_retries or config.MAX_RETRIES
            backoff = backoff_factor or config.RETRY_BACKOFF
            
            last_exception = None
            
            for attempt in range(max_tries + 1):
                try:
                    if attempt > 0:
                        logger.info(
                            f"Retry attempt {attempt}/{max_tries} for {func.__name__}"
                        )
                    
                    return await func(*args, **kwargs)
                    
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_tries:
                        logger.error(
                            f"Max retries ({max_tries}) exhausted for {func.__name__}: {e}"
                        )
                        raise RetryExhaustedError(
                            f"Service {func.__name__} failed after {max_tries} retries: {str(e)}"
                        ) from e
                    
                    # Calculate exponential backoff
                    delay = backoff ** attempt
                    logger.warning(
                        f"Attempt {attempt + 1}/{max_tries + 1} failed for {func.__name__}. "
                        f"Retrying in {delay:.2f}s. Error: {e}"
                    )
                    
                    await asyncio.sleep(delay)
            
            # This should never be reached due to the raise above
            raise RetryExhaustedError(
                f"Service {func.__name__} failed: {str(last_exception)}"
            )
        
        return wrapper
    return decorator


def idempotent_operation(operation_id: str):
    """
    Decorator to ensure idempotent operations.
    Uses short-term memory to track completed operations.
    
    Args:
        operation_id: Unique identifier for the operation
    """
    def decorator(func: Callable):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Try to extract client_id from args or kwargs
            client_id = kwargs.get('client_id')
            if not client_id and len(args) > 0:
                # Assume first arg might be client_id in some services
                client_id = str(args[0]) if args else None
            
            if client_id:
                from app.memory.memory_manager import memory_manager
                
                # Check if operation already completed
                completed = await memory_manager.retrieve_short_term(
                    client_id, f"completed_{operation_id}"
                )
                
                if completed:
                    logger.info(
                        f"Idempotent operation {operation_id} already completed for {client_id}"
                    )
                    return completed.get("result")
            
            # Execute operation
            result = await func(*args, **kwargs)
            
            # Store completion
            if client_id:
                from app.memory.memory_manager import memory_manager
                await memory_manager.store_short_term(
                    client_id, 
                    f"completed_{operation_id}",
                    {
                        "operation_id": operation_id,
                        "completed_at": datetime.utcnow().isoformat(),
                        "result": result
                    },
                    ttl=86400  # 24 hours
                )
            
            return result
        
        return wrapper
    return decorator