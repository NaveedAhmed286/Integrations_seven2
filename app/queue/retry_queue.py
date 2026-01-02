"""
Retry queue for network failure recovery.
Persistent across container restarts.
"""
import asyncio
import json
import pickle
from typing import Dict, Any, Optional, Callable
from datetime import datetime, timedelta
import uuid
import os

from app.errors import QueueError, RetryExhaustedError
from app.config import config
from app.logger import logger


class RetryQueue:
    """
    Guarantees reliability under outages.
    Handles operations that failed after max retries.
    """
    
    def __init__(self, storage_path: str = "/tmp/retry_queue"):
        self.storage_path = storage_path
        self.pending_tasks: Dict[str, Dict[str, Any]] = {}
        self.callbacks: Dict[str, Callable] = {}
        self.is_processing = False
        self._processing_task: Optional[asyncio.Task] = None  # Track processing task
        self._stop_processing = False  # Control flag for stopping
        
        # Create storage directory
        os.makedirs(storage_path, exist_ok=True)
    
    def register_operation(self, operation_name: str, callback: Callable):
        """
        Register an operation type with its handler.
        
        Args:
            operation_name: Unique operation identifier
            callback: Async function to handle the operation
        """
        self.callbacks[operation_name] = callback
        logger.debug(f"Registered retry operation: {operation_name}")
    
    async def enqueue_failed_operation(self, operation_name: str, 
                                      data: Dict[str, Any],
                                      error: str,
                                      max_attempts: int = 3,
                                      next_retry_at: Optional[datetime] = None) -> str:
        """
        Enqueue a failed operation for retry.
        
        Args:
            operation_name: Registered operation name
            data: Operation data
            error: Error that caused the failure
            max_attempts: Maximum retry attempts
            next_retry_at: When to retry (default: exponential backoff)
            
        Returns:
            Operation ID
        """
        if operation_name not in self.callbacks:
            raise QueueError(f"Unregistered operation: {operation_name}")
        
        operation_id = str(uuid.uuid4())
        
        if next_retry_at is None:
            # Exponential backoff: 5min, 15min, 45min
            attempt_count = data.get("_attempt", 0)
            backoff_minutes = 5 * (3 ** attempt_count)
            next_retry_at = datetime.utcnow() + timedelta(minutes=backoff_minutes)
        
        operation = {
            "id": operation_id,
            "name": operation_name,
            "data": data,
            "error": error,
            "attempt": data.get("_attempt", 0) + 1,
            "max_attempts": max_attempts,
            "next_retry_at": next_retry_at.isoformat(),
            "created_at": datetime.utcnow().isoformat(),
            "last_attempt": datetime.utcnow().isoformat()
        }
        
        # Store to disk for persistence
        await self._store_operation(operation)
        
        self.pending_tasks[operation_id] = operation
        logger.warning(
            f"Enqueued failed operation for retry: {operation_name} "
            f"(attempt {operation['attempt']}/{max_attempts})"
        )
        
        # Start processing if not already running
        if not self.is_processing and not self._stop_processing:
            self._processing_task = asyncio.create_task(self._process_retries())
        
        return operation_id
    
    async def _store_operation(self, operation: Dict[str, Any]):
        """Store operation to disk for persistence."""
        try:
            file_path = os.path.join(self.storage_path, f"{operation['id']}.json")
            
            # Convert datetime strings for JSON serialization
            operation_copy = operation.copy()
            
            with open(file_path, 'w') as f:
                json.dump(operation_copy, f, indent=2)
                
        except Exception as e:
            logger.error(f"Failed to store retry operation: {e}")
    
    async def _load_operations(self):
        """Load operations from disk on startup."""
        try:
            loaded_count = 0
            for filename in os.listdir(self.storage_path):
                if filename.endswith('.json') and not filename.startswith('dead_letter_'):
                    file_path = os.path.join(self.storage_path, filename)
                    
                    try:
                        with open(file_path, 'r') as f:
                            operation = json.load(f)
                        
                        # Check if still pending
                        next_retry = datetime.fromisoformat(operation['next_retry_at'])
                        if datetime.utcnow() < next_retry:
                            self.pending_tasks[operation['id']] = operation
                            loaded_count += 1
                        else:
                            # Schedule for immediate retry
                            operation['next_retry_at'] = datetime.utcnow().isoformat()
                            self.pending_tasks[operation['id']] = operation
                            loaded_count += 1
                            
                    except Exception as e:
                        logger.error(f"Failed to load retry operation file {filename}: {e}")
                        # Try to move corrupted file
                        try:
                            corrupted_path = os.path.join(self.storage_path, f"corrupted_{filename}")
                            os.rename(file_path, corrupted_path)
                        except:
                            pass
            
            if loaded_count > 0:
                logger.info(f"Loaded {loaded_count} pending retry operations from disk")
                
        except Exception as e:
            logger.error(f"Failed to load retry operations: {e}")
    
    async def _process_retries(self):
        """Process retry queue with proper error handling and exit conditions."""
        if self.is_processing:
            return
        
        self.is_processing = True
        self._stop_processing = False
        
        try:
            # Load pending operations on first run
            if not self.pending_tasks:
                await self._load_operations()
            
            logger.info(f"Retry queue processor started with {len(self.pending_tasks)} pending tasks")
            
            # FIXED: Add proper exit condition and longer sleep
            while not self._stop_processing and self.pending_tasks:
                now = datetime.utcnow()
                ready_operations = [
                    op for op in self.pending_tasks.values()
                    if datetime.fromisoformat(op['next_retry_at']) <= now
                ]
                
                if ready_operations:
                    # Process ready operations
                    for operation in ready_operations:
                        if self._stop_processing:
                            break
                        try:
                            await self._execute_retry(operation)
                        except Exception as e:
                            logger.error(f"Error processing retry operation {operation['id']}: {e}")
                            await asyncio.sleep(1)  # Brief pause on error
                else:
                    # No ready operations, sleep longer
                    await asyncio.sleep(5)  # Increased from 1 second
                
                # Brief pause between iterations
                await asyncio.sleep(0.1)
        
        except asyncio.CancelledError:
            logger.info("Retry queue processing cancelled")
        except Exception as e:
            logger.error(f"Retry queue processor crashed: {e}", exc_info=True)
        finally:
            self.is_processing = False
            self._processing_task = None
            logger.info("Retry queue processor stopped")
    
    async def _execute_retry(self, operation: Dict[str, Any]):
        """Execute a retry operation."""
        operation_id = operation['id']
        operation_name = operation['name']
        
        try:
            logger.info(
                f"Attempting retry {operation['attempt']}/{operation['max_attempts']} "
                f"for {operation_name}"
            )
            
            # Update attempt count in data
            operation['data']['_attempt'] = operation['attempt']
            operation['last_attempt'] = datetime.utcnow().isoformat()
            
            # Execute callback
            callback = self.callbacks[operation_name]
            result = await callback(operation['data'])
            
            # Success - remove from queue
            await self._remove_operation(operation_id)
            
            logger.info(f"Retry successful for {operation_name} ({operation_id})")
            
            return result
            
        except Exception as e:
            logger.error(f"Retry failed for {operation_name} ({operation_id}): {e}")
            
            # Check if we should retry again
            if operation['attempt'] >= operation['max_attempts']:
                # Max attempts exceeded - give up
                await self._remove_operation(operation_id)
                
                logger.error(
                    f"Max retry attempts ({operation['max_attempts']}) exhausted "
                    f"for {operation_name}. Giving up."
                )
                
                # Store in dead letter queue (could be sent to Sentry)
                await self._store_dead_letter(operation, str(e))
                
                raise RetryExhaustedError(
                    f"Operation {operation_name} failed after {operation['max_attempts']} attempts"
                )
            else:
                # Schedule next retry with exponential backoff
                backoff_minutes = 5 * (3 ** operation['attempt'])
                next_retry_at = datetime.utcnow() + timedelta(minutes=backoff_minutes)
                
                operation['attempt'] += 1
                operation['next_retry_at'] = next_retry_at.isoformat()
                operation['last_error'] = str(e)
                
                # Update storage
                await self._store_operation(operation)
    
    async def _remove_operation(self, operation_id: str):
        """Remove operation from queue and storage."""
        # Remove from memory
        if operation_id in self.pending_tasks:
            del self.pending_tasks[operation_id]
        
        # Remove from disk
        try:
            file_path = os.path.join(self.storage_path, f"{operation_id}.json")
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception as e:
            logger.error(f"Failed to remove retry operation file: {e}")
    
    async def _store_dead_letter(self, operation: Dict[str, Any], error: str):
        """Store failed operation in dead letter storage."""
        try:
            dead_letter_path = os.path.join(self.storage_path, "dead_letters")
            os.makedirs(dead_letter_path, exist_ok=True)
            
            dead_letter = {
                **operation,
                "final_error": error,
                "failed_at": datetime.utcnow().isoformat()
            }
            
            file_path = os.path.join(
                dead_letter_path, 
                f"{operation['id']}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
            )
            
            with open(file_path, 'w') as f:
                json.dump(dead_letter, f, indent=2)
                
            logger.error(f"Stored dead letter for {operation['name']} at {file_path}")
            
        except Exception as e:
            logger.error(f"Failed to store dead letter: {e}")
    
    async def stop_processing(self):
        """Gracefully stop retry queue processing."""
        self._stop_processing = True
        
        if self._processing_task and not self._processing_task.done():
            self._processing_task.cancel()
            try:
                await self._processing_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Retry queue processing stopped")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get queue statistics."""
        return {
            "pending_count": len(self.pending_tasks),
            "operations": list(self.callbacks.keys()),
            "storage_path": self.storage_path,
            "is_processing": self.is_processing
        }


# Global retry queue instance - BUT DON'T START IT AUTOMATICALLY
retry_queue = RetryQueue()
