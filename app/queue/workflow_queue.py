"""
Workflow queue for task sequencing only.
No retry logic here - that's in retry_queue.py.
"""
import asyncio
from typing import Callable, Dict, Any, Optional
from datetime import datetime
import uuid

from app.errors import QueueError
from app.logger import logger


class WorkflowQueue:
    """
    Controls task order only.
    Ensures steps run in correct sequence.
    """
    
    def __init__(self):
        self.tasks: Dict[str, Dict[str, Any]] = {}
        self.task_order: List[str] = []
        self.callbacks: Dict[str, Callable] = {}
        self.is_processing = False
    
    def register_task(self, task_name: str, callback: Callable):
        """
        Register a task type with its handler.
        
        Args:
            task_name: Unique task identifier
            callback: Async function to handle the task
        """
        self.callbacks[task_name] = callback
        logger.debug(f"Registered workflow task: {task_name}")
    
    async def enqueue(self, task_name: str, data: Dict[str, Any], 
                     dependencies: Optional[List[str]] = None) -> str:
        """
        Enqueue a task for execution.
        
        Args:
            task_name: Registered task name
            data: Task data
            dependencies: Task IDs that must complete first
            
        Returns:
            Task ID
        """
        if task_name not in self.callbacks:
            raise QueueError(f"Unregistered task: {task_name}")
        
        task_id = str(uuid.uuid4())
        
        self.tasks[task_id] = {
            "id": task_id,
            "name": task_name,
            "data": data,
            "dependencies": dependencies or [],
            "status": "pending",
            "created_at": datetime.utcnow().isoformat(),
            "started_at": None,
            "completed_at": None,
            "error": None
        }
        
        self.task_order.append(task_id)
        logger.debug(f"Enqueued workflow task: {task_name} ({task_id})")
        
        # Start processing if not already running
        if not self.is_processing:
            asyncio.create_task(self._process_queue())
        
        return task_id
    
    async def _process_queue(self):
        """Process tasks in order, respecting dependencies."""
        if self.is_processing:
            return
        
        self.is_processing = True
        
        try:
            while self.task_order:
                task_id = self.task_order[0]
                task = self.tasks.get(task_id)
                
                if not task:
                    self.task_order.pop(0)
                    continue
                
                # Check dependencies
                if task["dependencies"]:
                    deps_completed = all(
                        self.tasks.get(dep_id, {}).get("status") == "completed"
                        for dep_id in task["dependencies"]
                    )
                    
                    if not deps_completed:
                        # Wait for dependencies
                        await asyncio.sleep(0.1)
                        continue
                
                # Execute task
                await self._execute_task(task)
                
                # Remove from queue
                self.task_order.pop(0)
        
        finally:
            self.is_processing = False
    
    async def _execute_task(self, task: Dict[str, Any]):
        """Execute a single task."""
        task_id = task["id"]
        task_name = task["name"]
        
        try:
            # Update status
            task["status"] = "running"
            task["started_at"] = datetime.utcnow().isoformat()
            
            logger.info(f"Executing workflow task: {task_name} ({task_id})")
            
            # Execute callback
            callback = self.callbacks[task_name]
            result = await callback(task["data"])
            
            # Update status
            task["status"] = "completed"
            task["completed_at"] = datetime.utcnow().isoformat()
            task["result"] = result
            
            logger.info(f"Completed workflow task: {task_name} ({task_id})")
            
        except Exception as e:
            task["status"] = "failed"
            task["completed_at"] = datetime.utcnow().isoformat()
            task["error"] = str(e)
            
            logger.error(f"Failed workflow task: {task_name} ({task_id}): {e}")
            
            # Don't re-raise - workflow continues with other tasks
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get task status by ID."""
        return self.tasks.get(task_id)
    
    def clear_completed(self):
        """Clear completed tasks to free memory."""
        completed_ids = [
            task_id for task_id, task in self.tasks.items()
            if task.get("status") in ["completed", "failed"]
        ]
        
        for task_id in completed_ids:
            del self.tasks[task_id]
        
        logger.debug(f"Cleared {len(completed_ids)} completed tasks")


# Global workflow queue instance
workflow_queue = WorkflowQueue()