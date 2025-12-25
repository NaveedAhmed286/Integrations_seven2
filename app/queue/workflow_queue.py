import redis
import json
import uuid
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class WorkflowQueue:
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.queue_key = "workflow:queue"
        self.processing_key = "workflow:processing"
        self.results_key = "workflow:results"
        
    def enqueue(self, workflow_type: str, data: Dict[str, Any], 
                priority: int = 0, dependencies: Optional[List[str]] = None) -> str:
        """Enqueue a workflow task"""
        task_id = str(uuid.uuid4())
        task = {
            "id": task_id,
            "type": workflow_type,
            "data": data,
            "priority": priority,
            "dependencies": dependencies or [],
            "status": "pending",
            "created_at": datetime.utcnow().isoformat(),
            "attempts": 0
        }
        
        # Store task details
        self.redis.hset(self.results_key, task_id, json.dumps(task))
        
        # Add to priority queue (score = priority, higher = more important)
        self.redis.zadd(self.queue_key, {task_id: priority})
        
        logger.info(f"Enqueued workflow task {task_id} of type {workflow_type}")
        return task_id
        
    def dequeue(self) -> Optional[Dict[str, Any]]:
        """Dequeue the highest priority task"""
        # Get task with highest priority
        task_ids = self.redis.zrange(self.queue_key, -1, -1)
        
        if not task_ids:
            return None
            
        task_id = task_ids[0].decode() if isinstance(task_ids[0], bytes) else task_ids[0]
        
        # Move to processing
        task_data = self.redis.hget(self.results_key, task_id)
        if not task_data:
            return None
            
        task = json.loads(task_data)
        task["status"] = "processing"
        task["started_at"] = datetime.utcnow().isoformat()
        
        # Update task status
        self.redis.hset(self.results_key, task_id, json.dumps(task))
        
        # Remove from queue and add to processing
        self.redis.zrem(self.queue_key, task_id)
        self.redis.sadd(self.processing_key, task_id)
        
        logger.info(f"Dequeued workflow task {task_id}")
        return task
        
    def complete(self, task_id: str, result: Dict[str, Any], 
                 error: Optional[str] = None) -> None:
        """Mark a task as completed"""
        task_data = self.redis.hget(self.results_key, task_id)
        if not task_data:
            logger.warning(f"Task {task_id} not found for completion")
            return
            
        task = json.loads(task_data)
        task["status"] = "failed" if error else "completed"
        task["completed_at"] = datetime.utcnow().isoformat()
        task["result"] = result
        task["error"] = error
        
        # Update task
        self.redis.hset(self.results_key, task_id, json.dumps(task))
        
        # Remove from processing
        self.redis.srem(self.processing_key, task_id)
        
        logger.info(f"Completed workflow task {task_id} with status {task['status']}")
        
    def get_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get task status"""
        task_data = self.redis.hget(self.results_key, task_id)
        if not task_data:
            return None
        return json.loads(task_data)
        
    def retry_failed(self, max_attempts: int = 3) -> List[str]:
        """Retry failed tasks"""
        retried = []
        
        # Get all tasks
        all_tasks = self.redis.hgetall(self.results_key)
        
        for task_id_bytes, task_data_bytes in all_tasks.items():
            task_id = task_id_bytes.decode() if isinstance(task_id_bytes, bytes) else task_id_bytes
            task_data = task_data_bytes.decode() if isinstance(task_data_bytes, bytes) else task_data_bytes
            
            task = json.loads(task_data)
            
            # Check if task failed and can be retried
            if (task["status"] == "failed" and 
                task.get("attempts", 0) < max_attempts and
                task_id not in self.redis.smembers(self.processing_key)):
                
                task["attempts"] = task.get("attempts", 0) + 1
                task["status"] = "pending"
                task.pop("completed_at", None)
                task.pop("started_at", None)
                task.pop("error", None)
                
                # Update task and re-enqueue
                self.redis.hset(self.results_key, task_id, json.dumps(task))
                self.redis.zadd(self.queue_key, {task_id: task["priority"]})
                
                retried.append(task_id)
                logger.info(f"Retried failed task {task_id}, attempt {task['attempts']}")
                
        return retried

# Create a global instance (this would normally be initialized with a Redis connection)
# For testing purposes, we'll create a placeholder
try:
    import redis
    # Try to create a Redis connection
    redis_client = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
    workflow_queue = WorkflowQueue(redis_client)
except Exception as e:
    # Create a mock instance for testing
    workflow_queue = None
    logger.warning(f"Could not initialize workflow queue: {e}")
