"""
Production-grade memory system with three distinct memory types:
1. Short-term (Redis): fast, expiring, session-like
2. Long-term (PostgreSQL): persistent, structured, curated
3. Episodic: analysis history (summarized before AI consumption)

Rules:
- Memory is read-only for AI models
- Never store raw AI prompts or full scraper payloads
- Prevent duplicates, limit growth, support expiration
"""
import json
import hashlib
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import asyncio

from app.errors import MemoryError
from app.config import config
from app.logger import logger


class BaseMemory:
    """Base memory interface."""
    
    def __init__(self):
        self.is_available = False
    
    async def initialize(self):
        """Initialize connection (called after startup)."""
        pass
    
    async def store(self, client_id: str, key: str, value: Dict[str, Any], ttl: Optional[int] = None):
        """Store a memory entry."""
        raise NotImplementedError
    
    async def retrieve(self, client_id: str, key: str) -> Optional[Dict[str, Any]]:
        """Retrieve a memory entry."""
        raise NotImplementedError
    
    async def delete(self, client_id: str, key: str):
        """Delete a memory entry."""
        raise NotImplementedError
    
    async def search(self, client_id: str, memory_type: str) -> List[Dict[str, Any]]:
        """Search memories by type."""
        raise NotImplementedError


class ShortTermMemory(BaseMemory):
    """Redis-based short-term memory with TTL."""
    
    def __init__(self):
        super().__init__()
        self.redis = None
    
    async def initialize(self):
        """Initialize Redis connection (non-blocking)."""
        try:
            import redis.asyncio as redis
            
            self.redis = redis.from_url(
                config.REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            await self.redis.ping()
            self.is_available = True
            logger.info("Short-term memory (Redis) initialized")
            
        except Exception as e:
            logger.warning(f"Redis initialization failed: {e}. System will work in degraded mode.")
            self.is_available = False
    
    def _make_key(self, client_id: str, key: str) -> str:
        """Create namespaced Redis key."""
        return f"memory:{client_id}:{key}"
    
    async def store(self, client_id: str, key: str, value: Dict[str, Any], ttl: Optional[int] = None):
        """Store with TTL (default 24h)."""
        if not self.is_available:
            return
        
        try:
            redis_key = self._make_key(client_id, key)
            await self.redis.setex(
                redis_key,
                ttl or config.REDIS_TTL,
                json.dumps(value)
            )
            logger.debug(f"Stored in short-term memory: {redis_key}")
        except Exception as e:
            logger.error(f"Failed to store in short-term memory: {e}")
    
    async def retrieve(self, client_id: str, key: str) -> Optional[Dict[str, Any]]:
        """Retrieve from Redis."""
        if not self.is_available:
            return None
        
        try:
            redis_key = self._make_key(client_id, key)
            data = await self.redis.get(redis_key)
            return json.loads(data) if data else None
        except Exception as e:
            logger.error(f"Failed to retrieve from short-term memory: {e}")
            return None
    
    async def delete(self, client_id: str, key: str):
        """Delete from Redis."""
        if not self.is_available:
            return
        
        try:
            redis_key = self._make_key(client_id, key)
            await self.redis.delete(redis_key)
        except Exception as e:
            logger.error(f"Failed to delete from short-term memory: {e}")


class LongTermMemory(BaseMemory):
    """PostgreSQL-based long-term memory for curated insights."""
    
    def __init__(self):
        super().__init__()
        self.pool = None
    
    async def initialize(self):
        """Initialize PostgreSQL connection (non-blocking)."""
        try:
            import asyncpg
            
            self.pool = await asyncpg.create_pool(
                config.DATABASE_URL,
                min_size=1,
                max_size=5,
                command_timeout=5
            )
            
            # Create memories table if not exists
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS memories (
                        id SERIAL PRIMARY KEY,
                        client_id TEXT NOT NULL,
                        memory_type TEXT NOT NULL,
                        key TEXT NOT NULL,
                        value JSONB NOT NULL,
                        source_analysis TEXT,
                        created_at TIMESTAMP DEFAULT NOW(),
                        updated_at TIMESTAMP DEFAULT NOW(),
                        UNIQUE(client_id, memory_type, key)
                    );
                    CREATE INDEX IF NOT EXISTS idx_memories_client_type 
                    ON memories(client_id, memory_type);
                """)
            
            self.is_available = True
            logger.info("Long-term memory (PostgreSQL) initialized")
            
        except Exception as e:
            logger.warning(f"PostgreSQL initialization failed: {e}. System will work in degraded mode.")
            self.is_available = False
    
    async def store(self, client_id: str, key: str, value: Dict[str, Any], ttl: Optional[int] = None):
        """
        Store curated insight in long-term memory.
        
        Rules:
        - Never store raw AI prompts or full scraper payloads
        - Only high-value, curated insights
        - Prevent duplicates
        """
        if not self.is_available:
            return
        
        try:
            # Ensure we're only storing insights, not raw data
            if "raw_prompt" in value or "scraper_payload" in value:
                logger.warning("Attempted to store raw data in long-term memory - rejected")
                return
            
            # Create insight hash for duplicate prevention
            insight_hash = hashlib.md5(
                json.dumps(value, sort_keys=True).encode()
            ).hexdigest()
            
            async with self.pool.acquire() as conn:
                # Check for duplicates
                existing = await conn.fetchval("""
                    SELECT id FROM memories 
                    WHERE client_id = $1 AND key = $2
                """, client_id, f"{key}_{insight_hash}")
                
                if existing:
                    logger.debug(f"Duplicate insight skipped: {key}")
                    return
                
                # Enforce memory limit per client
                count = await conn.fetchval("""
                    SELECT COUNT(*) FROM memories WHERE client_id = $1
                """, client_id)
                
                if count >= config.MAX_MEMORIES_PER_CLIENT:
                    # Remove oldest memories
                    await conn.execute("""
                        DELETE FROM memories 
                        WHERE id IN (
                            SELECT id FROM memories 
                            WHERE client_id = $1 
                            ORDER BY created_at ASC 
                            LIMIT 10
                        )
                    """, client_id)
                
                # Store the insight
                await conn.execute("""
                    INSERT INTO memories (client_id, memory_type, key, value, source_analysis)
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (client_id, memory_type, key) 
                    DO UPDATE SET value = $4, updated_at = NOW()
                """, client_id, "insight", f"{key}_{insight_hash}", json.dumps(value), key)
                
                logger.debug(f"Stored in long-term memory: {key}")
                
        except Exception as e:
            logger.error(f"Failed to store in long-term memory: {e}")
    
    async def retrieve(self, client_id: str, key: str) -> Optional[Dict[str, Any]]:
        """Retrieve from long-term memory."""
        if not self.is_available:
            return None
        
        try:
            async with self.pool.acquire() as conn:
                result = await conn.fetchrow("""
                    SELECT value FROM memories 
                    WHERE client_id = $1 AND key = $2
                    ORDER BY created_at DESC LIMIT 1
                """, client_id, key)
                
                return dict(result["value"]) if result else None
        except Exception as e:
            logger.error(f"Failed to retrieve from long-term memory: {e}")
            return None
    
    async def search(self, client_id: str, memory_type: str) -> List[Dict[str, Any]]:
        """Search memories by type."""
        if not self.is_available:
            return []
        
        try:
            async with self.pool.acquire() as conn:
                results = await conn.fetch("""
                    SELECT key, value, created_at 
                    FROM memories 
                    WHERE client_id = $1 AND memory_type = $2
                    ORDER BY created_at DESC
                    LIMIT 50
                """, client_id, memory_type)
                
                return [
                    {
                        "key": r["key"],
                        "value": dict(r["value"]),
                        "created_at": r["created_at"].isoformat()
                    }
                    for r in results
                ]
        except Exception as e:
            logger.error(f"Failed to search long-term memory: {e}")
            return []


class EpisodicMemory:
    """In-memory episodic memory (analysis history)."""
    
    def __init__(self):
        self.memories: Dict[str, List[Dict[str, Any]]] = {}
    
    def store(self, client_id: str, analysis_type: str, 
              input_data: Dict[str, Any], output_data: Dict[str, Any],
              insights: List[str]):
        """
        Store episodic memory entry.
        
        Args:
            client_id: Client identifier
            analysis_type: Type of analysis performed
            input_data: Input data (summarized)
            output_data: Output data (summarized)
            insights: List of insights generated
        """
        if client_id not in self.memories:
            self.memories[client_id] = []
        
        entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "analysis_type": analysis_type,
            "input_summary": self._summarize_input(input_data),
            "output_summary": self._summarize_output(output_data),
            "insights": insights,
            "metadata": {
                "input_keys": list(input_data.keys()),
                "output_keys": list(output_data.keys())
            }
        }
        
        self.memories[client_id].append(entry)
        
        # Enforce memory bounds (latest N entries only)
        if len(self.memories[client_id]) > config.MAX_EPISODIC_MEMORIES:
            self.memories[client_id] = self.memories[client_id][-config.MAX_EPISODIC_MEMORIES:]
    
    def _summarize_input(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """Summarize input data for episodic memory."""
        summary = {}
        
        for key, value in input_data.items():
            if isinstance(value, list):
                summary[key] = f"List with {len(value)} items"
            elif isinstance(value, dict):
                summary[key] = f"Dict with {len(value)} keys"
            elif isinstance(value, str) and len(value) > 100:
                summary[key] = value[:100] + "..."
            else:
                summary[key] = value
        
        return summary
    
    def _summarize_output(self, output_data: Dict[str, Any]) -> Dict[str, Any]:
        """Summarize output data for episodic memory."""
        return self._summarize_input(output_data)
    
    def get_summary(self, client_id: str, max_entries: int = 10) -> List[Dict[str, Any]]:
        """
        Get summarized episodic memory for AI consumption.
        
        Args:
            client_id: Client identifier
            max_entries: Maximum number of entries to return
            
        Returns:
            Summarized memory entries
        """
        if client_id not in self.memories:
            return []
        
        entries = self.memories[client_id][-max_entries:]
        
        # Further summarize for AI context
        summarized = []
        for entry in entries:
            summarized.append({
                "when": entry["timestamp"],
                "analysis": entry["analysis_type"],
                "key_insights": entry["insights"][:3],  # Top 3 insights
                "input_size": len(entry["metadata"]["input_keys"]),
                "output_size": len(entry["metadata"]["output_keys"])
            })
        
        return summarized


class MemoryManager:
    """
    Unified memory manager interface.
    Abstracts Redis and PostgreSQL, handles partial failures gracefully.
    """
    
    def __init__(self):
        self.short_term = ShortTermMemory()
        self.long_term = LongTermMemory()
        self.episodic = EpisodicMemory()
        self.initialized = False
    
    async def initialize(self):
        """Initialize all memory systems (non-blocking)."""
        if self.initialized:
            return
        
        # Initialize in parallel
        await asyncio.gather(
            self.short_term.initialize(),
            self.long_term.initialize(),
            return_exceptions=True  # Don't crash if one fails
        )
        
        self.initialized = True
        logger.info("Memory manager initialized")
    
    async def store_short_term(self, client_id: str, key: str, 
                               value: Dict[str, Any], ttl: Optional[int] = None):
        """Store in short-term memory (Redis)."""
        await self.short_term.store(client_id, key, value, ttl)
    
    async def store_long_term(self, client_id: str, key: str, 
                              value: Dict[str, Any], source_analysis: str = ""):
        """
        Store curated insight in long-term memory.
        
        Args:
            client_id: Client identifier
            key: Memory key
            value: Curated insight (no raw data)
            source_analysis: Source analysis identifier
        """
        if source_analysis:
            value["_source"] = source_analysis
        
        await self.long_term.store(client_id, key, value)
    
    def store_episodic(self, client_id: str, analysis_type: str,
                       input_data: Dict[str, Any], output_data: Dict[str, Any],
                       insights: List[str]):
        """Store episodic memory entry."""
        self.episodic.store(client_id, analysis_type, input_data, output_data, insights)
    
    async def retrieve_short_term(self, client_id: str, key: str) -> Optional[Dict[str, Any]]:
        """Retrieve from short-term memory."""
        return await self.short_term.retrieve(client_id, key)
    
    async def retrieve_long_term(self, client_id: str, key: str) -> Optional[Dict[str, Any]]:
        """Retrieve from long-term memory."""
        return await self.long_term.retrieve(client_id, key)
    
    def get_episodic_summary(self, client_id: str, max_entries: int = 10) -> List[Dict[str, Any]]:
        """Get summarized episodic memory for AI consumption."""
        return self.episodic.get_summary(client_id, max_entries)
    
    async def get_ai_context(self, client_id: str) -> Dict[str, Any]:
        """
        Get memory context for AI models.
        
        Rules:
        - AI consumes summarized context only
        - No raw data or full prompts
        - Memory is read-only for AI
        """
        context = {
            "episodic_summary": self.get_episodic_summary(client_id),
            "recent_insights": []
        }
        
        # Get recent insights from long-term memory
        if self.long_term.is_available:
            insights = await self.long_term.search(client_id, "insight")
            context["recent_insights"] = insights[:5]  # Last 5 insights
        
        return context


# Global memory manager instance
memory_manager = MemoryManager()