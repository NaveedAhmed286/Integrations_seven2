import json
import hashlib
from typing import Dict, Any, Optional, List
from datetime import datetime
import asyncio

# Import external dependencies at module level for easier testing
import redis.asyncio as redis
import asyncpg

from app.errors import MemoryError
from app.config import config
from app.logger import logger


class BaseMemory:
    """Base interface for memory."""
    def __init__(self):
        self.is_available = False
    
    async def initialize(self):
        pass
    
    async def store(self, client_id: str, key: str, value: Dict[str, Any], ttl: Optional[int] = None):
        raise NotImplementedError
    
    async def retrieve(self, client_id: str, key: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError
    
    async def delete(self, client_id: str, key: str):
        raise NotImplementedError
    
    async def search(self, client_id: str, memory_type: str) -> List[Dict[str, Any]]:
        raise NotImplementedError


class ShortTermMemory(BaseMemory):
    """Redis-based short-term memory."""
    def __init__(self):
        super().__init__()
        self.redis = None
    
    async def initialize(self):
        try:
            self.redis = redis.from_url(config.REDIS_URL, decode_responses=True)
            await self.redis.ping()
            self.is_available = True
            logger.info("Short-term memory initialized")
        except Exception as e:
            logger.warning(f"Short-term memory init failed: {e}")
            self.is_available = False
    
    def _make_key(self, client_id: str, key: str) -> str:
        return f"memory:{client_id}:{key}"
    
    async def store(self, client_id: str, key: str, value: Dict[str, Any], ttl: Optional[int] = None):
        if not self.is_available:
            return
        try:
            await self.redis.setex(
                self._make_key(client_id, key), 
                ttl or config.REDIS_TTL, 
                json.dumps(value)
            )
        except Exception as e:
            logger.error(f"Failed to store short-term: {e}")
    
    async def retrieve(self, client_id: str, key: str) -> Optional[Dict[str, Any]]:
        if not self.is_available:
            return None
        try:
            data = await self.redis.get(self._make_key(client_id, key))
            return json.loads(data) if data else None
        except Exception as e:
            logger.error(f"Failed to retrieve short-term: {e}")
            return None
    
    async def delete(self, client_id: str, key: str):
        if not self.is_available:
            return
        try:
            await self.redis.delete(self._make_key(client_id, key))
        except Exception as e:
            logger.error(f"Failed to delete short-term: {e}")


class LongTermMemory(BaseMemory):
    """PostgreSQL-based long-term memory."""
    def __init__(self):
        super().__init__()
        self.pool = None
    
    async def initialize(self):
        try:
            self.pool = await asyncpg.create_pool(config.DATABASE_URL)
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
                """)
            self.is_available = True
            logger.info("Long-term memory initialized")
        except Exception as e:
            logger.warning(f"Long-term memory init failed: {e}")
            self.is_available = False
    
    async def store(self, client_id: str, key: str, value: Dict[str, Any], ttl: Optional[int] = None):
        if not self.is_available:
            return
        
        if "raw_prompt" in value or "scraper_payload" in value:
            logger.warning("Rejected raw data in long-term memory")
            return
        
        try:
            insight_hash = hashlib.md5(json.dumps(value, sort_keys=True).encode()).hexdigest()
            async with self.pool.acquire() as conn:
                existing = await conn.fetchval(
                    "SELECT id FROM memories WHERE client_id=$1 AND key=$2",
                    client_id, f"{key}_{insight_hash}"
                )
                if existing:
                    return
                
                count = await conn.fetchval(
                    "SELECT COUNT(*) FROM memories WHERE client_id=$1", client_id
                )
                if count >= config.MAX_MEMORIES_PER_CLIENT:
                    await conn.execute(
                        "DELETE FROM memories WHERE id IN (SELECT id FROM memories WHERE client_id=$1 ORDER BY created_at ASC LIMIT 10)",
                        client_id
                    )
                
                await conn.execute(
                    "INSERT INTO memories (client_id, memory_type, key, value, source_analysis) VALUES ($1,$2,$3,$4,$5) "
                    "ON CONFLICT (client_id, memory_type, key) DO UPDATE SET value=$4, updated_at=NOW()",
                    client_id, "insight", f"{key}_{insight_hash}", json.dumps(value), key
                )
        except Exception as e:
            logger.error(f"Failed to store long-term: {e}")
    
    async def retrieve(self, client_id: str, key: str) -> Optional[Dict[str, Any]]:
        if not self.is_available:
            return None
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT value FROM memories WHERE client_id=$1 AND key=$2 ORDER BY created_at DESC LIMIT 1", 
                    client_id, key
                )
                return dict(row["value"]) if row else None
        except Exception as e:
            logger.error(f"Failed to retrieve long-term: {e}")
            return None
    
    async def search(self, client_id: str, memory_type: str) -> List[Dict[str, Any]]:
        if not self.is_available:
            return []
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT key,value,created_at FROM memories WHERE client_id=$1 AND memory_type=$2 ORDER BY created_at DESC LIMIT 50", 
                    client_id, memory_type
                )
                return [{"key": r["key"], "value": dict(r["value"]), "created_at": r["created_at"].isoformat()} for r in rows]
        except Exception as e:
            logger.error(f"Failed to search long-term: {e}")
            return []


class EpisodicMemory:
    """In-memory episodic memory."""
    def __init__(self):
        self.memories: Dict[str, List[Dict[str, Any]]] = {}
    
    def store(self, client_id: str, analysis_type: str, input_data: Dict[str, Any], output_data: Dict[str, Any], insights: List[str]):
        if client_id not in self.memories:
            self.memories[client_id] = []
        
        entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "analysis_type": analysis_type,
            "input_summary": self._summarize(input_data),
            "output_summary": self._summarize(output_data),
            "insights": insights,
            "metadata": {"input_keys": list(input_data.keys()), "output_keys": list(output_data.keys())}
        }
        
        self.memories[client_id].append(entry)
        if len(self.memories[client_id]) > config.MAX_EPISODIC_MEMORIES:
            self.memories[client_id] = self.memories[client_id][-config.MAX_EPISODIC_MEMORIES:]
    
    def _summarize(self, data: Dict[str, Any]) -> Dict[str, Any]:
        summary = {}
        for k, v in data.items():
            if isinstance(v, list):
                summary[k] = f"List with {len(v)} items"
            elif isinstance(v, dict):
                summary[k] = f"Dict with {len(v)} keys"
            elif isinstance(v, str) and len(v) > 100:
                summary[k] = v[:100] + "..."
            else:
                summary[k] = v
        return summary
    
    def get_summary(self, client_id: str, max_entries: int = 10) -> List[Dict[str, Any]]:
        if client_id not in self.memories:
            return []
        
        entries = self.memories[client_id][-max_entries:]
        return [{
            "when": e["timestamp"],
            "analysis": e["analysis_type"],
            "key_insights": e["insights"][:3],
            "input_size": len(e["metadata"]["input_keys"]),
            "output_size": len(e["metadata"]["output_keys"])
        } for e in entries]


class MemoryManager:
    """Unified memory interface."""
    def __init__(self):
        self.short_term = ShortTermMemory()
        self.long_term = LongTermMemory()
        self.episodic = EpisodicMemory()
        self.initialized = False
    
    async def initialize(self):
        if self.initialized:
            return
        
        await asyncio.gather(
            self.short_term.initialize(),
            self.long_term.initialize(),
            return_exceptions=True
        )
        self.initialized = True
    
    async def store_short_term(self, client_id: str, key: str, value: Dict[str, Any], ttl: Optional[int] = None):
        await self.short_term.store(client_id, key, value, ttl)
    
    async def store_long_term(self, client_id: str, key: str, value: Dict[str, Any], source_analysis: str = ""):
        if source_analysis:
            value["_source"] = source_analysis
        await self.long_term.store(client_id, key, value)
    
    def store_episodic(self, client_id: str, analysis_type: str, input_data: Dict[str, Any], output_data: Dict[str, Any], insights: List[str]):
        self.episodic.store(client_id, analysis_type, input_data, output_data, insights)
    
    async def retrieve_short_term(self, client_id: str, key: str) -> Optional[Dict[str, Any]]:
        return await self.short_term.retrieve(client_id, key)
    
    async def retrieve_long_term(self, client_id: str, key: str) -> Optional[Dict[str, Any]]:
        return await self.long_term.retrieve(client_id, key)
    
    def get_episodic_summary(self, client_id: str, max_entries: int = 10) -> List[Dict[str, Any]]:
        return self.episodic.get_summary(client_id, max_entries)
    
    async def get_ai_context(self, client_id: str) -> Dict[str, Any]:
        context = {
            "episodic_summary": self.get_episodic_summary(client_id),
            "recent_insights": []
        }
        
        if self.long_term.is_available:
            insights = await self.long_term.search(client_id, "insight")
            context["recent_insights"] = insights[:5]
        
        return context


# global instance
memory_manager = MemoryManager()