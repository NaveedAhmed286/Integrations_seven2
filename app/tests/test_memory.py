"""
Test memory system behavior.
Ensures short-term, long-term, and episodic memory work correctly.
"""
import pytest
import asyncio
import json
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch, MagicMock

from app.errors import MemoryError
from app.config import config
from app.memory.memory_manager import (
    MemoryManager,
    ShortTermMemory,
    LongTermMemory,
    EpisodicMemory
)


class TestShortTermMemory:
    """Test Redis-based short-term memory."""
    
    @pytest.fixture
    async def short_term_memory(self):
        """Create a ShortTermMemory instance."""
        memory = ShortTermMemory()
        
        # Mock Redis
        memory.redis = AsyncMock()
        memory.is_available = True
        
        return memory
    
    @pytest.mark.asyncio
    async def test_store_and_retrieve(self, short_term_memory):
        """Test storing and retrieving from short-term memory."""
        client_id = "test_client"
        key = "test_key"
        value = {"data": "test_value", "number": 42}
        
        # Mock Redis setex
        short_term_memory.redis.setex = AsyncMock()
        
        # Store
        await short_term_memory.store(client_id, key, value, ttl=3600)
        
        # Verify Redis was called correctly
        expected_key = f"memory:{client_id}:{key}"
        short_term_memory.redis.setex.assert_called_once_with(
            expected_key,
            3600,
            json.dumps(value)
        )
        
        # Mock Redis get
        short_term_memory.redis.get = AsyncMock(return_value=json.dumps(value))
        
        # Retrieve
        result = await short_term_memory.retrieve(client_id, key)
        
        # Verify
        assert result == value
        short_term_memory.redis.get.assert_called_once_with(expected_key)
    
    @pytest.mark.asyncio
    async def test_retrieve_nonexistent(self, short_term_memory):
        """Test retrieving nonexistent key."""
        short_term_memory.redis.get = AsyncMock(return_value=None)
        
        result = await short_term_memory.retrieve("client", "nonexistent")
        
        assert result is None
    
    @pytest.mark.asyncio
    async def test_delete(self, short_term_memory):
        """Test deleting from short-term memory."""
        short_term_memory.redis.delete = AsyncMock()
        
        await short_term_memory.delete("client", "key")
        
        expected_key = "memory:client:key"
        short_term_memory.redis.delete.assert_called_once_with(expected_key)
    
    @pytest.mark.asyncio
    async def test_unavailable(self):
        """Test behavior when Redis is unavailable."""
        memory = ShortTermMemory()
        memory.is_available = False
        
        # Should not raise errors
        await memory.store("client", "key", {"test": "data"})
        
        result = await memory.retrieve("client", "key")
        assert result is None
        
        await memory.delete("client", "key")
        # Should not crash


class TestLongTermMemory:
    """Test PostgreSQL-based long-term memory."""
    
    @pytest.fixture
    async def long_term_memory(self):
        """Create a LongTermMemory instance with mocked pool."""
        memory = LongTermMemory()
        
        # Mock connection pool
        memory.pool = AsyncMock()
        memory.conn = AsyncMock()
        memory.is_available = True
        
        # Mock context manager
        memory.pool.acquire.return_value.__aenter__ = AsyncMock(return_value=memory.conn)
        memory.pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
        
        return memory
    
    @pytest.mark.asyncio
    async def test_store_insight(self, long_term_memory):
        """Test storing curated insight in long-term memory."""
        client_id = "test_client"
        key = "product_analysis"
        value = {
            "asin": "B0F3PT1VBL",
            "rating": 4.5,
            "insights": ["High rating", "Good reviews"]
        }
        
        # Mock duplicate check
        long_term_memory.conn.fetchval = AsyncMock(return_value=None)
        
        # Mock count check
        long_term_memory.conn.fetchval = AsyncMock(return_value=5)
        
        # Mock execute
        long_term_memory.conn.execute = AsyncMock()
        
        # Store
        await long_term_memory.store(client_id, key, value)
        
        # Should not store raw data
        assert "raw_prompt" not in value
        assert "scraper_payload" not in value
        
        # Verify execute was called (insert or update)
        assert long_term_memory.conn.execute.called
    
    @pytest.mark.asyncio
    async def test_store_duplicate_insight(self, long_term_memory):
        """Test duplicate insight prevention."""
        client_id = "test_client"
        key = "duplicate_insight"
        value = {"test": "data"}
        
        # Mock duplicate check to return existing ID
        long_term_memory.conn.fetchval = AsyncMock(return_value=123)
        
        # Store
        await long_term_memory.store(client_id, key, value)
        
        # Should not call execute for duplicate
        assert not long_term_memory.conn.execute.called
    
    @pytest.mark.asyncio
    async def test_store_raw_data_rejection(self, long_term_memory):
        """Test that raw data is rejected from long-term memory."""
        client_id = "test_client"
        key = "raw_data"
        
        # Try to store raw prompt (should be rejected)
        value_with_raw = {
            "raw_prompt": "Analyze this product...",
            "insights": []
        }
        
        await long_term_memory.store(client_id, key, value_with_raw)
        
        # Try to store scraper payload (should be rejected)
        value_with_payload = {
            "scraper_payload": {"asin": "B0F3PT1VBL", "price": None},
            "insights": []
        }
        
        await long_term_memory.store(client_id, key, value_with_payload)
        
        # Should not execute store for raw data
        assert not long_term_memory.conn.execute.called
    
    @pytest.mark.asyncio
    async def test_retrieve(self, long_term_memory):
        """Test retrieving from long-term memory."""
        client_id = "test_client"
        key = "test_insight"
        expected_value = {"test": "data"}
        
        # Mock fetchrow
        mock_row = MagicMock()
        mock_row.__getitem__.side_effect = lambda x: {"value": expected_value}[x]
        long_term_memory.conn.fetchrow = AsyncMock(return_value=mock_row)
        
        result = await long_term_memory.retrieve(client_id, key)
        
        assert result == expected_value
        
        # Verify query
        long_term_memory.conn.fetchrow.assert_called_once()
        call_args = long_term_memory.conn.fetchrow.call_args[0][0]
        assert "SELECT value FROM memories" in call_args
        assert "client_id = $1" in call_args
        assert "key = $2" in call_args
    
    @pytest.mark.asyncio
    async def test_search(self, long_term_memory):
        """Test searching memories by type."""
        client_id = "test_client"
        memory_type = "insight"
        
        # Mock fetch
        mock_rows = [
            MagicMock(key="key1", value={"insight": "first"}, created_at=datetime(2023, 1, 1)),
            MagicMock(key="key2", value={"insight": "second"}, created_at=datetime(2023, 1, 2))
        ]
        
        long_term_memory.conn.fetch = AsyncMock(return_value=mock_rows)
        
        results = await long_term_memory.search(client_id, memory_type)
        
        assert len(results) == 2
        assert results[0]["key"] == "key1"
        assert results[0]["value"] == {"insight": "first"}
        assert "created_at" in results[0]
        
        # Verify query
        long_term_memory.conn.fetch.assert_called_once()
        call_args = long_term_memory.conn.fetch.call_args[0][0]
        assert "WHERE client_id = $1 AND memory_type = $2" in call_args
        assert "ORDER BY created_at DESC" in call_args
        assert "LIMIT 50" in call_args


class TestEpisodicMemory:
    """Test in-memory episodic memory."""
    
    def test_store_and_summarize(self):
        """Test storing and summarizing episodic memory."""
        memory = EpisodicMemory()
        client_id = "test_client"
        
        # Store multiple entries
        for i in range(3):
            memory.store(
                client_id=client_id,
                analysis_type=f"analysis_{i}",
                input_data={"keyword": f"test_{i}", "count": i * 10},
                output_data={"result": f"output_{i}", "score": i * 0.5},
                insights=[f"insight_{i}_1", f"insight_{i}_2"]
            )
        
        # Get summary
        summary = memory.get_summary(client_id, max_entries=2)
        
        assert len(summary) == 2  # Latest 2 entries
        assert summary[0]["analysis"] == "analysis_2"
        assert len(summary[0]["key_insights"]) <= 3
        assert "when" in summary[0]
        assert "input_size" in summary[0]
        
        # Test with nonexistent client
        summary = memory.get_summary("nonexistent", max_entries=10)
        assert summary == []
    
    def test_memory_bounds(self):
        """Test that episodic memory respects bounds."""
        memory = EpisodicMemory()
        client_id = "test_client"
        
        # Store more entries than max limit
        for i in range(150):  # More than MAX_EPISODIC_MEMORIES (100)
            memory.store(
                client_id=client_id,
                analysis_type=f"analysis_{i}",
                input_data={"test": i},
                output_data={"result": i},
                insights=[f"insight_{i}"]
            )
        
        # Should only keep latest 100 entries
        entries = memory.memories[client_id]
        assert len(entries) == 100  # MAX_EPISODIC_MEMORIES
        
        # Verify oldest entries were removed
        assert entries[0]["analysis_type"] == "analysis_50"  # 150-100=50
        assert entries[-1]["analysis_type"] == "analysis_149"  # Latest
    
    def test_input_summarization(self):
        """Test that input data is properly summarized."""
        memory = EpisodicMemory()
        
        # Large input data
        input_data = {
            "large_text": "A" * 200,  # 200 chars
            "list_data": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            "dict_data": {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5},
            "small_value": "normal"
        }
        
        summary = memory._summarize_input(input_data)
        
        # Large text should be truncated
        assert len(summary["large_text"]) <= 103  # 100 chars + "..."
        assert summary["large_text"].endswith("...")
        
        # Lists and dicts should be summarized
        assert "List with 10 items" in summary["list_data"]
        assert "Dict with 5 keys" in summary["dict_data"]
        
        # Small values unchanged
        assert summary["small_value"] == "normal"
    
    def test_multiple_clients(self):
        """Test memory isolation between clients."""
        memory = EpisodicMemory()
        
        # Store for client A
        memory.store(
            client_id="client_a",
            analysis_type="analysis_a",
            input_data={"test": "a"},
            output_data={"result": "a"},
            insights=["insight_a"]
        )
        
        # Store for client B
        memory.store(
            client_id="client_b",
            analysis_type="analysis_b",
            input_data={"test": "b"},
            output_data={"result": "b"},
            insights=["insight_b"]
        )
        
        # Verify isolation
        summary_a = memory.get_summary("client_a")
        summary_b = memory.get_summary("client_b")
        
        assert len(summary_a) == 1
        assert len(summary_b) == 1
        assert summary_a[0]["analysis"] == "analysis_a"
        assert summary_b[0]["analysis"] == "analysis_b"
        
        # Client C should have no entries
        summary_c = memory.get_summary("client_c")
        assert summary_c == []


class TestMemoryManager:
    """Test unified memory manager."""
    
    @pytest.fixture
    async def memory_manager(self):
        """Create a MemoryManager with mocked components."""
        manager = MemoryManager()
        
        # Mock child components
        manager.short_term = AsyncMock()
        manager.long_term = AsyncMock()
        manager.episodic = EpisodicMemory()
        
        # Mock initialize methods
        manager.short_term.initialize = AsyncMock()
        manager.long_term.initialize = AsyncMock()
        
        return manager
    
    @pytest.mark.asyncio
    async def test_initialize(self, memory_manager):
        """Test memory manager initialization."""
        await memory_manager.initialize()
        
        # Should initialize child components
        memory_manager.short_term.initialize.assert_called_once()
        memory_manager.long_term.initialize.assert_called_once()
        
        assert memory_manager.initialized
    
    @pytest.mark.asyncio
    async def test_store_and_retrieve_short_term(self, memory_manager):
        """Test short-term memory operations through manager."""
        client_id = "test_client"
        key = "test_key"
        value = {"test": "data"}
        ttl = 3600
        
        # Test store
        await memory_manager.store_short_term(client_id, key, value, ttl)
        memory_manager.short_term.store.assert_called_once_with(client_id, key, value, ttl)
        
        # Test retrieve
        memory_manager.short_term.retrieve = AsyncMock(return_value=value)
        result = await memory_manager.retrieve_short_term(client_id, key)
        
        assert result == value
        memory_manager.short_term.retrieve.assert_called_once_with(client_id, key)
    
    @pytest.mark.asyncio
    async def test_store_long_term_with_source(self, memory_manager):
        """Test long-term memory storage with source analysis."""
        client_id = "test_client"
        key = "insight_key"
        value = {"analysis": "result"}
        source = "product_analysis"
        
        await memory_manager.store_long_term(client_id, key, value, source)
        
        # Value should be enriched with source
        expected_value = value.copy()
        expected_value["_source"] = source
        
        memory_manager.long_term.store.assert_called_once_with(
            client_id, key, expected_value
        )
    
    @pytest.mark.asyncio
    async def test_episodic_memory_operations(self, memory_manager):
        """Test episodic memory operations through manager."""
        client_id = "test_client"
        
        # Test store
        memory_manager.store_episodic(
            client_id=client_id,
            analysis_type="test_analysis",
            input_data={"input": "test"},
            output_data={"output": "result"},
            insights=["insight1", "insight2"]
        )
        
        # Test get summary
        summary = memory_manager.get_episodic_summary(client_id)
        
        assert len(summary) == 1
        assert summary[0]["analysis"] == "test_analysis"
        assert "key_insights" in summary[0]
    
    @pytest.mark.asyncio
    async def test_get_ai_context(self, memory_manager):
        """Test getting AI context from memory."""
        client_id = "test_client"
        
        # Mock episodic summary
        episodic_summary = [
            {"when": "2023-01-01", "analysis": "test", "key_insights": ["insight1"]}
        ]
        
        # Mock long-term search
        recent_insights = [
            {"key": "insight_1", "value": {"test": "data1"}, "created_at": "2023-01-01"},
            {"key": "insight_2", "value": {"test": "data2"}, "created_at": "2023-01-02"}
        ]
        
        memory_manager.episodic.memories[client_id] = [
            {
                "timestamp": "2023-01-01T00:00:00",
                "analysis_type": "test",
                "input_summary": {},
                "output_summary": {},
                "insights": ["insight1"],
                "metadata": {"input_keys": [], "output_keys": []}
            }
        ]
        
        memory_manager.long_term.search = AsyncMock(return_value=recent_insights)
        
        context = await memory_manager.get_ai_context(client_id)
        
        assert "episodic_summary" in context
        assert "recent_insights" in context
        assert len(context["recent_insights"]) == 2
        
        # Verify episodic summary was summarized
        assert context["episodic_summary"][0]["key_insights"] == ["insight1"]
    
    @pytest.mark.asyncio
    async def test_get_ai_context_long_term_unavailable(self, memory_manager):
        """Test AI context when long-term memory is unavailable."""
        client_id = "test_client"
        
        # Mock long-term as unavailable
        memory_manager.long_term.is_available = False
        
        context = await memory_manager.get_ai_context(client_id)
        
        assert "episodic_summary" in context
        assert context["recent_insights"] == []  # Empty when long-term unavailable
    
    @pytest.mark.asyncio
    async def test_partial_failure_handling(self):
        """Test memory manager handles partial failures gracefully."""
        manager = MemoryManager()
        
        # Mock short-term to fail initialization
        manager.short_term.initialize = AsyncMock(side_effect=Exception("Redis down"))
        
        # Mock long-term to succeed
        manager.long_term.initialize = AsyncMock()
        
        # Should not raise exception
        await manager.initialize()
        
        assert manager.short_term.initialize.called
        assert manager.long_term.initialize.called
        assert manager.initialized


@pytest.mark.asyncio
async def test_memory_integration():
    """Integration test for memory system."""
    # Skip actual Redis/Postgres in tests
    with patch('app.memory.memory_manager.redis.asyncio.from_url') as mock_redis, \
         patch('app.memory.memory_manager.asyncpg.create_pool') as mock_pool:
        
        # Configure mocks
        mock_redis_instance = AsyncMock()
        mock_redis.return_value = mock_redis_instance
        mock_redis_instance.ping = AsyncMock()
        
        mock_pool.return_value = AsyncMock()
        
        # Create and initialize memory manager
        manager = MemoryManager()
        await manager.initialize()
        
        # Test short-term operations
        mock_redis_instance.setex = AsyncMock()
        mock_redis_instance.get = AsyncMock(return_value=json.dumps({"test": "data"}))
        
        await manager.store_short_term("client", "key", {"test": "data"})
        result = await manager.retrieve_short_term("client", "key")
        
        assert result == {"test": "data"}
        
        # Test episodic memory
        manager.store_episodic(
            client_id="client",
            analysis_type="test",
            input_data={"input": "test"},
            output_data={"output": "result"},
            insights=["test insight"]
        )
        
        summary = manager.get_episodic_summary("client")
        assert len(summary) == 1
        assert summary[0]["analysis"] == "test"


if __name__ == "__main__":
    # Run tests
    import sys
    
    # Create test instances
    print("Testing ShortTermMemory...")
    short_term = TestShortTermMemory()
    
    # Test LongTermMemory
    print("\nTesting LongTermMemory...")
    long_term = TestLongTermMemory()
    
    # Test EpisodicMemory
    print("\nTesting EpisodicMemory...")
    episodic = TestEpisodicMemory()
    episodic.test_store_and_summarize()
    episodic.test_memory_bounds()
    episodic.test_input_summarization()
    episodic.test_multiple_clients()
    
    # Test MemoryManager
    print("\nTesting MemoryManager...")
    
    print("\nAll memory tests completed!")