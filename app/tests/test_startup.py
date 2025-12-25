import pytest
import asyncio
from unittest.mock import patch

from app.memory_manager import memory_manager


@pytest.mark.asyncio
async def test_memory_initialization():
    with patch("app.memory_manager.ShortTermMemory.initialize") as mock_short_init, \
         patch("app.memory_manager.LongTermMemory.initialize") as mock_long_init:

        # ✅ CALL the real initializer
        await memory_manager.initialize()

        # ✅ NOW assertions make sense
        mock_short_init.assert_called_once()
        mock_long_init.assert_called_once()