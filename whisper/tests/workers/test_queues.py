"""
Tests for workers/queues.py — AsyncQueue

AsyncQueue is a pure in-memory async data structure with no external
dependencies, so it can be tested directly without mocking.
"""

import pytest
from app.workers.queues import AsyncQueue, multi_lock


class TestAsyncQueueBasic:
    @pytest.fixture
    def queue(self):
        return AsyncQueue()

    @pytest.mark.asyncio
    async def test_add_and_get_all(self, queue):
        await queue.add(1)
        await queue.add(2)
        assert await queue.get_all() == [1, 2]

    @pytest.mark.asyncio
    async def test_add_duplicates_ignored(self, queue):
        await queue.add(1)
        await queue.add(1)
        assert await queue.get_all() == [1]

    @pytest.mark.asyncio
    async def test_remove_existing(self, queue):
        await queue.add(1)
        await queue.add(2)
        await queue.remove(1)
        assert await queue.get_all() == [2]

    @pytest.mark.asyncio
    async def test_remove_nonexistent_is_noop(self, queue):
        await queue.add(1)
        await queue.remove(99)
        assert await queue.get_all() == [1]

    @pytest.mark.asyncio
    async def test_contains(self, queue):
        await queue.add(10)
        assert await queue.contains(10) is True
        assert await queue.contains(20) is False

    @pytest.mark.asyncio
    async def test_peek_empty(self, queue):
        assert await queue.peek() is None

    @pytest.mark.asyncio
    async def test_peek_nonempty(self, queue):
        await queue.add(100)
        await queue.add(200)
        assert await queue.peek() == 100
        # peek should NOT remove the item
        assert await queue.get_all() == [100, 200]

    @pytest.mark.asyncio
    async def test_dequeue_fifo(self, queue):
        await queue.add(1)
        await queue.add(2)
        await queue.add(3)
        assert await queue.dequeue() == 1
        assert await queue.dequeue() == 2
        assert await queue.get_all() == [3]

    @pytest.mark.asyncio
    async def test_dequeue_empty(self, queue):
        assert await queue.dequeue() is None

    @pytest.mark.asyncio
    async def test_replace(self, queue):
        await queue.add(1)
        await queue.add(2)
        await queue.replace([10, 20, 30])
        assert await queue.get_all() == [10, 20, 30]

    @pytest.mark.asyncio
    async def test_replace_deduplicates(self, queue):
        await queue.replace([1, 2, 1, 3, 2])
        assert await queue.get_all() == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_sort_reverse(self, queue):
        await queue.add(1)
        await queue.add(3)
        await queue.add(2)
        await queue.sort_reverse()
        assert await queue.get_all() == [3, 2, 1]


class TestAsyncQueueUnlockedMethods:
    """Test the _unlocked variants used inside multi_lock contexts."""

    @pytest.fixture
    def queue(self):
        return AsyncQueue()

    @pytest.mark.asyncio
    async def test_add_unlocked(self, queue):
        await queue.add_unlocked(10)
        assert await queue.get_all() == [10]

    @pytest.mark.asyncio
    async def test_remove_unlocked(self, queue):
        await queue.add_unlocked(1)
        await queue.add_unlocked(2)
        await queue.remove_unlocked(1)
        assert await queue.get_all() == [2]

    @pytest.mark.asyncio
    async def test_dequeue_unlocked(self, queue):
        await queue.add_unlocked(100)
        await queue.add_unlocked(200)
        val = await queue.dequeue_unlocked()
        assert val == 100
        assert await queue.get_all() == [200]


class TestMultiLock:
    @pytest.mark.asyncio
    async def test_multi_lock_acquires_all(self):
        q1 = AsyncQueue()
        q2 = AsyncQueue()
        await q1.add(1)
        await q2.add(2)

        async with multi_lock([q1, q2]):
            # Inside multi_lock, we use unlocked methods
            await q1.add_unlocked(3)
            await q2.remove_unlocked(2)

        assert await q1.get_all() == [1, 3]
        assert await q2.get_all() == []
