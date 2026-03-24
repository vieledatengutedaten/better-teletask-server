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
        await queue.add("a")
        await queue.add("b")
        assert await queue.get_all() == ["a", "b"]

    @pytest.mark.asyncio
    async def test_add_duplicates_ignored(self, queue):
        await queue.add("a")
        await queue.add("a")
        assert await queue.get_all() == ["a"]

    @pytest.mark.asyncio
    async def test_remove_existing(self, queue):
        await queue.add("a")
        await queue.add("b")
        await queue.remove("a")
        assert await queue.get_all() == ["b"]

    @pytest.mark.asyncio
    async def test_remove_nonexistent_is_noop(self, queue):
        await queue.add("a")
        await queue.remove("z")
        assert await queue.get_all() == ["a"]

    @pytest.mark.asyncio
    async def test_contains(self, queue):
        await queue.add("x")
        assert await queue.contains("x") is True
        assert await queue.contains("y") is False

    @pytest.mark.asyncio
    async def test_peek_empty(self, queue):
        assert await queue.peek() is None

    @pytest.mark.asyncio
    async def test_peek_nonempty(self, queue):
        await queue.add("first")
        await queue.add("second")
        assert await queue.peek() == "first"
        # peek should NOT remove the item
        assert await queue.get_all() == ["first", "second"]

    @pytest.mark.asyncio
    async def test_dequeue_fifo(self, queue):
        await queue.add("a")
        await queue.add("b")
        await queue.add("c")
        assert await queue.dequeue() == "a"
        assert await queue.dequeue() == "b"
        assert await queue.get_all() == ["c"]

    @pytest.mark.asyncio
    async def test_dequeue_empty(self, queue):
        assert await queue.dequeue() is None

    @pytest.mark.asyncio
    async def test_replace(self, queue):
        await queue.add("old1")
        await queue.add("old2")
        await queue.replace(["new1", "new2", "new3"])
        assert await queue.get_all() == ["new1", "new2", "new3"]

    @pytest.mark.asyncio
    async def test_replace_deduplicates(self, queue):
        await queue.replace(["a", "b", "a", "c", "b"])
        assert await queue.get_all() == ["a", "b", "c"]

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
        await queue.add_unlocked("x")
        assert await queue.get_all() == ["x"]

    @pytest.mark.asyncio
    async def test_remove_unlocked(self, queue):
        await queue.add_unlocked("a")
        await queue.add_unlocked("b")
        await queue.remove_unlocked("a")
        assert await queue.get_all() == ["b"]

    @pytest.mark.asyncio
    async def test_dequeue_unlocked(self, queue):
        await queue.add_unlocked("first")
        await queue.add_unlocked("second")
        val = await queue.dequeue_unlocked()
        assert val == "first"
        assert await queue.get_all() == ["second"]


class TestMultiLock:
    @pytest.mark.asyncio
    async def test_multi_lock_acquires_all(self):
        q1 = AsyncQueue()
        q2 = AsyncQueue()
        await q1.add("a")
        await q2.add("b")

        async with multi_lock([q1, q2]):
            # Inside multi_lock, we use unlocked methods
            await q1.add_unlocked("c")
            await q2.remove_unlocked("b")

        assert await q1.get_all() == ["a", "c"]
        assert await q2.get_all() == []
