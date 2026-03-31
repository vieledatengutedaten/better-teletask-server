import asyncio
from collections import deque
import contextlib


class AsyncQueue:
    def __init__(self):
        self._queue: deque[int] = deque()
        self._lock = asyncio.Lock()

    # ----------------------
    # Lock-protected methods
    # ----------------------

    async def add(self, item_id: int):
        """Add an ID to the queue (if not already present)."""
        async with self._lock:
            if item_id not in self._queue:
                self._queue.append(item_id)

    async def remove(self, item_id: int):
        """Remove an ID from the queue (if present)."""
        async with self._lock:
            try:
                self._queue.remove(item_id)
            except ValueError:
                pass

    async def contains(self, item_id: int) -> bool:
        """Check if an ID is in the queue."""
        async with self._lock:
            return item_id in self._queue

    async def get_all(self) -> list[int]:
        """Get a copy of all items."""
        async with self._lock:
            return list(self._queue)

    async def replace(self, new_items: list[int]):
        """Replace the entire queue with a new, ordered list."""
        async with self._lock:
            self._queue = deque(dict.fromkeys(new_items))

    async def peek(self) -> int | None:
        """Return the first item (without removing it)."""
        async with self._lock:
            return self._queue[0] if self._queue else None

    async def dequeue(self) -> int | None:
        """Remove and return the first item (FIFO)."""
        async with self._lock:
            return self._queue.popleft() if self._queue else None

    async def sort_reverse(self):
        """Sort the queue in reverse order."""
        async with self._lock:
            self._queue = deque(sorted(self._queue, reverse=True))

    # ----------------------
    # Unlocked (non-blocking) methods
    # ----------------------

    async def add_unlocked(self, item_id: int):
        """Add an ID without acquiring lock."""
        if item_id not in self._queue:
            self._queue.append(item_id)

    async def remove_unlocked(self, item_id: int):
        """Remove an ID without acquiring lock."""
        try:
            self._queue.remove(item_id)
        except ValueError:
            pass

    async def contains_unlocked(self, item_id: int) -> bool:
        """Check if an ID exists without lock."""
        return item_id in self._queue

    async def get_all_unlocked(self) -> list[int]:
        """Return all items without lock."""
        return list(self._queue)

    async def replace_unlocked(self, new_items: list[int]) -> None:
        """Replace the queue without lock."""
        self._queue = deque(dict.fromkeys(new_items))

    async def peek_unlocked(self) -> int | None:
        """Return first item without lock."""
        return self._queue[0] if self._queue else None

    async def dequeue_unlocked(self) -> int | None:
        """Remove and return first item without lock."""
        return self._queue.popleft() if self._queue else None

    async def sort_reverse_unlocked(self):
        """Sort the queue in reverse order without lock."""
        self._queue = deque(sorted(self._queue, reverse=True))


@contextlib.asynccontextmanager
async def multi_lock(queues: list["AsyncQueue"]):
    """Acquire multiple AsyncQueue locks safely, avoiding deadlocks."""
    sorted_queues = sorted(queues, key=id)
    acquired_locks = []
    try:
        for q in sorted_queues:
            await q._lock.acquire()
            acquired_locks.append(q._lock)
        yield
    finally:
        for lock in reversed(acquired_locks):
            lock.release()


# --- Global queue instances ---
prio_queue = AsyncQueue()
forward_queue = AsyncQueue()
in_between_queue = AsyncQueue()
backward_queue = AsyncQueue()
in_process_queue = AsyncQueue()
