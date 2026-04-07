import asyncio
from collections import deque
from typing import Any, Callable, Sequence, cast, get_args
from app.models.dataclasses import (
    Job,
    ResourceCategory,
    TranscriptionJob,
    TranslationJob,
)

_RESOURCE_CATEGORIES: tuple[ResourceCategory, ...] = cast(
    tuple[ResourceCategory, ...], get_args(ResourceCategory)
)


class AsyncJobQueue:
    def __init__(
        self,
        sort_key: Callable[[Job], Any] | None = None,
        descending: bool = False,
    ):
        self._queue: deque[Job] = deque()
        self._lock = asyncio.Lock()
        self._sort_key = sort_key
        self._descending = descending

    def _apply_sort(self) -> None:
        if self._sort_key is None:
            return

        keyed_jobs: list[tuple[Any, Job]] = []
        unkeyed_jobs: list[Job] = []

        for job in self._queue:
            try:
                value = self._sort_key(job)
            except Exception:
                value = None

            if value is None:
                unkeyed_jobs.append(job)
            else:
                keyed_jobs.append((value, job))

        keyed_jobs.sort(key=lambda item: item[0], reverse=self._descending)
        self._queue = deque([job for _, job in keyed_jobs] + unkeyed_jobs)

    async def add(self, job: Job) -> bool:
        """Add a job to the queue. Returns False if a job with the same ID already exists."""
        async with self._lock:
            if any(j.id == job.id for j in self._queue):
                return False
            self._queue.append(job)
            self._apply_sort()
            return True

    async def add_all(self, jobs: Sequence[Job]) -> int:
        """Add multiple jobs. Returns number of successfully added jobs."""
        if not jobs:
            return 0

        async with self._lock:
            existing_ids = {job.id for job in self._queue}
            added = 0

            for job in jobs:
                if job.id in existing_ids:
                    continue
                self._queue.append(job)
                existing_ids.add(job.id)
                added += 1

            if added > 0:
                self._apply_sort()

            return added

    async def dequeue(self) -> Job | None:
        """Remove and return the first job (FIFO)."""
        async with self._lock:
            return self._queue.popleft() if self._queue else None

    async def dequeue_n(self, n: int) -> list[Job]:
        """Remove and return up to n jobs from the front."""
        async with self._lock:
            result = []
            for _ in range(min(n, len(self._queue))):
                result.append(self._queue.popleft())
            return result

    async def peek(self) -> Job | None:
        """Return the first job without removing it."""
        async with self._lock:
            return self._queue[0] if self._queue else None

    async def remove_by_id(self, job_id: str) -> Job | None:
        """Remove a job by its ID. Returns the removed job or None."""
        async with self._lock:
            for job in self._queue:
                if job.id == job_id:
                    self._queue.remove(job)
                    return job
            return None

    async def remove_by_teletask_id(self, teletask_id: int) -> list[Job]:
        """Remove all jobs matching a teletask_id. Returns removed jobs."""
        async with self._lock:
            def _job_teletask_id(job: Job) -> int | None:
                return cast(int | None, getattr(job.params, "teletask_id", None))

            removed = [j for j in self._queue if _job_teletask_id(j) == teletask_id]
            self._queue = deque(j for j in self._queue if _job_teletask_id(j) != teletask_id)
            return removed

    async def get_all(self) -> list[Job]:
        """Get a copy of all jobs."""
        async with self._lock:
            return list(self._queue)

    async def size(self) -> int:
        async with self._lock:
            return len(self._queue)

    async def contains_id(self, job_id: str) -> bool:
        async with self._lock:
            return any(j.id == job_id for j in self._queue)


class QueueManager:
    """Single entry point for all job queues.

    Two resource categories (whisper, ollama), each with a priority and normal queue.
    Priority jobs are always dequeued before normal jobs within the same category.
    """

    def __init__(self):
        def normal_queue_sort_key(job: Job) -> Any:
            return getattr(job.params, "teletask_id", None)

        self._queues: dict[ResourceCategory, dict[str, AsyncJobQueue]] = {
            cat: {
                "priority": AsyncJobQueue(),
                "normal": AsyncJobQueue(
                    sort_key=normal_queue_sort_key,
                    descending=True,
                ),
            }
            for cat in _RESOURCE_CATEGORIES
        }
        self._job_available = asyncio.Event()

    def _category_for(self, job: Job) -> ResourceCategory:
        if isinstance(job, TranscriptionJob):
            return "whisper"
        elif isinstance(job, TranslationJob):
            return "ollama"
        raise ValueError(f"Unknown job type: {type(job)}")

    async def add(self, job: Job, priority: bool = False) -> bool:
        """Add a job to the appropriate queue. Returns False if duplicate ID."""
        category = self._category_for(job)
        tier = "priority" if priority else "normal"
        result = await self._queues[category][tier].add(job)
        if result:
            self._job_available.set()
        return result

    async def add_all(self, jobs: Sequence[Job], priority: bool = False) -> int:
        """Add multiple jobs to the appropriate queues. Returns number of added jobs."""
        if not jobs:
            return 0

        grouped: dict[ResourceCategory, list[Job]] = {cat: [] for cat in _RESOURCE_CATEGORIES}
        for job in jobs:
            grouped[self._category_for(job)].append(job)

        tier = "priority" if priority else "normal"
        added_total = 0
        for category, grouped_jobs in grouped.items():
            if not grouped_jobs:
                continue
            added_total += await self._queues[category][tier].add_all(grouped_jobs)

        if added_total > 0:
            self._job_available.set()

        return added_total

    async def winning_category(self) -> ResourceCategory | None:
        """Return the category with the highest-priority pending job.

        Priority order: whisper priority > ollama priority > whisper normal > ollama normal.
        Returns None if all queues are empty.
        """
        for category in _RESOURCE_CATEGORIES:
            if await self._queues[category]["priority"].size() > 0:
                return category
        for category in _RESOURCE_CATEGORIES:
            if await self._queues[category]["normal"].size() > 0:
                return category
        return None

    async def next(self, category: ResourceCategory, n: int = 1) -> list[Job]:
        """Get the next n jobs for a category (priority first, then normal)."""
        prio_q = self._queues[category]["priority"]
        normal_q = self._queues[category]["normal"]

        jobs = await prio_q.dequeue_n(n)
        remaining = n - len(jobs)
        if remaining > 0:
            jobs.extend(await normal_q.dequeue_n(remaining))
        return jobs

    async def remove_by_id(self, job_id: str) -> Job | None:
        """Remove a job by ID from any queue."""
        for category in self._queues.values():
            for queue in category.values():
                job = await queue.remove_by_id(job_id)
                if job is not None:
                    return job
        return None

    async def remove_by_teletask_id(self, teletask_id: int) -> list[Job]:
        """Remove all jobs matching a teletask_id from all queues."""
        removed = []
        for category in self._queues.values():
            for queue in category.values():
                removed.extend(await queue.remove_by_teletask_id(teletask_id))
        return removed

    async def get_all(self, category: ResourceCategory | None = None) -> list[Job]:
        """Get all jobs, optionally filtered by category."""
        result: list[Any] = []
        for cat in (category,) if category else _RESOURCE_CATEGORIES:
            for queue in self._queues[cat].values():
                result.extend(await queue.get_all())
        return result

    async def wait_for_job(self, timeout: float = 120) -> bool:
        """Wait until a job is available or timeout. Returns True if a job was signaled."""
        self._job_available.clear()
        try:
            await asyncio.wait_for(self._job_available.wait(), timeout=timeout)
            return True
        except TimeoutError:
            return False


# Global instance
queue_manager = QueueManager()
