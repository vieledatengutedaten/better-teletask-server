import asyncio
from collections import deque
from collections.abc import Sequence
from typing import Any, Callable, cast

from app.utils.broadcast import fire_broadcast
from app.scheduler.registry import JOB_TYPES
from lib.models.jobs import BaseJob, JobType


def _job_sort_key(job: BaseJob) -> tuple[int, int]:
    """Sort by (priority DESC, teletask_id DESC). Larger tuple = head of queue."""
    tid = cast(int, getattr(job.params, "teletask_id", 0))
    return (job.priority, tid)


class AsyncJobQueue:
    def __init__(
        self,
        sort_key: Callable[[BaseJob], Any] | None = None,
        descending: bool = False,
    ):
        self._queue: deque[BaseJob] = deque()
        self._lock = asyncio.Lock()
        self._sort_key = sort_key
        self._descending = descending

    def _apply_sort(self) -> None:
        if self._sort_key is None:
            return

        keyed_jobs: list[tuple[Any, BaseJob]] = []
        unkeyed_jobs: list[BaseJob] = []

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

    async def add(self, job: BaseJob) -> bool:
        async with self._lock:
            if any(j.id == job.id for j in self._queue):
                return False
            self._queue.append(job)
            self._apply_sort()
            return True

    async def add_all(self, jobs: Sequence[BaseJob]) -> int:
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

    async def dequeue(self) -> BaseJob | None:
        async with self._lock:
            return self._queue.popleft() if self._queue else None

    async def dequeue_n(self, n: int) -> list[BaseJob]:
        async with self._lock:
            result: list[BaseJob] = []
            for _ in range(min(n, len(self._queue))):
                result.append(self._queue.popleft())
            return result

    async def peek(self) -> BaseJob | None:
        async with self._lock:
            return self._queue[0] if self._queue else None

    async def remove_by_id(self, job_id: str) -> BaseJob | None:
        async with self._lock:
            for job in self._queue:
                if job.id == job_id:
                    self._queue.remove(job)
                    return job
            return None

    async def remove_by_teletask_id(self, teletask_id: int) -> list[BaseJob]:
        async with self._lock:
            def _job_teletask_id(job: BaseJob) -> int | None:
                return cast(int | None, getattr(job.params, "teletask_id", None))

            removed = [j for j in self._queue if _job_teletask_id(j) == teletask_id]
            self._queue = deque(j for j in self._queue if _job_teletask_id(j) != teletask_id)
            return removed

    async def get_all(self) -> list[BaseJob]:
        async with self._lock:
            return list(self._queue)

    async def size(self) -> int:
        async with self._lock:
            return len(self._queue)

    async def contains_id(self, job_id: str) -> bool:
        async with self._lock:
            return any(j.id == job_id for j in self._queue)


class QueueManager:
    """One queue per jobtype, sorted by (job.priority DESC, teletask_id DESC).

    Between-jobtype priority is NOT handled here — that's the scheduler's job
    via JobTypeSpec.base_priority. This class only orders jobs within a single
    jobtype's queue, and deduplicates against both pending and in-flight jobs.
    """

    def __init__(self):
        self._queues: dict[JobType, AsyncJobQueue] = {
            jt: AsyncJobQueue(sort_key=_job_sort_key, descending=True)
            for jt in JOB_TYPES
        }
        self._in_flight: dict[JobType, set[str]] = {jt: set() for jt in JOB_TYPES}
        self._job_available = asyncio.Event()

    async def add(self, job: BaseJob) -> bool:
        if job.id in self._in_flight[job.job_type]:
            return False

        result = await self._queues[job.job_type].add(job)
        if result:
            self._job_available.set()
            fire_broadcast()
        return result

    async def add_all(self, jobs: Sequence[BaseJob]) -> int:
        if not jobs:
            return 0

        grouped: dict[JobType, list[BaseJob]] = {jt: [] for jt in JOB_TYPES}
        for job in jobs:
            if job.id in self._in_flight[job.job_type]:
                continue
            grouped[job.job_type].append(job)

        added = 0
        for jt, jt_jobs in grouped.items():
            if not jt_jobs:
                continue
            added += await self._queues[jt].add_all(jt_jobs)

        if added > 0:
            self._job_available.set()
            fire_broadcast()
        return added

    async def next(self, job_type: JobType, n: int = 1) -> list[BaseJob]:
        """Dequeue and claim up to n jobs for dispatch.

        Claimed jobs are moved into an in-flight set and remain deduplicated
        against add/add_all until release/release_all is called.
        """
        if n <= 0:
            return []

        claimed: list[BaseJob] = []
        while len(claimed) < n:
            candidates = await self._queues[job_type].dequeue_n(n - len(claimed))
            if not candidates:
                break

            for job in candidates:
                if job.id in self._in_flight[job_type]:
                    continue
                self._in_flight[job_type].add(job.id)
                claimed.append(job)

        return claimed

    def release(self, job_type: JobType, job_id: str) -> None:
        self._in_flight[job_type].discard(job_id)

    def release_all(self, jobs: Sequence[BaseJob]) -> None:
        for job in jobs:
            self.release(job.job_type, job.id)

    async def has_pending(self, job_type: JobType) -> bool:
        return await self._queues[job_type].size() > 0

    async def pending_teletask_ids(self, job_type: JobType) -> set[int]:
        all_jobs = await self._queues[job_type].get_all()
        return {
            cast(int, getattr(j.params, "teletask_id", 0))
            for j in all_jobs
        }

    async def remove_by_id(self, job_id: str) -> BaseJob | None:
        for queue in self._queues.values():
            job = await queue.remove_by_id(job_id)
            if job is not None:
                return job
        return None

    async def remove_by_teletask_id(self, teletask_id: int) -> list[BaseJob]:
        removed: list[BaseJob] = []
        for queue in self._queues.values():
            removed.extend(await queue.remove_by_teletask_id(teletask_id))
        return removed

    async def get_all(self, job_type: JobType | None = None) -> list[BaseJob]:
        result: list[BaseJob] = []
        targets: tuple[JobType, ...] = (job_type,) if job_type else tuple(self._queues.keys())
        for jt in targets:
            result.extend(await self._queues[jt].get_all())
        return result

    async def snapshot(self) -> dict[str, int]:
        return {jt: await self._queues[jt].size() for jt in self._queues}

    async def wait_for_job(self, timeout: float = 120) -> bool:
        self._job_available.clear()
        try:
            await asyncio.wait_for(self._job_available.wait(), timeout=timeout)
            return True
        except TimeoutError:
            return False


queue_manager = QueueManager()
