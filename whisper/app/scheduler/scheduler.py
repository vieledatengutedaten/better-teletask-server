
import asyncio
from asyncio.locks import Event
from typing import cast

from app.core.logger import logger
from app.models.dataclasses import Job, ResourceCategory, TranscriptionJob, TranslationJob
from app.scheduler.queues import QueueManager
from app.worker.worker import Worker
from app.worker.local.local_worker import LocalWorker
from app.worker.worker_manager import WorkerManager


_scheduler: "Scheduler | None" = None


def set_scheduler(scheduler: "Scheduler") -> None:
    global _scheduler
    _scheduler = scheduler


def get_scheduler() -> "Scheduler":
    if _scheduler is None:
        raise RuntimeError("Scheduler not initialized")
    return _scheduler


class Scheduler:
    """Dispatches job batches from the QueueManager when worker capacity is available.

    Priority order (highest to lowest):
        1. whisper priority  (transcription priority)
        2. ollama priority   (translation priority)
        3. whisper normal    (transcription normal)
        4. ollama normal     (translation normal)

    Once the winning category is determined, the batch is filled exclusively from
    that category (priority slots first, then normal) up to batch_size. This allows
    a single worker process to load the model once and process multiple jobs.

    Workers are external (SLURM / subprocess). When a worker finishes or fails,
    it calls back via the API, which should call `worker_finished()` to free up
    capacity and wake the scheduler.
    """

    def __init__(
        self,
        queue_manager: QueueManager,
        max_workers: int = 5,
        batch_size: int = 10,
        worker_manager: WorkerManager | None = None,
    ) -> None:
        self.queue_manager: QueueManager = queue_manager
        self.max_workers: int = max_workers
        self.batch_size: int = batch_size
        self.worker_manager: WorkerManager = worker_manager or WorkerManager()
        self._active_workers: dict[str, list[Job]] = {}
        self._jobs_by_id: dict[str, Job] = {}
        self._wake: Event = asyncio.Event()
        self._worker_counter: int = 0

    @property
    def available_capacity(self) -> int:
        return self.max_workers - len(self._active_workers)

    @property
    def active_jobs(self) -> list[Job]:
        return list(self._jobs_by_id.values())

    def get_job(self, job_id: str) -> Job | None:
        return self._jobs_by_id.get(job_id)

    def get_worker_id_for_job(self, job_id: str) -> str | None:
        for worker_id, jobs in self._active_workers.items():
            if any(job.id == job_id for job in jobs):
                return worker_id
        return None

    async def _next_batch(self, n: int) -> list[Job]:
        """Return up to n jobs, all from the highest-priority category.

        Within the chosen category, priority jobs are dequeued before normal jobs.
        Returns an empty list if all queues are empty.
        """
        category: ResourceCategory | None = await self.queue_manager.winning_category()
        if category is None:
            return []
        return await self.queue_manager.next(category, n)

    def _dispatch_batch(self, jobs: list[Job]) -> None:
        """Register a batch of jobs as active and dispatch to worker. Returns worker_id."""
        self._worker_counter += 1
        worker_id = f"worker-{self._worker_counter}"
        for job in jobs:
            job.status = "RUNNING"
            self._jobs_by_id[job.id] = job
        self._active_workers[worker_id] = jobs
        logger.info(
            f"Dispatched {worker_id} with {len(jobs)} {jobs[0].job_type} job(s) [{', '.join(j.id for j in jobs)}]"
        )

        first = jobs[0]
        if isinstance(first, TranscriptionJob):
            self.worker_manager.transcribe(worker_id, cast(list[TranscriptionJob], jobs))
        else:
            self.worker_manager.translate(worker_id, cast(list[TranslationJob], jobs))

    async def _dispatch_available(self) -> int:
        """Dispatch workers until capacity is full or all queues are empty.

        Each iteration starts one worker with up to batch_size jobs from the
        highest-priority category. Returns total number of workers dispatched.
        """
        dispatched = 0
        while self.available_capacity > 0:
            jobs = await self._next_batch(self.batch_size)
            if not jobs:
                break
            self._dispatch_batch(jobs)
            dispatched += 1
        return dispatched

    def worker_finished(self, worker_id: str) -> list[Job] | None:
        """Called when a worker completes or fails.

        Frees one capacity slot and wakes the scheduler so it can dispatch
        the next batch immediately without waiting for the timeout.
        """
        jobs = self._active_workers.pop(worker_id, None)
        if jobs is None:
            logger.warning(f"worker_finished called for unknown worker {worker_id}")
        else:
            for job in jobs:
                _ = self._jobs_by_id.pop(job.id, None)
            logger.info(f"Worker {worker_id} finished ({len(jobs)} job(s))")
        self._wake.set()
        return jobs

    def worker_finished_for_job(self, job_id: str) -> list[Job] | None:
        """Finish the worker that currently owns the given job ID."""
        worker_id = self.get_worker_id_for_job(job_id)
        if worker_id is not None:
            return self.worker_finished(worker_id)

        logger.warning(f"worker_finished_for_job called for unknown job {job_id}")
        self._wake.set()
        return None

    async def run(self) -> None:
        """Main scheduler loop. Runs forever, dispatching jobs as capacity opens up."""
        logger.info(
            f"Scheduler started (max_workers={self.max_workers}, batch_size={self.batch_size})"
        )
        while True:
            dispatched = await self._dispatch_available()

            if dispatched > 0:
                logger.info(
                    f"Dispatched {dispatched} job(s), {self.available_capacity} slot(s) remaining"
                )

            # Wait for either: a new job added to queues, or a worker finishing
            self._wake.clear()
            _, pending = await asyncio.wait(
                [
                    asyncio.create_task(self.queue_manager.wait_for_job(timeout=120)),
                    asyncio.create_task(self._wake.wait()),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                _ = task.cancel()
