import asyncio
from typing import cast

from lib.core.logger import logger
from app.utils.broadcast import fire_broadcast
from lib.models.jobs import Job, JobType, ResourceType
from app.scheduler.queues import QueueManager
from app.scheduler.registry import JOB_TYPES, RESOURCES, spec_for
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
    """Dispatches job batches when worker capacity is available.

    Capacity is tracked per resource (whisper, ollama, cpu — see registry.RESOURCES).
    Each tick:
      1. Find candidate jobtypes: those with pending jobs AND free capacity on their resource.
      2. Pick the winner by max(spec.base_priority).
      3. Pull up to spec.batch_size jobs from that queue, run prepare(), dispatch.

    Workers are external (SLURM / subprocess). When a worker finishes or fails,
    it calls back via the API, which calls worker_finished() to free capacity
    and wake the scheduler.
    """

    def __init__(
        self,
        queue_manager: QueueManager,
        worker_manager: WorkerManager | None = None,
    ) -> None:
        self.queue_manager: QueueManager = queue_manager
        self.worker_manager: WorkerManager = worker_manager or WorkerManager()
        self._active: dict[ResourceType, dict[str, list[Job]]] = {
            r: {} for r in RESOURCES
        }
        self._jobs_by_id: dict[str, Job] = {}
        self._wake: asyncio.Event = asyncio.Event()
        self._worker_counter: int = 0

    def capacity_for(self, resource: ResourceType) -> int:
        return RESOURCES[resource].max_workers - len(self._active[resource])

    @property
    def active_jobs(self) -> list[Job]:
        return list(self._jobs_by_id.values())

    def get_job(self, job_id: str) -> Job | None:
        return self._jobs_by_id.get(job_id)

    def get_worker_id_for_job(self, job_id: str) -> str | None:
        for resource_workers in self._active.values():
            for worker_id, jobs in resource_workers.items():
                if any(j.id == job_id for j in jobs):
                    return worker_id
        return None

    def active_teletask_ids(self, job_type: JobType) -> set[int]:
        resource = spec_for(job_type).resource
        return {
            cast(int, getattr(j.params, "teletask_id", 0))
            for jobs in self._active[resource].values()
            for j in jobs
            if j.job_type == job_type
        }

    def snapshot(self) -> dict[str, object]:
        return {
            "resources": {
                r: {
                    "max_workers": RESOURCES[r].max_workers,
                    "available_capacity": self.capacity_for(r),
                    "active_workers": {
                        worker_id: [job.model_dump() for job in jobs]
                        for worker_id, jobs in self._active[r].items()
                    },
                }
                for r in RESOURCES
            },
            "jobs_by_id": {
                job_id: job.model_dump() for job_id, job in self._jobs_by_id.items()
            },
        }

    async def _pick_next_jobtype(self) -> JobType | None:
        """Highest-base_priority jobtype that has pending work AND free capacity on its resource."""
        candidates: list[JobType] = []
        for jt, spec in JOB_TYPES.items():
            if self.capacity_for(spec.resource) <= 0:
                continue
            if not await self.queue_manager.has_pending(jt):
                continue
            candidates.append(jt)
        if not candidates:
            return None
        return max(candidates, key=lambda jt: JOB_TYPES[jt].base_priority)

    async def _next_prepared_batch(self, job_type: JobType, n: int) -> list[Job]:
        """Drain up to n jobs from job_type's queue that pass handler.prepare()."""
        spec = spec_for(job_type)
        prepared: list[Job] = []
        while len(prepared) < n:
            candidates = await self.queue_manager.next(job_type, 1)
            if not candidates:
                break
            job = candidates[0]

            if not isinstance(job, spec.job_cls):
                job.status = "FAILED"
                logger.error(
                    f"Queue returned invalid job class for {job_type}: {type(job).__name__}"
                )
                self.queue_manager.release(job_type, job.id)
                continue

            typed_job = cast(Job, job)
            try:
                ok = spec.handler.prepare(typed_job)
            except Exception as exc:
                ok = False
                logger.exception(f"Prepare crashed for job {typed_job.id}: {exc}")
            if ok:
                prepared.append(typed_job)
            else:
                typed_job.status = "FAILED"
                logger.warning(f"Prepare rejected job {typed_job.id}; skipping dispatch")
                self.queue_manager.release(job_type, typed_job.id)
        return prepared

    def _dispatch_batch(self, job_type: JobType, jobs: list[Job]) -> None:
        spec = spec_for(job_type)
        self._worker_counter += 1
        worker_id = f"worker-{self._worker_counter}"
        for job in jobs:
            job.status = "RUNNING"
            self._jobs_by_id[job.id] = job
        self._active[spec.resource][worker_id] = jobs
        logger.info(
            f"Dispatched {worker_id} ({spec.resource}) with {len(jobs)} {job_type} job(s) [{', '.join(j.id for j in jobs)}]"
        )
        self.worker_manager.dispatch(worker_id, job_type, jobs)
        fire_broadcast()

    async def _dispatch_available(self) -> int:
        """Dispatch workers until no jobtype has both pending work and free capacity."""
        dispatched = 0
        while True:
            jt = await self._pick_next_jobtype()
            if jt is None:
                break
            spec = spec_for(jt)
            jobs = await self._next_prepared_batch(jt, spec.batch_size)
            if not jobs:
                continue
            self._dispatch_batch(jt, jobs)
            dispatched += 1
        return dispatched

    def worker_finished(self, worker_id: str) -> list[Job] | None:
        for resource, workers in self._active.items():
            if worker_id in workers:
                jobs = workers.pop(worker_id)
                for job in jobs:
                    if job.status not in ("COMPLETED", "FAILED"):
                        # TODO reschedule job at this point?
                        logger.error(
                            f"Worker {worker_id} finished, although Job {job.id} did not complete or fail {job.status}"
                        )
                    _ = self._jobs_by_id.pop(job.id, None)
                self.queue_manager.release_all(jobs)
                logger.info(f"Worker {worker_id} finished ({len(jobs)} job(s) on {resource})")
                self._wake.set()
                fire_broadcast()
                return jobs
        logger.warning(f"worker_finished called for unknown worker {worker_id}")
        self._wake.set()
        fire_broadcast()
        return None

    def worker_finished_for_job(self, job_id: str) -> list[Job] | None:
        worker_id = self.get_worker_id_for_job(job_id)
        if worker_id is not None:
            return self.worker_finished(worker_id)
        logger.warning(f"worker_finished_for_job called for unknown job {job_id}")
        self._wake.set()
        return None

    async def run(self) -> None:
        limits = ", ".join(f"{r}={spec.max_workers}" for r, spec in RESOURCES.items())
        logger.info(f"Scheduler started; resource limits: {limits}")
        while True:
            dispatched = await self._dispatch_available()
            if dispatched > 0:
                free = ", ".join(f"{r}={self.capacity_for(r)}" for r in RESOURCES)
                logger.info(f"Dispatched {dispatched} worker(s); free capacity: {free}")

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
