import asyncio
from collections.abc import Sequence

from app.worker.worker import MockWorker, Worker
from app.scheduler.registry import JOB_TYPES
from lib.core.logger import logger
from lib.models.jobs import BaseJob, JobParamsBase, JobPayload, JobType


class WorkerManager:
    workers: dict[JobType, Worker]

    def __init__(self, workers: dict[JobType, Worker] | None = None):
        self.workers = (
            workers
            if workers is not None
            else {
                job_type: spec.worker_factory() for job_type, spec in JOB_TYPES.items()
            }
        )

    def dispatch(
        self, worker_id: str, job_type: JobType, jobs: Sequence[BaseJob]
    ) -> None:
        payloads: list[JobPayload[JobParamsBase]] = [
            JobPayload(job_id=job.id, params=job.params) for job in jobs
        ]
        task = asyncio.create_task(
            self.workers[job_type].run(worker_id, job_type, payloads)
        )
        task.add_done_callback(self._log_task_result)

    @staticmethod
    def _log_task_result(task: asyncio.Task[None]) -> None:
        try:
            _ = task.result()
        except Exception:
            logger.exception("Worker task crashed")


class MockWorkerManager(WorkerManager):
    def __init__(self):
        super().__init__(workers={job_type: MockWorker() for job_type in JOB_TYPES})
