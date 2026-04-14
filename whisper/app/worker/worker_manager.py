from collections.abc import Sequence

from app.worker.worker import MockWorker, Worker
from app.worker.local.local_worker import LocalWorker
from app.scheduler.registry import JOB_TYPES, RESOURCES
from lib.models.jobs import BaseJob, JobType, ResourceType


class WorkerManager:
    workers: dict[ResourceType, Worker]

    def __init__(self, workers: dict[ResourceType, Worker] | None = None):
        self.workers = workers if workers is not None else {r: LocalWorker() for r in RESOURCES}

    def dispatch(self, worker_id: str, job_type: JobType, jobs: Sequence[BaseJob]) -> None:
        resource = JOB_TYPES[job_type].resource
        self.workers[resource].run(worker_id, job_type, jobs)


class MockWorkerManager(WorkerManager):
    def __init__(self):
        super().__init__(workers={r: MockWorker() for r in RESOURCES})
