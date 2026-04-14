from collections.abc import Sequence
from typing import override

from app.worker.worker import Worker
from lib.models.jobs import BaseJob, JobType


class LocalWorker(Worker):
    @override
    def run(self, worker_id: str, job_type: JobType, jobs: Sequence[BaseJob]) -> None:
        return
