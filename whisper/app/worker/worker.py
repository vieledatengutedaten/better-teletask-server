from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import override

from lib.models.jobs import BaseJob, JobType


class Worker(ABC):
    @abstractmethod
    def run(self, worker_id: str, job_type: JobType, jobs: Sequence[BaseJob]) -> None: ...


class MockWorker(Worker):
    @override
    def run(self, worker_id: str, job_type: JobType, jobs: Sequence[BaseJob]) -> None:
        return
