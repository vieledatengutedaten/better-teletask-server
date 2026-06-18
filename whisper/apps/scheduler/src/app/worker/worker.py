from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import override

from lib.models.jobs import JobParamsBase, JobPayload, JobType


class Worker(ABC):
    @abstractmethod
    async def run(
        self,
        worker_id: str,
        job_type: JobType,
        payloads: Sequence[JobPayload[JobParamsBase]],
    ) -> None: ...


class MockWorker(Worker):
    @override
    async def run(
        self,
        worker_id: str,
        job_type: JobType,
        payloads: Sequence[JobPayload[JobParamsBase]],
    ) -> None:
        return
