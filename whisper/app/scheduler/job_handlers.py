from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import override

from app.core.logger import logger
from app.models.dataclasses import (
    Job,
    JobType,
    TranscriptionResult,
    TranslationResult,
)


class JobHandler(ABC):
    @abstractmethod
    def parse_result(
        self, body: Mapping[str, object]
    ) -> TranscriptionResult | TranslationResult:
        raise NotImplementedError

    @abstractmethod
    async def handle_result(
        self, job: Job, result: TranscriptionResult | TranslationResult
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def handle_failed(self, job: Job, reason: str) -> None:
        raise NotImplementedError


class TranscriptionJobHandler(JobHandler):
    @override
    def parse_result(self, body: Mapping[str, object]) -> TranscriptionResult:
        return TranscriptionResult.model_validate(body)

    @override
    async def handle_result(
        self, job: Job, result: TranscriptionResult | TranslationResult
    ) -> None:
        if not isinstance(result, TranscriptionResult):
            raise TypeError("TranscriptionJobHandler received non-transcription result")
        logger.info(f"[mock] handled transcription result for {result.job_id}")

    @override
    async def handle_failed(self, job: Job, reason: str) -> None:
        logger.error(f"[mock] transcription job {job.id} failed: {reason}")


class TranslationJobHandler(JobHandler):
    @override
    def parse_result(self, body: Mapping[str, object]) -> TranslationResult:
        return TranslationResult.model_validate(body)

    @override
    async def handle_result(
        self, job: Job, result: TranscriptionResult | TranslationResult
    ) -> None:
        if not isinstance(result, TranslationResult):
            raise TypeError("TranslationJobHandler received non-translation result")
        logger.info(f"[mock] handled translation result for {result.job_id}")

    @override
    async def handle_failed(self, job: Job, reason: str) -> None:
        logger.error(f"[mock] translation job {job.id} failed: {reason}")


_JOB_HANDLERS: dict[JobType, JobHandler] = {
    "transcription": TranscriptionJobHandler(),
    "translation": TranslationJobHandler(),
}


def get_job_handler(job_type: JobType) -> JobHandler:
    return _JOB_HANDLERS[job_type]
