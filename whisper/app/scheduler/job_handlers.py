from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import override

from lib.core.logger import logger
from lib.models.dataclasses import (
    Job,
    JobResult,
    JobType,
    TranscriptionResult,
    TranslationResult,
)

"""Handles pre and post job logic"""
class JobHandler(ABC):
    
    @abstractmethod
    def prepare(self, job: Job) -> bool:
        """
        Perform any necessary preparation before the job is executed.
        Return True if preparation succeeded and the job can proceed, False to skip the job.
        """
        raise NotImplementedError


    @abstractmethod
    def parse_result(self, body: Mapping[str, object]) -> JobResult:
        raise NotImplementedError

    @abstractmethod
    async def handle_result(self, job: Job, result: JobResult) -> None:
        raise NotImplementedError

    @abstractmethod
    async def handle_failed(self, job: Job, reason: str) -> None:
        raise NotImplementedError


class TranscriptionJobHandler(JobHandler):
    @override
    def prepare(self, job: Job) -> bool:
        """
        Here we will do
        1. check if lecture is pingable
        2. scrape the lecture data if not already availbale
        3. figgure out language
        4. optinally prepare prompt based on old lectures of the series
        """
        logger.info(f"[mock] preparing transcription job {job.id}")
        return True

    @override
    def parse_result(self, body: Mapping[str, object]) -> TranscriptionResult:
        return TranscriptionResult.model_validate(body)

    @override
    async def handle_result(self, job: Job, result: JobResult) -> None:
        if not isinstance(result, TranscriptionResult):
            raise TypeError("TranscriptionJobHandler received non-transcription result")
        logger.info(f"[mock] handled transcription result for {result.job_id}")

    @override
    async def handle_failed(self, job: Job, reason: str) -> None:
        logger.error(f"[mock] transcription job {job.id} failed: {reason}")


class TranslationJobHandler(JobHandler):
    @override
    def prepare(self, job: Job) -> bool:
        logger.info(f"[mock] preparing translation job {job.id}")
        return True

    @override
    def parse_result(self, body: Mapping[str, object]) -> TranslationResult:
        return TranslationResult.model_validate(body)

    @override
    async def handle_result(self, job: Job, result: JobResult) -> None:
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
