import asyncio
from collections.abc import Sequence
from typing import override

from app.worker.worker import Worker
from lib.models.jobs import (
    JobType,
    ScrapeLectureDataJob,
    ScrapeLectureDataParams,
    JobParamsBase,
    TranscriptionParams,
    TranslationParams,
)
from worker.scrape import run_scrape
from worker.transcribe import run_transcription
from app.worker.utils import require_params



class LocalWorker(Worker):

    @override
    async def run(
        self,
        worker_id: str,
        job_type: JobType,
        params: Sequence[JobParamsBase],
    ) -> None:
        match job_type:
            case "scrape_lecture_data":
                scrape_params = require_params(
                    params, ScrapeLectureDataParams, job_type
                )
                scrape_jobs = [ScrapeLectureDataJob(params=p) for p in scrape_params]
                _ = await asyncio.to_thread(run_scrape, scrape_jobs, worker_id)
            case "transcription":
                transcription_params = require_params(
                    params, TranscriptionParams, job_type
                )
                for item in transcription_params:
                    await asyncio.to_thread(run_transcription, item)
            case "translation":
                _ = require_params(params, TranslationParams, job_type)
                raise NotImplementedError(
                    "LocalWorker does not implement 'translation' execution yet"
                )
