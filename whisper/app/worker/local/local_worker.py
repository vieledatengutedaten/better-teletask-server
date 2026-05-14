import asyncio
from collections.abc import Sequence
from typing import override

from app.worker.worker import Worker
from lib.models.jobs import (
    JobType,
    ScrapeLectureDataParams,
    JobParamsBase,
    JobPayload,
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
        payloads: Sequence[JobPayload[JobParamsBase]],
    ) -> None:
        match job_type:
            case "scrape_lecture_data":
                scrape_params = require_params(
                    [payload.params for payload in payloads],
                    ScrapeLectureDataParams,
                    job_type,
                )
                scrape_payloads = [
                    JobPayload(job_id=payload.job_id, params=params)
                    for payload, params in zip(payloads, scrape_params, strict=True)
                ]
                _ = await asyncio.to_thread(run_scrape, scrape_payloads, worker_id)
            case "transcription":
                transcription_params = require_params(
                    [payload.params for payload in payloads],
                    TranscriptionParams,
                    job_type,
                )
                for params in transcription_params:
                    await asyncio.to_thread(run_transcription, params)
            case "translation":
                _ = require_params(
                    [payload.params for payload in payloads],
                    TranslationParams,
                    job_type,
                )
                raise NotImplementedError(
                    "LocalWorker does not implement 'translation' execution yet"
                )
