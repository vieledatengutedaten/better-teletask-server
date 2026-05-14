from collections.abc import Sequence
from typing import override

from pydantic import BaseModel
from pytest import param


from app.worker.worker import Worker
from lib.models.jobs import (
    JobType,
    JobParamsBase,
    ScrapeLectureDataParams,
    TranscriptionParams,
    TranslationParams,
)
from app.worker.utils import require_params


class JobPayload(BaseModel):
    worker_id: str
    job_type: JobType
    params: Sequence[dict]


def build_json(
    worker_id: str, job_type: JobType, params: Sequence[JobParamsBase]
) -> str:
    return JobPayload(
        worker_id=worker_id, job_type=job_type, params=[p.model_dump() for p in params]
    ).model_dump_json()


class SlurmWorker(Worker):

    @override
    async def run(
        self,
        worker_id: str,
        job_type: JobType,
        params: Sequence[JobParamsBase],
    ) -> None:
        match job_type:
            case "scrape_lecture_data":
                params = require_params(params, ScrapeLectureDataParams, job_type)
                print(build_json(worker_id, job_type, params))
                # raise NotImplementedError("SlurmWorker does not implement 'scrape_lecture_data' execution yet")
            case "transcription":
                _ = require_params(params, TranscriptionParams, job_type)
                raise NotImplementedError(
                    "SlurmWorker does not implement 'transcription' execution yet"
                )
            case "translation":
                _ = require_params(params, TranslationParams, job_type)
                raise NotImplementedError(
                    "SlurmWorker does not implement 'translation' execution yet"
                )
