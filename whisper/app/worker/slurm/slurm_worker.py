from collections.abc import Sequence
from typing import override

from pydantic import BaseModel


from app.worker.worker import Worker
from lib.models.jobs import (
    JobType,
    JobParamsBase,
    JobPayload,
    ScrapeLectureDataParams,
    TranscriptionParams,
    TranslationParams,
)
from app.worker.utils import require_params


class DumpJobPayload(BaseModel):
    worker_id: str
    job_type: JobType
    jobs: Sequence[dict]


def build_json(
    worker_id: str,
    job_type: JobType,
    payloads: Sequence[JobPayload[JobParamsBase]],
) -> str:
    return DumpJobPayload(
        worker_id=worker_id,
        job_type=job_type,
        jobs=[
            {"job_id": p.job_id, "params": p.params.model_dump()} for p in payloads
        ],
    ).model_dump_json()


class SlurmWorker(Worker):

    @override
    async def run(
        self,
        worker_id: str,
        job_type: JobType,
        payloads: Sequence[JobPayload[JobParamsBase]],
    ) -> None:
        match job_type:
            case "scrape_lecture_data":
                _ = require_params(
                    [payload.params for payload in payloads],
                    ScrapeLectureDataParams,
                    job_type,
                )
                print(build_json(worker_id, job_type, payloads))
                # dispatch
                # raise NotImplementedError("SlurmWorker does not implement 'scrape_lecture_data' execution yet")
            case "transcription":
                _ = require_params(
                    [payload.params for payload in payloads],
                    TranscriptionParams,
                    job_type,
                )
                print(build_json(worker_id, job_type, payloads))
                raise NotImplementedError("SlurmWorker does not implement 'transcription' execution yet")
            case "translation":
                _ = require_params(
                    [payload.params for payload in payloads],
                    TranslationParams,
                    job_type,
                )
                raise NotImplementedError(
                    "SlurmWorker does not implement 'translation' execution yet"
                )
