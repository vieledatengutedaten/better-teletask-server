from typing import Literal, override

from pydantic import BaseModel

from lib.models.jobs.base import BaseJob, JobResultBase, JobType
from ...core.config import ASR_MODEL, COMPUTE_TYPE


class TranscriptionParams(BaseModel):
    teletask_id: int
    initial_prompt: str | None = None
    asr_model : str | None = ASR_MODEL
    compute_type: str | None = COMPUTE_TYPE


class TranscriptionResult(JobResultBase):
    job_type: Literal["transcription"] = "transcription"
    language: str | None = None
    vtt_data: str | None = None
    txt_data: str | None = None


class TranscriptionJob(BaseJob):
    job_type: JobType = "transcription"
    params: TranscriptionParams

    @override
    def model_post_init(self, __context: object) -> None:
        if not self.id:
            self.id = f"tc-{self.params.teletask_id}-{self.params.asr_model}-{self.params.compute_type}"
