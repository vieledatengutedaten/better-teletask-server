from typing import Literal, override

from pydantic import BaseModel

from lib.models.jobs.base import BaseJob, JobResultBase, JobType


class TranscriptionParams(BaseModel):
    teletask_id: int
    initial_prompt: str | None = None


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
            self.id = f"tc-{self.params.teletask_id}"
