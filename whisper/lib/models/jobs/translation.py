from typing import Literal, TypeAlias, override

from pydantic import BaseModel

from lib.models.jobs.base import BaseJob, JobResultBase, JobType


Language: TypeAlias = Literal["en", "de", "original"]

TARGET_LANGUAGES: list[Language] = ["en", "de"]


class TranslationParams(BaseModel):
    teletask_id: int
    from_language: str
    to_language: str
    additional_prompt: str | None = None


class TranslationResult(JobResultBase):
    job_type: Literal["translation"] = "translation"
    vtt_data: str | None = None
    txt_data: str | None = None


class TranslationJob(BaseJob):
    job_type: JobType = "translation"
    params: TranslationParams

    @override
    def model_post_init(self, __context: object) -> None:
        if not self.id:
            self.id = (
                f"tl-{self.params.teletask_id}-{self.params.from_language}-{self.params.to_language}"
            )
