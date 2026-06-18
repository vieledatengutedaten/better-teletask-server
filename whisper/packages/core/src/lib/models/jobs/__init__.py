from typing import Annotated, TypeAlias

from pydantic import Field

from lib.models.jobs.base import (
    BaseJob,
    BatchPayload,
    JobPayload,
    JobParamsBase,
    JobResultBase,
    JobType,
    LogLevel,
    ResourceType,
    SchedulerStatuses,
    SLURMWorkerStatuses,
)
from lib.models.jobs.scrape import (
    LectureScrapeData,
    ScrapeLectureDataJob,
    ScrapeLectureDataParams,
    ScrapeLectureDataResult,
)
from lib.models.jobs.transcription import (
    TranscriptionJob,
    TranscriptionParams,
    TranscriptionResult,
)
from lib.models.jobs.translation import (
    Language,
    TARGET_LANGUAGES,
    TranslationJob,
    TranslationParams,
    TranslationResult,
)

Job: TypeAlias = ScrapeLectureDataJob | TranscriptionJob | TranslationJob

JobResult: TypeAlias = Annotated[
    ScrapeLectureDataResult | TranscriptionResult | TranslationResult,
    Field(discriminator="job_type"),
]


__all__ = [
    "BaseJob",
    "BatchPayload",
    "JobPayload",
    "JobParamsBase",
    "Job",
    "JobResult",
    "JobResultBase",
    "JobType",
    "Language",
    "LectureScrapeData",
    "LogLevel",
    "ResourceType",
    "SchedulerStatuses",
    "SLURMWorkerStatuses",
    "ScrapeLectureDataJob",
    "ScrapeLectureDataParams",
    "ScrapeLectureDataResult",
    "TARGET_LANGUAGES",
    "TranscriptionJob",
    "TranscriptionParams",
    "TranscriptionResult",
    "TranslationJob",
    "TranslationParams",
    "TranslationResult",
]
