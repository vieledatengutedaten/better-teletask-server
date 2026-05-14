from datetime import datetime as dt_datetime
from typing import Generic, Literal, TypeAlias, TypeVar

from pydantic import BaseModel, Field

SLURMWorkerStatuses: TypeAlias = Literal[
    "PENDING",
    "RUNNING",
    "PREEMPTED",
    "DEADLINE",
    "TIMEOUT",
    "SUSPENDED",
    "COMPLETED",
    "CANCELLED",
    "FAILED",
]

LogLevel: TypeAlias = Literal["debug", "info", "warning", "error", "critical"]

SchedulerStatuses: TypeAlias = Literal["RUNNING", "COMPLETED", "FAILED", "ENQUEUED"]

JobType: TypeAlias = Literal["scrape_lecture_data", "transcription", "translation"]

ResourceType: TypeAlias = Literal["whisper", "ollama", "cpu"]


class JobParamsBase(BaseModel):
    pass


JobParamsT = TypeVar("JobParamsT", bound=JobParamsBase)


class JobPayload(BaseModel, Generic[JobParamsT]):
    job_id: str
    params: JobParamsT


class BatchPayload(BaseModel, Generic[JobParamsT]):
    worker_id: str
    job_type: JobType
    jobs: list[JobPayload[JobParamsT]]


class JobResultBase(BaseModel):
    job_id: str
    success: bool
    message: str | None = None


class BaseJob(BaseModel):
    id: str = ""
    worker_id: str | None = None
    job_type: JobType  # subclasses must set a default
    params: JobParamsBase
    status: SchedulerStatuses = "ENQUEUED"
    priority: int = 0
    created_at: dt_datetime = Field(default_factory=dt_datetime.now)
