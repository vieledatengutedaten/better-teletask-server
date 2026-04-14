from datetime import datetime as dt_datetime
from typing import Literal, TypeAlias

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


class JobResultBase(BaseModel):
    job_id: str
    success: bool
    message: str | None = None


class BaseJob(BaseModel):
    id: str = ""
    job_type: JobType  # subclasses must set a default
    status: SchedulerStatuses = "ENQUEUED"
    priority: int = 0
    created_at: dt_datetime = Field(default_factory=dt_datetime.now)
