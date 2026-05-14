from pydantic import BaseModel

from .jobs.base import LogLevel, SchedulerStatuses


class StatusUpdate(BaseModel):
    status: SchedulerStatuses


class LogMessage(BaseModel):
    message: str
    level: LogLevel = "info"


class FailureReport(BaseModel):
    reason: str
