from datetime import date as dt_date, datetime as dt_datetime, timedelta as dt_timedelta
from typing import Annotated, Literal, TypeAlias, override
import uuid

from sqlalchemy.engine import url
import webvtt
from pydantic import BaseModel, Field, field_validator


class SeriesData(BaseModel):
    series_id: int
    series_name: str | None = None


class LecturerData(BaseModel):
    lecturer_id: int
    lecturer_name: str | None = None


class LectureData(BaseModel):
    lecture_id: int
    language: str | None = None
    date: dt_date | None = None
    series_id: int | None = None
    semester: str | None = None
    duration: dt_timedelta | None = None
    title: str | None = None
    video_mp4: str | None = None
    desktop_mp4: str | None = None
    podcast_mp4: str | None = None


class VttFile(BaseModel):
    id: int
    lecture_id: int
    language: str
    is_original_lang: bool
    vtt_data: bytes
    txt_data: bytes
    asr_model: str | None = None
    compute_type: str | None = None
    creation_date: dt_datetime | None = None

    @field_validator("vtt_data")
    @classmethod
    def vtt_data_must_be_parseable(cls, v: bytes) -> bytes:
        try:
            webvtt.from_string(v.decode("utf-8"))
        except Exception as e:
            raise ValueError(f"vtt_data is not a valid WebVTT file: {e}")
        return v


class VttLine(BaseModel):
    id: int | None = None
    vtt_file_id: int
    series_id: int
    language: str
    lecturer_ids: list[int]
    line_number: int
    ts_start: int
    ts_end: int
    content: str


class ApiKey(BaseModel):
    api_key: str
    person_name: str | None = None
    person_email: str | None = None
    creation_date: dt_datetime | None = None
    expiration_date: dt_datetime | None = None
    status: str | None = "active"
    id: int | None = None


class BlacklistEntry(BaseModel):
    lecture_id: int
    reason: str | None = None
    times_tried: int = 1
    creation_date: dt_datetime | None = None


class SearchResult(BaseModel):
    vtt_file_id: int
    lecture_id: int
    series_id: int
    series_name: str | None = None
    language: str
    line_number: int
    ts_start: int
    ts_end: int
    content: str
    similarity: float


# --- Job scheduling models ---

Language: TypeAlias = Literal["en", "de", "original"]

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

JobType: TypeAlias = Literal["transcription", "translation"]

class JobResultBase(BaseModel):
    job_id: str
    success: bool
    message: str | None = None
    #batch_size: int | None = None
    #no_in_batch: int | None = None


class TranscriptionParams(BaseModel):
    teletask_id: int
    initial_prompt: str | None = None


class TranscriptionResult(JobResultBase):
    job_type: Literal["transcription"] = "transcription"
    language: str | None = None
    vtt_data: str | None = None
    txt_data: str | None = None

examplejson = {
    "job_id": "tc-1234abcd",
    "success": True,
    "message": "Transcription completed successfully.",
    "language": "en",
    "vtt_data": "WEBVTT\n\n00:00:00.000 --> 00:00:05.000\nHello, world!\n",
    "txt_data": "Hello, world!",
}

class TranslationParams(BaseModel):
    teletask_id: int
    from_language: str
    to_language: str
    additional_prompt: str | None = None


class TranslationResult(JobResultBase):
    job_type: Literal["translation"] = "translation"
    vtt_data: str | None = None
    txt_data: str | None = None


JobResult: TypeAlias = Annotated[
    TranscriptionResult | TranslationResult,
    Field(discriminator="job_type"),
]



class BaseJob(BaseModel):
    id: str = ""
    job_type: JobType  # subclasses must set a default
    status: SchedulerStatuses = "ENQUEUED"
    created_at: dt_datetime = Field(default_factory=dt_datetime.now)


class TranscriptionJob(BaseJob):
    job_type: JobType = "transcription"
    params: TranscriptionParams

    @override
    def model_post_init(self, __context: object) -> None:
        if not self.id:
            self.id: str = f"tc-{self.params.teletask_id}-{uuid.uuid4().hex[:8]}"


class TranslationJob(BaseJob):
    job_type: JobType = "translation"
    params: TranslationParams

    @override
    def model_post_init(self, __context: object) -> None:
        if not self.id:
            self.id: str = (
                f"tl-{self.params.teletask_id}-{self.params.from_language}-{self.params.to_language}-{uuid.uuid4().hex[:8]}"
            )


Job: TypeAlias = TranscriptionJob | TranslationJob

ResourceCategory: TypeAlias = Literal["whisper", "ollama"]
