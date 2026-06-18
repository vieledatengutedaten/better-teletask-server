from datetime import date as dt_date, datetime as dt_datetime, timedelta as dt_timedelta

import webvtt
from pydantic import BaseModel, field_validator


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
