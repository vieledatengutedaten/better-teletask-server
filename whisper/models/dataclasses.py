from datetime import date as dt_date, datetime as dt_datetime, timedelta as dt_timedelta
from typing import Optional

from pydantic import BaseModel


class SeriesData(BaseModel):
    series_id: int
    series_name: Optional[str] = None
    lecturer_ids: Optional[list[int]] = None


class LecturerData(BaseModel):
    lecturer_id: int
    lecturer_name: Optional[str] = None


class LectureData(BaseModel):
    lecture_id: int
    language: Optional[str] = None
    date: Optional[dt_date] = None
    lecturer_ids: Optional[list[int]] = None
    series_id: Optional[int] = None
    semester: Optional[str] = None
    duration: Optional[dt_timedelta] = None
    title: Optional[str] = None
    video_mp4: Optional[str] = None
    desktop_mp4: Optional[str] = None
    podcast_mp4: Optional[str] = None


class VttFile(BaseModel):
    id: int
    lecture_id: int
    language: str
    is_original_lang: bool
    vtt_data: bytes
    txt_data: bytes
    asr_model: Optional[str] = None
    compute_type: Optional[str] = None
    creation_date: Optional[dt_datetime] = None


class VttLine(BaseModel):
    id: Optional[int] = None
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
    person_name: Optional[str] = None
    person_email: Optional[str] = None
    creation_date: Optional[dt_datetime] = None
    expiration_date: Optional[dt_datetime] = None
    status: Optional[str] = "active"
    id: Optional[int] = None


class BlacklistEntry(BaseModel):
    lecture_id: int
    reason: Optional[str] = None
    times_tried: int = 1
    creation_date: Optional[dt_datetime] = None


class SearchResult(BaseModel):
    vtt_file_id: int
    lecture_id: int
    series_id: int
    series_name: Optional[str] = None
    language: str
    line_number: int
    ts_start: int
    ts_end: int
    content: str
    similarity: float

