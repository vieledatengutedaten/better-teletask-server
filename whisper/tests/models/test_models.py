"""
Tests for models/dataclasses.py

These are Pydantic models — no mocking required.
Verifies construction, defaults, validation, and basic behavior.
"""

import pytest
from datetime import date, datetime, timedelta
from app.models import (
    SeriesData,
    LecturerData,
    LectureData,
    VttFile,
    VttLine,
    ApiKey,
    BlacklistEntry,
    SearchResult,
)


class TestSeriesData:
    def test_create_with_all_fields(self):
        s = SeriesData(series_id=1, series_name="Test Series", lecturer_ids=[10, 20])
        assert s.series_id == 1
        assert s.series_name == "Test Series"
        assert s.lecturer_ids == [10, 20]

    def test_defaults(self):
        s = SeriesData(series_id=1)
        assert s.series_name is None
        assert s.lecturer_ids is None


class TestLectureData:
    def test_create_minimal(self):
        ld = LectureData(lecture_id=11401)
        assert ld.lecture_id == 11401
        assert ld.language is None
        assert ld.video_mp4 is None

    def test_create_full(self):
        ld = LectureData(
            lecture_id=11401,
            language="de",
            date=date(2025, 6, 15),
            lecturer_ids=[1, 2],
            series_id=42,
            semester="SS2025",
            duration=timedelta(hours=1, minutes=30),
            title="Intro to Testing",
            video_mp4="https://example.com/video.mp4",
            desktop_mp4="https://example.com/desktop.mp4",
            podcast_mp4="https://example.com/podcast.mp4",
        )
        assert ld.language == "de"
        assert ld.duration == timedelta(hours=1, minutes=30)


class TestApiKey:
    def test_defaults(self):
        ak = ApiKey(api_key="key123")
        assert ak.status == "active"
        assert ak.id is None
        assert ak.person_name is None

    def test_equality(self):
        """Pydantic models support equality by default."""
        a = ApiKey(api_key="x", person_name="Alice")
        b = ApiKey(api_key="x", person_name="Alice")
        assert a == b

    def test_inequality(self):
        a = ApiKey(api_key="x")
        b = ApiKey(api_key="y")
        assert a != b


VALID_VTT = b"WEBVTT\n\n00:00:01.000 --> 00:00:02.000\nHello world\n"


class TestVttFile:
    def test_create(self):
        vf = VttFile(
            id=1,
            lecture_id=11401,
            language="de",
            is_original_lang=True,
            vtt_data=VALID_VTT,
            txt_data=b"hello",
        )
        assert isinstance(vf.vtt_data, bytes)
        assert vf.asr_model is None

    def test_valid_vtt_passes_validation(self):
        vf = VttFile(
            id=1,
            lecture_id=1,
            language="en",
            is_original_lang=True,
            vtt_data=VALID_VTT,
            txt_data=b"Hello world",
        )
        assert vf.vtt_data == VALID_VTT

    def test_invalid_vtt_raises_validation_error(self):
        with pytest.raises(Exception, match="not a valid WebVTT"):
            VttFile(
                id=1,
                lecture_id=1,
                language="en",
                is_original_lang=True,
                vtt_data=b"this is not a vtt file",
                txt_data=b"hello",
            )

    def test_empty_vtt_raises_validation_error(self):
        with pytest.raises(Exception):
            VttFile(
                id=1,
                lecture_id=1,
                language="en",
                is_original_lang=True,
                vtt_data=b"",
                txt_data=b"hello",
            )

    def test_non_utf8_vtt_raises_validation_error(self):
        with pytest.raises(Exception):
            VttFile(
                id=1,
                lecture_id=1,
                language="en",
                is_original_lang=True,
                vtt_data=b"\x80\x81\x82",
                txt_data=b"hello",
            )


class TestVttLine:
    def test_create(self):
        vl = VttLine(
            id=1,
            vtt_file_id=1,
            series_id=42,
            language="de",
            lecturer_ids=[1],
            line_number=1,
            ts_start=0,
            ts_end=5000,
            content="Hello world",
        )
        assert vl.ts_end - vl.ts_start == 5000


class TestSearchResult:
    def test_similarity_range(self):
        sr = SearchResult(
            vtt_file_id=1,
            lecture_id=11401,
            series_id=42,
            series_name="Test",
            language="de",
            line_number=1,
            ts_start=0,
            ts_end=5000,
            content="Hello",
            similarity=0.85,
        )
        assert 0.0 <= sr.similarity <= 1.0


class TestBlacklistEntry:
    def test_defaults(self):
        be = BlacklistEntry(lecture_id=99999)
        assert be.times_tried == 1
        assert be.reason is None
