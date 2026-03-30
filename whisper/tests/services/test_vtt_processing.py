"""
Tests for services/vtt_processing.py

timestamp_to_ms is a pure function — test it directly.
save_vtt_lines requires DB calls — mock those.
"""

import pytest
from unittest.mock import patch, MagicMock

from app.services.vtt_processing import timestamp_to_ms


class TestTimestampToMs:
    """Pure function — no mocking needed."""

    def test_hours_minutes_seconds_ms(self):
        assert timestamp_to_ms("01:02:03.456") == 3723456

    def test_zero(self):
        assert timestamp_to_ms("00:00:00.000") == 0

    def test_minutes_seconds_only(self):
        # Some VTT timestamps omit hours: "02:30.500"
        assert timestamp_to_ms("02:30.500") == 150500

    def test_one_hour_exactly(self):
        assert timestamp_to_ms("01:00:00.000") == 3600000

    def test_subsecond_precision(self):
        assert timestamp_to_ms("00:00:00.001") == 1
        assert timestamp_to_ms("00:00:00.999") == 999

    def test_large_timestamp(self):
        # 2h 30m 45s 123ms
        assert timestamp_to_ms("02:30:45.123") == 9045123


class TestSaveVttLines:
    """
    save_vtt_lines calls:
      - get_vtt_file_by_id (db)
      - get_series_of_vtt_file (db)
      - get_lecturer_ids_of_lecture (db)
      - webvtt.from_string (pure)
      - bulk_insert_vtt_lines (db)

    We mock all DB calls and provide a valid VTT string.
    """

    @patch("app.services.vtt_processing.bulk_insert_vtt_lines")
    @patch("app.services.vtt_processing.get_lecturer_ids_of_lecture")
    @patch("app.services.vtt_processing.get_series_of_vtt_file")
    @patch("app.services.vtt_processing.get_vtt_file_by_id")
    def test_parses_vtt_and_inserts_lines(
        self, mock_get_vtt, mock_get_series, mock_get_lecturers, mock_bulk_insert
    ):
        from app.models import VttFile, SeriesData

        vtt_content = (
            "WEBVTT\n\n"
            "00:00:01.000 --> 00:00:05.000\n"
            "Hello world\n\n"
            "00:00:06.000 --> 00:00:10.000\n"
            "Second line\n"
        )
        mock_get_vtt.return_value = VttFile(
            id=1,
            lecture_id=11401,
            language="de",
            is_original_lang=True,
            vtt_data=vtt_content.encode("utf-8"),
            txt_data=b"Hello world\nSecond line",
        )
        mock_get_series.return_value = SeriesData(
            series_id=42, series_name="Test Series"
        )
        mock_get_lecturers.return_value = [1, 2]

        from app.services.vtt_processing import save_vtt_lines

        save_vtt_lines(1)

        # Should have called bulk_insert with 2 VttLine objects
        mock_bulk_insert.assert_called_once()
        lines = mock_bulk_insert.call_args[0][0]
        assert len(lines) == 2
        assert lines[0].content == "Hello world"
        assert lines[0].line_number == 1
        assert lines[0].series_id == 42
        assert lines[0].lecturer_ids == [1, 2]
        assert lines[1].content == "Second line"
        assert lines[1].line_number == 2

    @patch("app.services.vtt_processing.bulk_insert_vtt_lines")
    @patch("app.services.vtt_processing.get_lecturer_ids_of_lecture")
    @patch("app.services.vtt_processing.get_series_of_vtt_file")
    @patch("app.services.vtt_processing.get_vtt_file_by_id")
    def test_returns_early_if_vtt_not_found(
        self, mock_get_vtt, mock_get_series, mock_get_lecturers, mock_bulk_insert
    ):
        mock_get_vtt.return_value = None

        from app.services.vtt_processing import save_vtt_lines

        save_vtt_lines(999)

        mock_bulk_insert.assert_not_called()
