"""
Shared test fixtures for the Better Teletask test suite.

Key concepts:
- mock_db_conn: provides a fake psycopg2 connection + cursor so that
  db.* modules can be tested without a running PostgreSQL instance.
- mock_config: patches config module values for deterministic tests.
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta


# ---------------------------------------------------------------------------
# Database fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_cursor():
    """A MagicMock that behaves like a psycopg2 cursor."""
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    cursor.fetchall.return_value = []
    return cursor


@pytest.fixture
def mock_conn(mock_cursor):
    """A MagicMock that behaves like a psycopg2 connection.
    
    Calling conn.cursor() returns the shared mock_cursor fixture,
    so you can assert on executed SQL via mock_cursor.execute.
    """
    conn = MagicMock()
    conn.cursor.return_value = mock_cursor
    return conn


@pytest.fixture
def patch_get_connection(mock_conn):
    """Patches get_connection everywhere it was imported.

    Because each db.* module does `from db.connection import get_connection`,
    the name is bound locally in each module. We must patch at every usage site.

    Yields (mock_conn, mock_cursor) for easy assertion access.

    Example:
        def test_something(patch_get_connection):
            conn, cur = patch_get_connection
            cur.fetchall.return_value = [(1,), (2,)]
            result = some_db_function()
            assert result == [1, 2]
    """
    mock_cursor = mock_conn.cursor()
    targets = [
        "db.connection.get_connection",
        "db.api_keys.get_connection",
        "db.lectures.get_connection",
        "db.vtt_files.get_connection",
        "db.vtt_lines.get_connection",
        "db.blacklist.get_connection",
        "db.migrations.get_connection",
    ]
    patches = [patch(t, return_value=mock_conn) for t in targets]
    for p in patches:
        p.start()
    yield mock_conn, mock_cursor
    for p in patches:
        p.stop()


# ---------------------------------------------------------------------------
# Model factory helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_api_key():
    """Returns a dict of kwargs suitable for creating an ApiKey."""
    return {
        "api_key": "abc123def456",
        "person_name": "Test User",
        "person_email": "test@example.com",
        "creation_date": datetime(2026, 1, 1),
        "expiration_date": datetime(2026, 4, 1),
        "status": "active",
    }


@pytest.fixture
def sample_vtt_file():
    """Returns a dict of kwargs suitable for creating a VttFile."""
    return {
        "id": 1,
        "lecture_id": 11401,
        "language": "de",
        "is_original_lang": True,
        "vtt_data": b"WEBVTT\n\n00:00:01.000 --> 00:00:05.000\nHello world",
        "txt_data": b"Hello world",
        "asr_model": "large-v2",
        "compute_type": "float16",
        "creation_date": datetime(2026, 1, 15),
    }


@pytest.fixture
def sample_series_data():
    """Returns a dict of kwargs suitable for creating a SeriesData."""
    return {
        "series_id": 42,
        "series_name": "Introduction to Testing",
        "lecturer_ids": [1, 2],
    }
