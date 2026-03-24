"""
Shared test fixtures for the Better Teletask test suite.

Key concepts:
- patch_get_session: provides a fake SQLAlchemy Session so that
    db.* modules can be tested without a running PostgreSQL instance.
- mock_config: patches config module values for deterministic tests.
"""

import pytest
from contextlib import contextmanager
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Database fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_result():
    """A MagicMock that behaves like a SQLAlchemy Result."""
    result = MagicMock()
    result.scalar_one_or_none.return_value = None
    result.scalar_one.return_value = 0
    result.first.return_value = None
    result.all.return_value = []
    result.scalars.return_value.all.return_value = []
    return result


@pytest.fixture
def mock_conn(mock_result):
    """A MagicMock that behaves like a SQLAlchemy Session."""
    session = MagicMock()
    session.execute.return_value = mock_result
    return session


@pytest.fixture
def patch_get_session(mock_conn):
    """Patches get_session everywhere it was imported.

    Because each db.* module does `from db.connection import get_session`,
    the name is bound locally in each module. We must patch at every usage site.

    Yields (mock_session, mock_result) for easy assertion access.

    Example:
        def test_something(patch_get_session):
            session, result = patch_get_session
            result.all.return_value = [(1,), (2,)]
            result = some_db_function()
            assert result == [1, 2]
    """
    targets = [
        "db.connection.get_session",
        "db.api_keys.get_session",
        "db.lectures.get_session",
        "db.vtt_files.get_session",
        "db.vtt_lines.get_session",
        "db.blacklist.get_session",
    ]

    @contextmanager
    def fake_get_session():
        try:
            yield mock_conn
            mock_conn.commit()
        except Exception:
            mock_conn.rollback()
            raise
        finally:
            mock_conn.close()

    patches = [patch(t, side_effect=fake_get_session) for t in targets]
    for p in patches:
        p.start()
    yield mock_conn, mock_conn.execute.return_value
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
