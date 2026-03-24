"""
Tests for db/vtt_lines.py — search_vtt_lines

Captures the current behaviour of search_vtt_lines so we can safely
refactor the implementation from raw text() SQL to SQLAlchemy ORM.

Strategy:
-   We mock the SQLAlchemy session (via the shared patch_get_session fixture)
    and use `session.execute.call_args` to inspect the generated SQL text
    and bound parameters.
-   We also verify that the returned list[SearchResult] is built correctly
    from the row tuples the database would return.
"""

from unittest.mock import MagicMock

import pytest

from models import SearchResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_row(
    vtt_file_id=1,
    lecture_id=11401,
    series_id=42,
    series_name="Intro to Testing",
    language="de",
    line_number=7,
    ts_start=1000,
    ts_end=5000,
    content="Hallo Welt",
    similarity=0.85,
):
    """Return a tuple that mimics a raw DB row from the search query."""
    return (
        vtt_file_id,
        lecture_id,
        series_id,
        series_name,
        language,
        line_number,
        ts_start,
        ts_end,
        content,
        similarity,
    )


def _get_sql_and_params(mock_session):
    """Extract the SQL string and params dict from the session.execute call."""
    call_args = mock_session.execute.call_args
    sql_text = call_args[0][0]  # first positional arg: text() object
    params = call_args[0][1]  # second positional arg: params dict
    return str(sql_text), params


# ---------------------------------------------------------------------------
# Tests: result mapping
# ---------------------------------------------------------------------------


class TestSearchResultMapping:
    """Verify that raw DB rows are correctly mapped to SearchResult objects."""

    def test_single_result_mapped_correctly(self, patch_get_session):
        session, result_proxy = patch_get_session
        result_proxy.all.return_value = [_make_row()]

        from db.vtt_lines import search_vtt_lines

        results = search_vtt_lines("Hallo")

        assert len(results) == 1
        r = results[0]
        assert isinstance(r, SearchResult)
        assert r.vtt_file_id == 1
        assert r.lecture_id == 11401
        assert r.series_id == 42
        assert r.series_name == "Intro to Testing"
        assert r.language == "de"
        assert r.line_number == 7
        assert r.ts_start == 1000
        assert r.ts_end == 5000
        assert r.content == "Hallo Welt"
        assert r.similarity == 0.85

    def test_multiple_results(self, patch_get_session):
        session, result_proxy = patch_get_session
        result_proxy.all.return_value = [
            _make_row(vtt_file_id=1, content="alpha", similarity=0.9),
            _make_row(vtt_file_id=2, content="beta", similarity=0.7),
            _make_row(vtt_file_id=3, content="gamma", similarity=0.5),
        ]

        from db.vtt_lines import search_vtt_lines

        results = search_vtt_lines("test")

        assert len(results) == 3
        assert results[0].content == "alpha"
        assert results[1].content == "beta"
        assert results[2].content == "gamma"

    def test_empty_results(self, patch_get_session):
        session, result_proxy = patch_get_session
        result_proxy.all.return_value = []

        from db.vtt_lines import search_vtt_lines

        results = search_vtt_lines("nothing")

        assert results == []


# ---------------------------------------------------------------------------
# Tests: SQL generation & parameters (query only)
# ---------------------------------------------------------------------------


class TestSearchQueryOnly:
    """When called with just a query string (no optional filters)."""

    def test_base_params_are_passed(self, patch_get_session):
        session, result_proxy = patch_get_session
        result_proxy.all.return_value = []

        from db.vtt_lines import search_vtt_lines

        search_vtt_lines("hello world")

        _, params = _get_sql_and_params(session)
        assert params["query_where"] == "hello world"
        assert params["query_select"] == "hello world"
        assert params["threshold"] == 0.15
        assert params["limit"] == 20

    def test_sql_contains_similarity_filter(self, patch_get_session):
        session, result_proxy = patch_get_session
        result_proxy.all.return_value = []

        from db.vtt_lines import search_vtt_lines

        search_vtt_lines("hello")

        sql, _ = _get_sql_and_params(session)
        assert "similarity(vl.content, :query_where) >= :threshold" in sql

    def test_sql_contains_joins(self, patch_get_session):
        session, result_proxy = patch_get_session
        result_proxy.all.return_value = []

        from db.vtt_lines import search_vtt_lines

        search_vtt_lines("hello")

        sql, _ = _get_sql_and_params(session)
        assert "JOIN vtt_files vf" in sql
        assert "JOIN series_data sd" in sql

    def test_sql_orders_by_similarity_desc(self, patch_get_session):
        session, result_proxy = patch_get_session
        result_proxy.all.return_value = []

        from db.vtt_lines import search_vtt_lines

        search_vtt_lines("hello")

        sql, _ = _get_sql_and_params(session)
        assert "ORDER BY similarity DESC" in sql

    def test_sql_has_limit(self, patch_get_session):
        session, result_proxy = patch_get_session
        result_proxy.all.return_value = []

        from db.vtt_lines import search_vtt_lines

        search_vtt_lines("hello")

        sql, _ = _get_sql_and_params(session)
        assert "LIMIT :limit" in sql

    def test_no_optional_filters_in_where(self, patch_get_session):
        """Without optional args, the WHERE should only have the similarity clause."""
        session, result_proxy = patch_get_session
        result_proxy.all.return_value = []

        from db.vtt_lines import search_vtt_lines

        search_vtt_lines("hello")

        sql, params = _get_sql_and_params(session)
        # Should NOT contain optional filter fragments
        assert "vl.series_id = :series_id" not in sql
        assert "vl.language = :language" not in sql
        assert ":lecturer_id = ANY" not in sql
        assert "vf.lecture_id = :lecture_id" not in sql
        # Params should not have optional keys
        assert "series_id" not in params
        assert "language" not in params
        assert "lecturer_id" not in params
        assert "lecture_id" not in params


# ---------------------------------------------------------------------------
# Tests: custom threshold and limit
# ---------------------------------------------------------------------------


class TestCustomThresholdAndLimit:
    def test_custom_threshold(self, patch_get_session):
        session, result_proxy = patch_get_session
        result_proxy.all.return_value = []

        from db.vtt_lines import search_vtt_lines

        search_vtt_lines("test", threshold=0.5)

        _, params = _get_sql_and_params(session)
        assert params["threshold"] == 0.5

    def test_custom_limit(self, patch_get_session):
        session, result_proxy = patch_get_session
        result_proxy.all.return_value = []

        from db.vtt_lines import search_vtt_lines

        search_vtt_lines("test", limit=100)

        _, params = _get_sql_and_params(session)
        assert params["limit"] == 100


# ---------------------------------------------------------------------------
# Tests: optional filters
# ---------------------------------------------------------------------------


class TestSeriesIdFilter:
    def test_series_id_appears_in_sql_and_params(self, patch_get_session):
        session, result_proxy = patch_get_session
        result_proxy.all.return_value = []

        from db.vtt_lines import search_vtt_lines

        search_vtt_lines("test", series_id=42)

        sql, params = _get_sql_and_params(session)
        assert "vl.series_id = :series_id" in sql
        assert params["series_id"] == 42


class TestLanguageFilter:
    def test_language_appears_in_sql_and_params(self, patch_get_session):
        session, result_proxy = patch_get_session
        result_proxy.all.return_value = []

        from db.vtt_lines import search_vtt_lines

        search_vtt_lines("test", language="en")

        sql, params = _get_sql_and_params(session)
        assert "vl.language = :language" in sql
        assert params["language"] == "en"


class TestLecturerIdFilter:
    def test_lecturer_id_appears_in_sql_and_params(self, patch_get_session):
        session, result_proxy = patch_get_session
        result_proxy.all.return_value = []

        from db.vtt_lines import search_vtt_lines

        search_vtt_lines("test", lecturer_id=7)

        sql, params = _get_sql_and_params(session)
        assert ":lecturer_id = ANY(vl.lecturer_ids)" in sql
        assert params["lecturer_id"] == 7


class TestLectureIdFilter:
    def test_lecture_id_appears_in_sql_and_params(self, patch_get_session):
        session, result_proxy = patch_get_session
        result_proxy.all.return_value = []

        from db.vtt_lines import search_vtt_lines

        search_vtt_lines("test", lecture_id=11401)

        sql, params = _get_sql_and_params(session)
        assert "vf.lecture_id = :lecture_id" in sql
        assert params["lecture_id"] == 11401


class TestAllFiltersCombined:
    def test_all_filters_present(self, patch_get_session):
        session, result_proxy = patch_get_session
        result_proxy.all.return_value = [_make_row()]

        from db.vtt_lines import search_vtt_lines

        results = search_vtt_lines(
            "test",
            series_id=42,
            language="de",
            lecturer_id=7,
            lecture_id=11401,
            threshold=0.3,
            limit=5,
        )

        sql, params = _get_sql_and_params(session)

        # All filter fragments should be present
        assert "similarity(vl.content, :query_where) >= :threshold" in sql
        assert "vl.series_id = :series_id" in sql
        assert "vl.language = :language" in sql
        assert ":lecturer_id = ANY(vl.lecturer_ids)" in sql
        assert "vf.lecture_id = :lecture_id" in sql

        # All params should match
        assert params["query_where"] == "test"
        assert params["query_select"] == "test"
        assert params["series_id"] == 42
        assert params["language"] == "de"
        assert params["lecturer_id"] == 7
        assert params["lecture_id"] == 11401
        assert params["threshold"] == 0.3
        assert params["limit"] == 5

        # Result mapping still works
        assert len(results) == 1
        assert isinstance(results[0], SearchResult)


# ---------------------------------------------------------------------------
# Tests: session lifecycle
# ---------------------------------------------------------------------------


class TestSessionLifecycle:
    def test_session_is_committed_and_closed(self, patch_get_session):
        session, result_proxy = patch_get_session
        result_proxy.all.return_value = []

        from db.vtt_lines import search_vtt_lines

        search_vtt_lines("test")

        session.commit.assert_called_once()
        session.close.assert_called_once()

    def test_session_rolls_back_on_error(self, patch_get_session):
        session, result_proxy = patch_get_session
        session.execute.side_effect = RuntimeError("DB error")

        from db.vtt_lines import search_vtt_lines

        with pytest.raises(RuntimeError, match="DB error"):
            search_vtt_lines("test")

        session.rollback.assert_called_once()
        session.close.assert_called_once()
