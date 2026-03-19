"""
Tests for db/api_keys.py

Demonstrates how to mock the database connection so you can test 
SQL logic without a running PostgreSQL instance.
"""

import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

from models import ApiKey


class TestAddApiKey:
    def test_inserts_correctly(self, patch_get_connection):
        """Verify the INSERT SQL and parameters are correct."""
        conn, cur = patch_get_connection

        from db.api_keys import add_api_key
        add_api_key("testkey123", "Alice", "alice@example.com")

        cur.execute.assert_called_once()
        sql, params = cur.execute.call_args[0]
        assert "INSERT INTO api_keys" in sql
        assert params == ("testkey123", "Alice", "alice@example.com")
        conn.commit.assert_called_once()

    def test_closes_connection_on_success(self, patch_get_connection):
        conn, cur = patch_get_connection

        from db.api_keys import add_api_key
        add_api_key("k", "n", "e")

        cur.close.assert_called_once()
        conn.close.assert_called_once()

    def test_closes_connection_on_error(self, patch_get_connection):
        """Even if execute raises, the connection should be closed."""
        conn, cur = patch_get_connection
        cur.execute.side_effect = Exception("DB down")

        from db.api_keys import add_api_key
        add_api_key("k", "n", "e")

        cur.close.assert_called_once()
        conn.close.assert_called_once()


class TestGetApiKeyByKey:
    def test_returns_api_key_when_found(self, patch_get_connection):
        conn, cur = patch_get_connection
        cur.fetchone.return_value = (
            "key123", "Bob", "bob@example.com",
            datetime(2026, 1, 1), datetime(2026, 4, 1), "active"
        )

        from db.api_keys import get_api_key_by_key
        result = get_api_key_by_key("key123")

        assert isinstance(result, ApiKey)
        assert result.api_key == "key123"
        assert result.person_name == "Bob"
        assert result.status == "active"

    def test_returns_none_when_not_found(self, patch_get_connection):
        conn, cur = patch_get_connection
        cur.fetchone.return_value = None

        from db.api_keys import get_api_key_by_key
        result = get_api_key_by_key("nonexistent")

        assert result is None


class TestGetAllApiKeys:
    def test_returns_list_of_api_keys(self, patch_get_connection):
        conn, cur = patch_get_connection
        cur.fetchall.return_value = [
            ("k1", "Alice", "a@a.com", datetime(2026, 1, 1), datetime(2026, 4, 1), "active"),
            ("k2", "Bob", "b@b.com", datetime(2026, 2, 1), datetime(2026, 5, 1), "active"),
        ]

        from db.api_keys import get_all_api_keys
        result = get_all_api_keys()

        assert len(result) == 2
        assert result[0].person_name == "Alice"
        assert result[1].person_name == "Bob"

    def test_returns_empty_list_when_none(self, patch_get_connection):
        conn, cur = patch_get_connection
        cur.fetchall.return_value = []

        from db.api_keys import get_all_api_keys
        result = get_all_api_keys()

        assert result == []


class TestRemoveApiKey:
    def test_executes_delete(self, patch_get_connection):
        conn, cur = patch_get_connection

        from db.api_keys import remove_api_key
        remove_api_key("key_to_delete")

        sql, params = cur.execute.call_args[0]
        assert "DELETE" in sql.upper()
        assert params == ("key_to_delete",)
        conn.commit.assert_called_once()
