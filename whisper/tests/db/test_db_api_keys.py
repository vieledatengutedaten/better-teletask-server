"""
Tests for db/api_keys.py

Demonstrates how to mock SQLAlchemy session calls so you can test
database logic without a running PostgreSQL instance.
"""

from datetime import datetime
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import SQLAlchemyError

from app.models import ApiKey


class TestAddApiKey:
    def test_inserts_correctly(self, patch_get_session):
        """Verify SQLAlchemy executes an INSERT and commits."""
        session, _ = patch_get_session

        from app.db.api_keys import add_api_key

        add_api_key("testkey123", "Alice", "alice@example.com")

        session.execute.assert_called_once()
        stmt = session.execute.call_args[0][0]
        assert "INSERT INTO api_keys" in str(stmt)
        assert stmt.compile().params["api_key"] == "testkey123"
        assert stmt.compile().params["person_name"] == "Alice"
        assert stmt.compile().params["person_email"] == "alice@example.com"
        session.commit.assert_called_once()

    def test_closes_connection_on_success(self, patch_get_session):
        session, _ = patch_get_session

        from app.db.api_keys import add_api_key

        add_api_key("k", "n", "e")

        session.close.assert_called_once()

    def test_closes_connection_on_error(self, patch_get_session):
        """Even if execute raises, the session should be closed."""
        session, _ = patch_get_session
        session.execute.side_effect = SQLAlchemyError("DB down")

        from app.db.api_keys import add_api_key

        with pytest.raises(SQLAlchemyError, match="DB down"):
            add_api_key("k", "n", "e")

        session.rollback.assert_called_once()
        session.close.assert_called_once()


class TestGetApiKeyByKey:
    def test_returns_api_key_when_found(self, patch_get_session):
        session, result_proxy = patch_get_session
        result_proxy.scalar_one_or_none.return_value = SimpleNamespace(
            id=1,
            api_key="key123",
            person_name="Bob",
            person_email="bob@example.com",
            creation_date=datetime(2026, 1, 1),
            expiration_date=datetime(2026, 4, 1),
            status="active",
        )

        from app.db.api_keys import get_api_key_by_key

        result = get_api_key_by_key("key123")

        assert isinstance(result, ApiKey)
        assert result.api_key == "key123"
        assert result.person_name == "Bob"
        assert result.status == "active"
        session.close.assert_called_once()

    def test_returns_none_when_not_found(self, patch_get_session):
        session, result_proxy = patch_get_session
        result_proxy.scalar_one_or_none.return_value = None

        from app.db.api_keys import get_api_key_by_key

        result = get_api_key_by_key("nonexistent")

        assert result is None
        session.close.assert_called_once()


class TestGetAllApiKeys:
    def test_returns_list_of_api_keys(self, patch_get_session):
        session, result_proxy = patch_get_session
        result_proxy.scalars.return_value.all.return_value = [
            SimpleNamespace(
                id=1,
                api_key="k1",
                person_name="Alice",
                person_email="a@a.com",
                creation_date=datetime(2026, 1, 1),
                expiration_date=datetime(2026, 4, 1),
                status="active",
            ),
            SimpleNamespace(
                id=2,
                api_key="k2",
                person_name="Bob",
                person_email="b@b.com",
                creation_date=datetime(2026, 2, 1),
                expiration_date=datetime(2026, 5, 1),
                status="active",
            ),
        ]

        from app.db.api_keys import get_all_api_keys

        result = get_all_api_keys()

        assert len(result) == 2
        assert result[0].person_name == "Alice"
        assert result[1].person_name == "Bob"
        session.close.assert_called_once()

    def test_returns_empty_list_when_none(self, patch_get_session):
        session, result_proxy = patch_get_session
        result_proxy.scalars.return_value.all.return_value = []

        from app.db.api_keys import get_all_api_keys

        result = get_all_api_keys()

        assert result == []
        session.close.assert_called_once()


class TestRemoveApiKey:
    def test_executes_delete(self, patch_get_session):
        session, _ = patch_get_session

        from app.db.api_keys import remove_api_key

        remove_api_key("key_to_delete")

        session.execute.assert_called_once()
        stmt = session.execute.call_args[0][0]
        assert "DELETE FROM api_keys" in str(stmt)
        session.commit.assert_called_once()
