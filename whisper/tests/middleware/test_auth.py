"""
Tests for middleware/auth.py

Builds a minimal FastAPI app with only the AuthMiddleware and a dummy route,
then exercises every branch of the API-key validation logic.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.middleware.auth import AuthMiddleware
from app.models import ApiKey


def create_app():
    app = FastAPI()
    app.add_middleware(AuthMiddleware)

    @app.get("/ping")
    def ping():
        return "pong"

    return app


@pytest.fixture
def client():
    return TestClient(create_app())


def make_api_key(status="active", expiration_date=None):
    return ApiKey(
        api_key="valid-token",
        person_name="Tester",
        person_email="test@example.com",
        creation_date=datetime(2026, 1, 1),
        expiration_date=expiration_date,
        status=status,
    )


class TestNoHeader:
    def test_missing_header_returns_401(self, client):
        response = client.get("/ping")
        assert response.status_code == 401
        assert response.json()["detail"] == "Missing Authorization header"


class TestEmptyToken:
    def test_empty_bearer_returns_401(self, client):
        response = client.get("/ping", headers={"Authorization": "Bearer "})
        assert response.status_code == 401
        assert response.json()["detail"] == "Invalid Authorization header"

    @patch("app.middleware.auth.db.get_api_key_by_key", return_value=None)
    def test_bare_bearer_keyword_falls_through_to_key_lookup(self, mock_db, client):
        """'Bearer' without a trailing space isn't stripped, so the whole
        string is treated as the token and looked up in the DB."""
        response = client.get("/ping", headers={"Authorization": "Bearer"})
        assert response.status_code == 401
        assert response.json()["detail"] == "Invalid API key"


class TestInvalidKey:
    @patch("app.middleware.auth.db.get_api_key_by_key", return_value=None)
    def test_unknown_key_returns_401(self, mock_db, client):
        response = client.get("/ping", headers={"Authorization": "Bearer unknown"})
        assert response.status_code == 401
        assert response.json()["detail"] == "Invalid API key"


class TestRevokedKey:
    @patch("app.middleware.auth.db.get_api_key_by_key")
    def test_revoked_key_returns_403(self, mock_db, client):
        mock_db.return_value = make_api_key(status="revoked")
        response = client.get("/ping", headers={"Authorization": "Bearer valid-token"})
        assert response.status_code == 403
        assert response.json()["detail"] == "API key has been revoked"


class TestExpiredKeyByStatus:
    @patch("app.middleware.auth.db.get_api_key_by_key")
    def test_expired_status_returns_403(self, mock_db, client):
        mock_db.return_value = make_api_key(status="expired")
        response = client.get("/ping", headers={"Authorization": "Bearer valid-token"})
        assert response.status_code == 403
        assert response.json()["detail"] == "API key has expired"


class TestInactiveKey:
    @patch("app.middleware.auth.db.get_api_key_by_key")
    def test_inactive_status_returns_403(self, mock_db, client):
        mock_db.return_value = make_api_key(status="suspended")
        response = client.get("/ping", headers={"Authorization": "Bearer valid-token"})
        assert response.status_code == 403
        assert response.json()["detail"] == "API key is not active"


class TestExpiredKeyByDate:
    @patch("app.middleware.auth.db.get_api_key_by_key")
    def test_past_expiration_date_returns_403(self, mock_db, client):
        mock_db.return_value = make_api_key(
            status="active",
            expiration_date=datetime(2020, 1, 1),
        )
        response = client.get("/ping", headers={"Authorization": "Bearer valid-token"})
        assert response.status_code == 403
        assert response.json()["detail"] == "API key has expired"


class TestValidKey:
    @patch("app.middleware.auth.db.get_api_key_by_key")
    def test_active_key_no_expiry_passes(self, mock_db, client):
        mock_db.return_value = make_api_key(status="active")
        response = client.get("/ping", headers={"Authorization": "Bearer valid-token"})
        assert response.status_code == 200
        assert response.json() == "pong"

    @patch("app.middleware.auth.db.get_api_key_by_key")
    def test_active_key_future_expiry_passes(self, mock_db, client):
        mock_db.return_value = make_api_key(
            status="active",
            expiration_date=datetime.now() + timedelta(days=30),
        )
        response = client.get("/ping", headers={"Authorization": "Bearer valid-token"})
        assert response.status_code == 200
        assert response.json() == "pong"
