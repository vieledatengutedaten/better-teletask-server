"""
Tests for CORS middleware behavior.

Builds a minimal FastAPI app with CORSMiddleware configured to allow
only https://www.tele-task.de, then verifies that CORS headers are
set correctly for allowed/disallowed origins and preflight requests.
"""

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.middleware.cors import CORSMiddleware

ALLOWED_ORIGIN = "https://www.tele-task.de"
DISALLOWED_ORIGIN = "https://www.charliekirk.com/"


def create_app(origins=None):
    app = FastAPI()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins or [ALLOWED_ORIGIN],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/ping")
    def ping():
        return "pong"

    return app


@pytest.fixture
def client():
    return TestClient(create_app())


class TestAllowedOrigin:
    def test_response_includes_cors_header(self, client):
        response = client.get("/ping", headers={"Origin": ALLOWED_ORIGIN})
        assert response.headers["access-control-allow-origin"] == ALLOWED_ORIGIN

    def test_response_body_is_passed_through(self, client):
        response = client.get("/ping", headers={"Origin": ALLOWED_ORIGIN})
        assert response.status_code == 200
        assert response.json() == "pong"


class TestDisallowedOrigin:
    def test_no_cors_header_for_disallowed_origin(self, client):
        response = client.get("/ping", headers={"Origin": DISALLOWED_ORIGIN})
        assert "access-control-allow-origin" not in response.headers

    def test_response_still_succeeds(self, client):
        """Server still responds — CORS enforcement is on the browser side."""
        response = client.get("/ping", headers={"Origin": DISALLOWED_ORIGIN})
        assert response.status_code == 200


class TestNoOriginHeader:
    def test_same_origin_request_has_no_cors_header(self, client):
        response = client.get("/ping")
        assert "access-control-allow-origin" not in response.headers

    def test_same_origin_request_passes(self, client):
        response = client.get("/ping")
        assert response.status_code == 200


class TestPreflightAllowed:
    def test_preflight_returns_200(self, client):
        response = client.options(
            "/ping",
            headers={
                "Origin": ALLOWED_ORIGIN,
                "Access-Control-Request-Method": "GET",
            },
        )
        assert response.status_code == 200

    def test_preflight_includes_allow_origin(self, client):
        response = client.options(
            "/ping",
            headers={
                "Origin": ALLOWED_ORIGIN,
                "Access-Control-Request-Method": "DELETE",
            },
        )
        assert response.headers["access-control-allow-origin"] == ALLOWED_ORIGIN

    def test_preflight_includes_allow_methods(self, client):
        response = client.options(
            "/ping",
            headers={
                "Origin": ALLOWED_ORIGIN,
                "Access-Control-Request-Method": "POST",
            },
        )
        assert "access-control-allow-methods" in response.headers

    def test_preflight_custom_header_allowed(self, client):
        response = client.options(
            "/ping",
            headers={
                "Origin": ALLOWED_ORIGIN,
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "Authorization",
            },
        )
        assert response.status_code == 200
        assert "authorization" in response.headers.get(
            "access-control-allow-headers", ""
        ).lower()


class TestPreflightDisallowed:
    def test_preflight_disallowed_origin_no_cors_header(self, client):
        response = client.options(
            "/ping",
            headers={
                "Origin": DISALLOWED_ORIGIN,
                "Access-Control-Request-Method": "GET",
            },
        )
        assert "access-control-allow-origin" not in response.headers


class TestWildcardOrigin:
    def test_wildcard_allows_any_origin(self):
        app = create_app(origins=["*"])
        wildcard_client = TestClient(app)
        response = wildcard_client.get(
            "/ping", headers={"Origin": "https://anything.example.com"}
        )
        assert response.headers["access-control-allow-origin"] == "*"
