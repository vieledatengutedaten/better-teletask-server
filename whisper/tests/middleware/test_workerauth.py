"""
Tests for middleware/workerauth.py

Builds a minimal FastAPI app whose worker router carries the
verify_worker_token dependency, alongside an unguarded route, then verifies
that the shared VM_WORKER_AUTH_TOKEN is enforced on the worker routes only.
"""

import pytest
from unittest.mock import patch

from fastapi import APIRouter, Depends, FastAPI
from fastapi.testclient import TestClient

import app.middleware.workerauth as workerauth
from app.middleware.workerauth import verify_worker_token

TOKEN = "shared-worker-token"


def create_app():
    app = FastAPI()

    worker_router = APIRouter()

    @worker_router.get("/ping")
    def worker_ping():
        return "pong"

    schedule_router = APIRouter()

    @schedule_router.get("/ping")
    def schedule_ping():
        return "pong"

    app.include_router(
        worker_router, prefix="/worker", dependencies=[Depends(verify_worker_token)]
    )
    app.include_router(schedule_router, prefix="/schedule")
    return app


@pytest.fixture
def client():
    with patch.object(workerauth, "ENVIRONMENT", "vm"), patch.object(
        workerauth, "VM_WORKER_AUTH_TOKEN", TOKEN
    ):
        yield TestClient(create_app())


class TestNonWorkerRoute:
    def test_non_worker_route_is_unguarded(self, client):
        response = client.get("/schedule/ping")
        assert response.status_code == 200
        assert response.json() == "pong"


class TestNoHeader:
    def test_missing_header_returns_401(self, client):
        response = client.get("/worker/ping")
        assert response.status_code == 401
        assert response.json()["detail"] == "Missing Authorization header"


class TestEmptyToken:
    def test_empty_bearer_returns_401(self, client):
        response = client.get("/worker/ping", headers={"Authorization": "Bearer "})
        assert response.status_code == 401
        assert response.json()["detail"] == "Invalid Authorization header"


class TestInvalidToken:
    def test_wrong_token_returns_401(self, client):
        response = client.get("/worker/ping", headers={"Authorization": "Bearer wrong"})
        assert response.status_code == 401
        assert response.json()["detail"] == "Invalid worker token"


class TestValidToken:
    def test_correct_token_passes(self, client):
        response = client.get(
            "/worker/ping", headers={"Authorization": f"Bearer {TOKEN}"}
        )
        assert response.status_code == 200
        assert response.json() == "pong"


class TestTokenNotConfigured:
    def test_unset_token_returns_500(self):
        with patch.object(workerauth, "ENVIRONMENT", "vm"), patch.object(
            workerauth, "VM_WORKER_AUTH_TOKEN", None
        ):
            response = TestClient(create_app()).get(
                "/worker/ping", headers={"Authorization": f"Bearer {TOKEN}"}
            )
            assert response.status_code == 500
            assert response.json()["detail"] == "Worker auth token is not configured"
