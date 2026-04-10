"""
Tests for api/scheduling_routes.py

Uses FastAPI's TestClient to make HTTP calls against schedule_router.
Queue and scheduler dependencies are mocked.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.scheduling_routes import schedule_router
from app.models.dataclasses import TranscriptionJob, TranscriptionParams


@pytest.fixture
def app():
    app = FastAPI()
    app.include_router(schedule_router)
    return app


@pytest.fixture
def client(app):
    return TestClient(app)


class TestPing:
    def test_ping_returns_pong(self, client):
        response = client.get("/ping")
        assert response.status_code == 200
        assert response.json() == "pong"


class TestGetQueues:
    @patch("app.api.scheduling_routes.queue_manager")
    @patch("app.api.scheduling_routes.get_scheduler")
    def test_returns_queued_and_active_jobs(self, mock_get_scheduler, mock_queue_manager, client):
        whisper_job = TranscriptionJob(params=TranscriptionParams(teletask_id=100))
        active_job = TranscriptionJob(params=TranscriptionParams(teletask_id=200))

        mock_queue_manager.get_all = AsyncMock(side_effect=[[whisper_job], []])
        mock_get_scheduler.return_value = SimpleNamespace(active_jobs=[active_job])

        response = client.get("/queues")
        assert response.status_code == 200

        data = response.json()
        assert len(data["whisper"]) == 1
        assert len(data["ollama"]) == 0
        assert len(data["active"]) == 1
        assert data["whisper"][0]["params"]["teletask_id"] == 100
        assert data["active"][0]["params"]["teletask_id"] == 200

    @patch("app.api.scheduling_routes.queue_manager")
    @patch("app.api.scheduling_routes.get_scheduler", side_effect=RuntimeError("not initialized"))
    def test_returns_empty_active_when_scheduler_unavailable(
        self, _mock_get_scheduler, mock_queue_manager, client
    ):
        mock_queue_manager.get_all = AsyncMock(side_effect=[[], []])

        response = client.get("/queues")
        assert response.status_code == 200
        assert response.json()["active"] == []


class TestGetScheduler:
    @patch("app.api.scheduling_routes.get_scheduler")
    def test_returns_scheduler_snapshot(self, mock_get_scheduler, client):
        mock_get_scheduler.return_value = SimpleNamespace(
            snapshot=lambda: {
                "max_workers": 5,
                "batch_size": 10,
                "available_capacity": 3,
                "active_worker_count": 2,
                "active_workers": {"worker-1": [], "worker-2": []},
                "jobs_by_id": {},
            }
        )

        response = client.get("/scheduler")
        assert response.status_code == 200

        data = response.json()
        assert data["max_workers"] == 5
        assert data["batch_size"] == 10
        assert data["available_capacity"] == 3
        assert data["active_worker_count"] == 2
        assert set(data["active_workers"].keys()) == {"worker-1", "worker-2"}

    @patch("app.api.scheduling_routes.get_scheduler", side_effect=RuntimeError("not initialized"))
    def test_returns_empty_snapshot_when_scheduler_unavailable(self, _mock_get_scheduler, client):
        response = client.get("/scheduler")
        assert response.status_code == 200
        assert response.json() == {
            "max_workers": 0,
            "batch_size": 0,
            "available_capacity": 0,
            "active_worker_count": 0,
            "active_workers": {},
            "jobs_by_id": {},
        }


class TestPrioritize:
    @patch("app.api.scheduling_routes.queue_manager")
    @patch("app.api.scheduling_routes.pingVideoByID", return_value="200")
    def test_prioritize_available_id(self, _mock_ping, mock_queue_manager, client):
        mock_queue_manager.remove_by_teletask_id = AsyncMock(return_value=[])
        mock_queue_manager.add = AsyncMock(return_value=True)

        response = client.post("/prioritize/11401")
        assert response.status_code == 200
        assert "prioritized" in response.json()["message"]

    @patch("app.api.scheduling_routes.pingVideoByID", return_value="404")
    def test_prioritize_unavailable_id(self, _mock_ping, client):
        response = client.post("/prioritize/99999")
        assert response.status_code == 200
        assert "not available" in response.json()["message"]
