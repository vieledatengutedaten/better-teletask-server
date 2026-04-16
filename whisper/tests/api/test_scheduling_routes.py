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
from lib.models.jobs import TranscriptionJob, TranscriptionParams


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

        mock_queue_manager.get_all = AsyncMock(side_effect=[[], [whisper_job], []])
        mock_get_scheduler.return_value = SimpleNamespace(active_jobs=[active_job])

        response = client.get("/queues")
        assert response.status_code == 200

        data = response.json()
        assert len(data["whisper"]) == 1
        assert len(data["ollama"]) == 0
        assert len(data["cpu"]) == 0
        assert len(data["active"]) == 1
        assert data["whisper"][0]["params"]["teletask_id"] == 100
        assert data["active"][0]["params"]["teletask_id"] == 200

    @patch("app.api.scheduling_routes.queue_manager")
    @patch("app.api.scheduling_routes.get_scheduler", side_effect=RuntimeError("not initialized"))
    def test_returns_empty_active_when_scheduler_unavailable(
        self, _mock_get_scheduler, mock_queue_manager, client
    ):
        mock_queue_manager.get_all = AsyncMock(side_effect=[[], [], []])

        response = client.get("/queues")
        assert response.status_code == 200
        assert response.json()["active"] == []


class TestGetScheduler:
    @patch("app.api.scheduling_routes.get_scheduler")
    def test_returns_scheduler_snapshot(self, mock_get_scheduler, client):
        snapshot = {
            "resources": {
                "whisper": {
                    "max_workers": 2,
                    "available_capacity": 1,
                    "active_workers": {"worker-1": []},
                },
                "ollama": {
                    "max_workers": 3,
                    "available_capacity": 3,
                    "active_workers": {},
                },
                "cpu": {
                    "max_workers": 8,
                    "available_capacity": 8,
                    "active_workers": {},
                },
            },
            "jobs_by_id": {},
        }
        mock_get_scheduler.return_value = SimpleNamespace(snapshot=lambda: snapshot)

        response = client.get("/scheduler")
        assert response.status_code == 200

        data = response.json()
        assert data["resources"]["whisper"]["max_workers"] == 2
        assert data["resources"]["ollama"]["available_capacity"] == 3
        assert "worker-1" in data["resources"]["whisper"]["active_workers"]

    @patch("app.api.scheduling_routes.get_scheduler", side_effect=RuntimeError("not initialized"))
    def test_returns_empty_snapshot_when_scheduler_unavailable(self, _mock_get_scheduler, client):
        from app.scheduler.registry import RESOURCES

        response = client.get("/scheduler")
        assert response.status_code == 200
        data = response.json()
        assert data["jobs_by_id"] == {}
        for resource, spec in RESOURCES.items():
            assert data["resources"][resource]["max_workers"] == spec.max_workers
            assert data["resources"][resource]["available_capacity"] == 0
            assert data["resources"][resource]["active_workers"] == {}


class TestPrioritize:
    @patch("app.api.scheduling_routes.get_coordinator")
    @patch("app.api.scheduling_routes.queue_manager")
    @patch("app.api.scheduling_routes.pingVideoByID", return_value="200")
    def test_prioritize_available_id(
        self, _mock_ping, mock_queue_manager, mock_get_coordinator, client
    ):
        mock_queue_manager.remove_by_teletask_id = AsyncMock(return_value=[])
        coordinator = SimpleNamespace(advance=AsyncMock(return_value=["scrape"]))
        mock_get_coordinator.return_value = coordinator

        response = client.post("/prioritize/11401")
        assert response.status_code == 200
        assert "prioritized" in response.json()["message"]
        assert response.json()["next_steps"] == ["scrape"]
        coordinator.advance.assert_awaited_once_with(11401, priority=1)

    @patch("app.api.scheduling_routes.get_coordinator")
    @patch("app.api.scheduling_routes.queue_manager")
    @patch("app.api.scheduling_routes.pingVideoByID", return_value="200")
    def test_prioritize_returns_complete_when_pipeline_done(
        self, _mock_ping, mock_queue_manager, mock_get_coordinator, client
    ):
        mock_queue_manager.remove_by_teletask_id = AsyncMock(return_value=[])
        mock_get_coordinator.return_value = SimpleNamespace(
            advance=AsyncMock(return_value=[])
        )

        response = client.post("/prioritize/11401")
        assert response.status_code == 200
        assert "already complete" in response.json()["message"]

    @patch("app.api.scheduling_routes.pingVideoByID", return_value="404")
    def test_prioritize_unavailable_id(self, _mock_ping, client):
        response = client.post("/prioritize/99999")
        assert response.status_code == 200
        assert "not available" in response.json()["message"]
