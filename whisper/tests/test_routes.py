"""
Tests for api/routes.py

Uses FastAPI's TestClient to make real HTTP calls against the app
without needing a running server. Queues and external services are mocked.
"""

import pytest
from unittest.mock import patch, AsyncMock, MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.routes import router


@pytest.fixture
def app():
    """Create a fresh FastAPI app with the router mounted."""
    app = FastAPI()
    app.include_router(router)
    return app


@pytest.fixture
def client(app):
    """TestClient that speaks to the app without a real server."""
    return TestClient(app)


class TestPing:
    def test_ping_returns_pong(self, client):
        response = client.get("/ping")
        assert response.status_code == 200
        assert response.json() == "pong"


class TestGetQueues:
    @patch("api.routes.in_process_queue")
    @patch("api.routes.backward_queue")
    @patch("api.routes.in_between_queue")
    @patch("api.routes.forward_queue")
    @patch("api.routes.prio_queue")
    def test_returns_all_queues(
        self, mock_prio, mock_fwd, mock_inb, mock_bwd, mock_inp, client
    ):
        mock_prio.get_all = AsyncMock(return_value=[100])
        mock_fwd.get_all = AsyncMock(return_value=[200, 201])
        mock_inb.get_all = AsyncMock(return_value=[])
        mock_bwd.get_all = AsyncMock(return_value=[300])
        mock_inp.get_all = AsyncMock(return_value=[])

        response = client.get("/queues")
        assert response.status_code == 200
        data = response.json()

        assert data["priority_queue"] == [100]
        assert data["forward_queue"] == [200, 201]
        assert data["in_between_queue"] == []
        assert data["backward_queue"] == [300]
        assert data["in_process_queue"] == []


class TestPrioritize:
    @patch("api.routes.multi_lock")
    @patch("api.routes.in_process_queue")
    @patch("api.routes.backward_queue")
    @patch("api.routes.in_between_queue")
    @patch("api.routes.forward_queue")
    @patch("api.routes.prio_queue")
    @patch("api.routes.pingVideoByID", return_value="200")
    def test_prioritize_available_id(
        self, mock_ping, mock_prio, mock_fwd, mock_inb, mock_bwd, mock_inp,
        mock_multi_lock, client
    ):
        # Setup: ID is not in any queue
        mock_inp.contains_unlocked = AsyncMock(return_value=False)
        mock_prio.contains_unlocked = AsyncMock(return_value=False)
        mock_fwd.contains_unlocked = AsyncMock(return_value=False)
        mock_inb.contains_unlocked = AsyncMock(return_value=False)
        mock_bwd.contains_unlocked = AsyncMock(return_value=False)
        mock_prio.add_unlocked = AsyncMock()

        # multi_lock returns an async context manager
        mock_multi_lock.return_value.__aenter__ = AsyncMock()
        mock_multi_lock.return_value.__aexit__ = AsyncMock(return_value=False)

        response = client.post("/prioritize/11401")
        assert response.status_code == 200
        assert "prioritized" in response.json()["message"]

    @patch("api.routes.pingVideoByID", return_value="404")
    def test_prioritize_unavailable_id(self, mock_ping, client):
        response = client.post("/prioritize/99999")
        assert response.status_code == 200
        assert "not available" in response.json()["message"]
