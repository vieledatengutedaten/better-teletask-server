from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from app.api.worker_routes import worker_router
from app.models.dataclasses import TranscriptionJob, TranscriptionParams
from app.scheduler.scheduler import get_scheduler


class FakeScheduler:
    def __init__(self, job: TranscriptionJob, worker_id: str = "worker-1") -> None:
        self.job: TranscriptionJob = job
        self.worker_id: str = worker_id
        self.active: bool = True
        self.worker_finished_calls: list[str] = []
        self.worker_finished_for_job_calls: list[str] = []

    def get_job(self, job_id: str):
        if self.job.id == job_id:
            return self.job
        return None

    def get_worker_id_for_job(self, job_id: str):
        if self.active and self.job.id == job_id:
            return self.worker_id
        return None

    def worker_finished(self, worker_id: str):
        self.worker_finished_calls.append(worker_id)
        was_active = self.active
        self.active = False
        return [self.job] if was_active else None

    def worker_finished_for_job(self, job_id: str):
        self.worker_finished_for_job_calls.append(job_id)
        if self.job.id != job_id:
            return None
        return self.worker_finished(self.worker_id)


@pytest.fixture
def job() -> TranscriptionJob:
    return TranscriptionJob(params=TranscriptionParams(teletask_id=12345))


@pytest.fixture
def scheduler(job: TranscriptionJob) -> FakeScheduler:
    return FakeScheduler(job=job)


@pytest.fixture
def client(scheduler: FakeScheduler):
    app = FastAPI()
    app.include_router(worker_router, prefix="/worker")
    app.dependency_overrides[get_scheduler] = lambda: scheduler
    return TestClient(app)


class TestWorkerRoutesV2:
    def test_status_v2_updates_job_status(self, client: TestClient, job: TranscriptionJob):
        response = client.post(
            f"/worker/worker-1/jobs/{job.id}/status",
            json={"status": "RUNNING"},
        )
        assert response.status_code == 200
        assert job.status == "RUNNING"

    def test_status_v2_rejects_wrong_owner(self, client: TestClient, job: TranscriptionJob):
        response = client.post(
            f"/worker/worker-2/jobs/{job.id}/status",
            json={"status": "RUNNING"},
        )
        assert response.status_code == 409

    def test_result_v2_keeps_worker_open_until_finished_called(
        self, client: TestClient, scheduler: FakeScheduler, job: TranscriptionJob
    ):
        response = client.post(
            f"/worker/worker-1/jobs/{job.id}/result",
            json={"job_id": job.id, "success": True, "job_type": "transcription"},
        )
        assert response.status_code == 200
        assert scheduler.worker_finished_calls == []

    def test_finished_v2_closes_worker(
        self, client: TestClient, scheduler: FakeScheduler
    ):
        response = client.post("/worker/worker-1/finished", json={})
        assert response.status_code == 200
        assert scheduler.worker_finished_calls == ["worker-1"]


    def test_legacy_route_not_available(self, client: TestClient, job: TranscriptionJob):
        response = client.post(
            f"/worker/{job.id}/result",
            json={"job_id": job.id, "success": True, "job_type": "transcription"},
        )
        assert response.status_code == 404
