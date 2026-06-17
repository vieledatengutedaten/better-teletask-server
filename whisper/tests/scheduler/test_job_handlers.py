"""
Tests for scheduler/job_handlers.py — focused on ScrapeLectureDataJobHandler.

The headline test (`test_full_scrape_job_lifecycle`) drives a scrape job through
the whole pipeline the way the running system does:

    enqueue -> scheduler dispatch (prepare + worker handoff) -> worker produces a
    result -> handler.handle_result (which persists via add_lecture_data) ->
    worker_finished cleans up and frees capacity

The result-handling step mirrors what app/api/worker_routes.py does in
`_submit_result_common` (call the registered handler, then mark the job COMPLETED),
so we exercise the real handler instead of a stand-in.
"""

import asyncio
from collections.abc import Sequence
from typing import override

import pytest

import app.scheduler.job_handlers as job_handlers_module
from lib.models.jobs import (
    JobPayload,
    JobParamsBase,
    JobType,
    LectureScrapeData,
    ScrapeLectureDataJob,
    ScrapeLectureDataParams,
    ScrapeLectureDataResult,
    TranscriptionResult,
)
from app.scheduler.queues import QueueManager
from app.scheduler.registry import JOB_TYPES, spec_for
from app.scheduler.scheduler import Scheduler
from app.worker.worker import Worker
from app.worker.worker_manager import WorkerManager


class RecordingWorker(Worker):
    """Captures the payloads the scheduler hands to the worker."""

    def __init__(self) -> None:
        self.dispatched: list[tuple[str, JobType, list[JobPayload[JobParamsBase]]]] = []

    @override
    async def run(
        self,
        worker_id: str,
        job_type: JobType,
        payloads: Sequence[JobPayload[JobParamsBase]],
    ) -> None:
        self.dispatched.append((worker_id, job_type, list(payloads)))


def make_lecture_data(teletask_id: int) -> LectureScrapeData:
    return LectureScrapeData(
        lecture_id=teletask_id,
        lecturer_ids=[1, 2],
        lecturer_names=["Ada Lovelace", "Grace Hopper"],
        date="April 16, 2026",
        language="English",
        duration="00:42:00",
        lecture_title="Compiler Construction",
        series_id=99,
        series_name="Foundations of Computer Science",
        url="https://example.com/lecture/12345/podcast.mp4",
    )


@pytest.fixture
def recording_worker() -> RecordingWorker:
    return RecordingWorker()


@pytest.fixture
def queue_manager() -> QueueManager:
    return QueueManager()


@pytest.fixture
def scheduler(
    queue_manager: QueueManager, recording_worker: RecordingWorker
) -> Scheduler:
    worker_manager = WorkerManager(workers={jt: recording_worker for jt in JOB_TYPES})
    return Scheduler(queue_manager=queue_manager, worker_manager=worker_manager)


@pytest.fixture
def captured_lecture_data(monkeypatch: pytest.MonkeyPatch) -> list[LectureScrapeData]:
    """Stub the DB write the handler performs and record what it was given.

    The handler does `from app.db.lectures import add_lecture_data`, so the name
    is bound in the job_handlers module — patch it there.
    """
    captured: list[LectureScrapeData] = []
    monkeypatch.setattr(
        job_handlers_module,
        "add_lecture_data",
        lambda lecture_data: captured.append(lecture_data),
    )
    return captured


class TestScrapeJobLifecycle:
    @pytest.mark.asyncio
    async def test_full_scrape_job_lifecycle(
        self,
        scheduler: Scheduler,
        queue_manager: QueueManager,
        recording_worker: RecordingWorker,
        captured_lecture_data: list[LectureScrapeData],
    ) -> None:
        teletask_id = 12345
        job = ScrapeLectureDataJob(
            params=ScrapeLectureDataParams(teletask_id=teletask_id)
        )

        # 1. Enqueue.
        assert await queue_manager.add(job) is True

        # 2. Scheduler dispatches: prepare() runs and the job is handed to a
        #    worker on the cpu resource. sleep(0) flushes the create_task the
        #    WorkerManager fires for the worker run.
        cpu_cap = scheduler.capacity_for("cpu")
        dispatched = await scheduler._dispatch_available()
        await asyncio.sleep(0)

        assert dispatched == 1
        assert scheduler.capacity_for("cpu") == cpu_cap - 1
        assert job.status == "RUNNING"

        # The worker received exactly this job's payload.
        assert len(recording_worker.dispatched) == 1
        worker_id, dispatched_type, payloads = recording_worker.dispatched[0]
        assert dispatched_type == "scrape_lecture_data"
        assert [p.job_id for p in payloads] == [job.id]
        assert scheduler.get_worker_id_for_job(job.id) == worker_id

        # 3. Worker finishes and reports a result. Replay exactly what
        #    worker_routes._submit_result_common does: run the registered
        #    handler, then mark the job COMPLETED.
        result = ScrapeLectureDataResult(
            job_id=job.id,
            success=True,
            lecture_data=make_lecture_data(teletask_id),
        )
        handler = spec_for(job.job_type).handler
        await handler.handle_result(job, result)
        job.status = "COMPLETED"

        # handle_result persisted the scraped lecture data.
        assert len(captured_lecture_data) == 1
        assert captured_lecture_data[0].lecture_id == teletask_id
        assert captured_lecture_data[0].lecturer_names == [
            "Ada Lovelace",
            "Grace Hopper",
        ]

        # 4. Worker close-out frees capacity and drops the job from the index.
        finished = scheduler.worker_finished(worker_id)
        assert finished is not None
        assert [j.id for j in finished] == [job.id]
        assert scheduler.get_job(job.id) is None
        assert scheduler.active_jobs == []
        assert scheduler.capacity_for("cpu") == cpu_cap

        # Job is no longer in flight, so it may be re-enqueued.
        assert (
            await queue_manager.add(
                ScrapeLectureDataJob(
                    params=ScrapeLectureDataParams(teletask_id=teletask_id)
                )
            )
            is True
        )


class TestScrapeJobHandlerUnit:
    def test_prepare_returns_true(self) -> None:
        handler = spec_for("scrape_lecture_data").handler
        job = ScrapeLectureDataJob(params=ScrapeLectureDataParams(teletask_id=1))
        assert handler.prepare(job) is True

    def test_parse_result_builds_typed_result(self) -> None:
        handler = spec_for("scrape_lecture_data").handler
        body = {
            "job_id": "sc-1",
            "success": True,
            "job_type": "scrape_lecture_data",
            "lecture_data": make_lecture_data(1).model_dump(),
        }
        result = handler.parse_result(body)
        assert isinstance(result, ScrapeLectureDataResult)
        assert result.lecture_data.lecture_id == 1

    @pytest.mark.asyncio
    async def test_handle_result_persists_lecture_data(
        self, captured_lecture_data: list[LectureScrapeData]
    ) -> None:
        handler = spec_for("scrape_lecture_data").handler
        job = ScrapeLectureDataJob(params=ScrapeLectureDataParams(teletask_id=7))
        result = ScrapeLectureDataResult(
            job_id=job.id, success=True, lecture_data=make_lecture_data(7)
        )
        await handler.handle_result(job, result)
        assert [d.lecture_id for d in captured_lecture_data] == [7]

    @pytest.mark.asyncio
    async def test_handle_result_rejects_wrong_result_type(
        self, captured_lecture_data: list[LectureScrapeData]
    ) -> None:
        handler = spec_for("scrape_lecture_data").handler
        job = ScrapeLectureDataJob(params=ScrapeLectureDataParams(teletask_id=8))
        wrong_result = TranscriptionResult(job_id=job.id, success=True)
        with pytest.raises(TypeError):
            await handler.handle_result(job, wrong_result)
        assert captured_lecture_data == []

    @pytest.mark.asyncio
    async def test_handle_failed_does_not_raise(self) -> None:
        handler = spec_for("scrape_lecture_data").handler
        job = ScrapeLectureDataJob(params=ScrapeLectureDataParams(teletask_id=9))
        # Failure path is a no-op beyond logging; it must not raise.
        await handler.handle_failed(job, "boom")
