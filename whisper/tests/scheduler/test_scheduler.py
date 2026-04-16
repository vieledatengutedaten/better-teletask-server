"""
Tests for scheduler/scheduler.py — Scheduler
"""

import asyncio

import pytest

from lib.models.jobs import (
    BaseJob,
    JobType,
    TranscriptionJob,
    TranscriptionParams,
    TranslationJob,
    TranslationParams,
)
from app.scheduler.queues import QueueManager
from app.scheduler.registry import JOB_TYPES, RESOURCES, JobTypeSpec, ResourceSpec, spec_for
from app.scheduler.scheduler import Scheduler
from app.worker.worker_manager import WorkerManager
from app.worker.worker import Worker


def make_transcription(teletask_id: int = 1, priority: int = 0) -> TranscriptionJob:
    return TranscriptionJob(
        params=TranscriptionParams(teletask_id=teletask_id),
        priority=priority,
    )


def make_translation(
    teletask_id: int = 1,
    from_lang: str = "en",
    to_lang: str = "de",
    priority: int = 0,
) -> TranslationJob:
    return TranslationJob(
        params=TranslationParams(
            teletask_id=teletask_id, from_language=from_lang, to_language=to_lang
        ),
        priority=priority,
    )


class FakeWorker(Worker):
    """Records dispatched batches for assertions."""

    def __init__(self) -> None:
        self.calls_by_jobtype: dict[JobType, list[list[BaseJob]]] = {}
        self.dispatch_order: list[list[BaseJob]] = []
        self.worker_ids: list[str] = []

    def run(self, worker_id: str, job_type: JobType, jobs: list[BaseJob]) -> None:
        self.worker_ids.append(worker_id)
        self.calls_by_jobtype.setdefault(job_type, []).append(jobs)
        self.dispatch_order.append(jobs)

    @property
    def transcribe_calls(self) -> list[list[BaseJob]]:
        return self.calls_by_jobtype.get("transcription", [])

    @property
    def translate_calls(self) -> list[list[BaseJob]]:
        return self.calls_by_jobtype.get("translation", [])


@pytest.fixture
def queue_manager() -> QueueManager:
    return QueueManager()


@pytest.fixture
def fake_worker() -> FakeWorker:
    return FakeWorker()


@pytest.fixture
def fake_worker_manager(fake_worker: FakeWorker) -> WorkerManager:
    return WorkerManager(workers={r: fake_worker for r in RESOURCES})


@pytest.fixture
def scheduler(
    queue_manager: QueueManager, fake_worker_manager: WorkerManager
) -> Scheduler:
    return Scheduler(
        queue_manager=queue_manager,
        worker_manager=fake_worker_manager,
    )


@pytest.fixture
def override_resources(monkeypatch: pytest.MonkeyPatch):
    """Helper to override RESOURCES.max_workers in-place for a test."""

    def _apply(**limits: int) -> None:
        for resource, max_workers in limits.items():
            monkeypatch.setitem(
                RESOURCES,
                resource,
                ResourceSpec(resource=resource, max_workers=max_workers),
            )

    return _apply


@pytest.fixture
def override_batch_size(monkeypatch: pytest.MonkeyPatch):
    """Helper to override JOB_TYPES[jt].batch_size in-place for a test."""

    def _apply(**sizes: int) -> None:
        for jt, batch_size in sizes.items():
            current = JOB_TYPES[jt]
            monkeypatch.setitem(
                JOB_TYPES,
                jt,
                JobTypeSpec(
                    job_type=current.job_type,
                    resource=current.resource,
                    job_cls=current.job_cls,
                    result_cls=current.result_cls,
                    handler=current.handler,
                    batch_size=batch_size,
                    base_priority=current.base_priority,
                    stage_name=current.stage_name,
                    stage_order=current.stage_order,
                    factory=current.factory,
                    is_done=current.is_done,
                    done_ids=current.done_ids,
                    depends_on=current.depends_on,
                ),
            )

    return _apply


class TestCapacity:
    @pytest.mark.asyncio
    async def test_initial_capacity_per_resource(self, scheduler: Scheduler) -> None:
        assert scheduler.capacity_for("whisper") == RESOURCES["whisper"].max_workers
        assert scheduler.capacity_for("ollama") == RESOURCES["ollama"].max_workers
        assert scheduler.capacity_for("cpu") == RESOURCES["cpu"].max_workers

    @pytest.mark.asyncio
    async def test_dispatch_reduces_capacity(
        self, scheduler: Scheduler, queue_manager: QueueManager
    ) -> None:
        whisper_cap = scheduler.capacity_for("whisper")
        await queue_manager.add(make_transcription(1))
        dispatched = await scheduler._dispatch_available()
        assert dispatched == 1
        assert scheduler.capacity_for("whisper") == whisper_cap - 1

    @pytest.mark.asyncio
    async def test_worker_finished_restores_capacity(
        self, scheduler: Scheduler, queue_manager: QueueManager
    ) -> None:
        whisper_cap = scheduler.capacity_for("whisper")
        await queue_manager.add(make_transcription(1))
        await scheduler._dispatch_available()
        worker_id = next(iter(scheduler._active["whisper"].keys()))
        scheduler.worker_finished(worker_id)
        assert scheduler.capacity_for("whisper") == whisper_cap

    @pytest.mark.asyncio
    async def test_worker_finished_unknown_returns_none(
        self, scheduler: Scheduler
    ) -> None:
        result = scheduler.worker_finished("nonexistent")
        assert result is None


class TestBatching:
    @pytest.mark.asyncio
    async def test_batch_respects_batch_size(
        self,
        scheduler: Scheduler,
        queue_manager: QueueManager,
        fake_worker: FakeWorker,
        override_batch_size,
        override_resources,
    ) -> None:
        override_resources(whisper=3)
        override_batch_size(transcription=5)
        for i in range(8):
            await queue_manager.add(make_transcription(i))
        await scheduler._dispatch_available()
        # batch_size=5, so first worker gets 5 jobs, second gets 3
        assert len(fake_worker.transcribe_calls) == 2
        assert len(fake_worker.transcribe_calls[0]) == 5
        assert len(fake_worker.transcribe_calls[1]) == 3

    @pytest.mark.asyncio
    async def test_batch_same_jobtype(
        self,
        scheduler: Scheduler,
        queue_manager: QueueManager,
        fake_worker: FakeWorker,
        override_batch_size,
    ) -> None:
        """All jobs in a batch must be from the same jobtype."""
        override_batch_size(transcription=5)
        for i in range(3):
            await queue_manager.add(make_transcription(i))
        await queue_manager.add(make_translation(10))
        await scheduler._dispatch_available()
        assert len(fake_worker.transcribe_calls) == 1
        assert len(fake_worker.translate_calls) == 1
        assert len(fake_worker.transcribe_calls[0]) == 3
        assert len(fake_worker.translate_calls[0]) == 1

    @pytest.mark.asyncio
    async def test_empty_queues_dispatch_nothing(
        self, scheduler: Scheduler, fake_worker: FakeWorker
    ) -> None:
        dispatched = await scheduler._dispatch_available()
        assert dispatched == 0
        assert fake_worker.dispatch_order == []

    @pytest.mark.asyncio
    async def test_prepare_rejection_is_replaced_in_same_batch(
        self,
        scheduler: Scheduler,
        queue_manager: QueueManager,
        fake_worker: FakeWorker,
        monkeypatch: pytest.MonkeyPatch,
        override_batch_size,
        override_resources,
    ) -> None:
        override_resources(whisper=1)
        override_batch_size(transcription=3)

        for teletask_id in [1, 2, 3, 4, 5]:
            await queue_manager.add(make_transcription(teletask_id))

        handler = spec_for("transcription").handler
        monkeypatch.setattr(
            handler,
            "prepare",
            lambda job: job.params.teletask_id != 3,
        )

        dispatched = await scheduler._dispatch_available()

        assert dispatched == 1
        assert len(fake_worker.transcribe_calls) == 1
        batch_ids = [job.params.teletask_id for job in fake_worker.transcribe_calls[0]]
        assert len(batch_ids) == 3
        assert 3 not in batch_ids
        assert batch_ids == [5, 4, 2]

    @pytest.mark.asyncio
    async def test_all_prepare_rejected_dispatches_nothing(
        self,
        scheduler: Scheduler,
        queue_manager: QueueManager,
        fake_worker: FakeWorker,
        monkeypatch: pytest.MonkeyPatch,
        override_batch_size,
        override_resources,
    ) -> None:
        override_resources(whisper=1)
        override_batch_size(transcription=2)

        await queue_manager.add(make_transcription(1))
        await queue_manager.add(make_transcription(2))

        handler = spec_for("transcription").handler
        monkeypatch.setattr(handler, "prepare", lambda job: False)

        dispatched = await scheduler._dispatch_available()

        assert dispatched == 0
        assert len(fake_worker.transcribe_calls) == 0
        assert scheduler.capacity_for("whisper") == 1

    @pytest.mark.asyncio
    async def test_prepare_rejection_releases_claim_for_reenqueue(
        self,
        scheduler: Scheduler,
        queue_manager: QueueManager,
        monkeypatch: pytest.MonkeyPatch,
        override_batch_size,
        override_resources,
    ) -> None:
        override_resources(whisper=1)
        override_batch_size(transcription=1)

        await queue_manager.add(make_transcription(55))

        handler = spec_for("transcription").handler
        monkeypatch.setattr(handler, "prepare", lambda _job: False)

        dispatched = await scheduler._dispatch_available()

        assert dispatched == 0
        assert await queue_manager.add(make_transcription(55)) is True


class TestPriority:
    @pytest.mark.asyncio
    async def test_higher_base_priority_jobtype_picked_first(
        self,
        scheduler: Scheduler,
        queue_manager: QueueManager,
        fake_worker: FakeWorker,
    ) -> None:
        """Transcription has higher base_priority than translation."""
        await queue_manager.add(make_translation(1))
        await queue_manager.add(make_transcription(2))
        await scheduler._dispatch_available()
        # Transcription dispatched first
        assert fake_worker.dispatch_order[0][0].job_type == "transcription"

    @pytest.mark.asyncio
    async def test_priority_field_orders_within_jobtype(
        self,
        scheduler: Scheduler,
        queue_manager: QueueManager,
        fake_worker: FakeWorker,
        override_batch_size,
    ) -> None:
        override_batch_size(transcription=2)
        await queue_manager.add(make_transcription(1, priority=0))
        await queue_manager.add(make_transcription(2, priority=1))
        await scheduler._dispatch_available()
        batch = fake_worker.transcribe_calls[0]
        assert batch[0].params.teletask_id == 2
        assert batch[1].params.teletask_id == 1

    @pytest.mark.asyncio
    async def test_full_priority_order(
        self,
        scheduler: Scheduler,
        queue_manager: QueueManager,
        fake_worker: FakeWorker,
        override_batch_size,
        override_resources,
    ) -> None:
        """With batch_size=1, each dispatch is one job, revealing exact order."""
        override_resources(whisper=2, ollama=2)
        override_batch_size(transcription=1, translation=1)

        await queue_manager.add(make_translation(4, priority=0))   # translation normal (lowest)
        await queue_manager.add(make_transcription(3, priority=0)) # transcription normal
        await queue_manager.add(make_translation(2, priority=1))   # translation priority
        await queue_manager.add(make_transcription(1, priority=1)) # transcription priority (highest)

        await scheduler._dispatch_available()

        # Transcription has higher base_priority, drained until whisper full or queue empty.
        # Whisper has 2 capacity → both transcription jobs dispatch first (priority=1 then priority=0).
        # Then translation (only ollama left) → priority=1 then priority=0.
        dispatched_jts = [batch[0].job_type for batch in fake_worker.dispatch_order]
        dispatched_tids = [batch[0].params.teletask_id for batch in fake_worker.dispatch_order]
        assert dispatched_jts[:2] == ["transcription", "transcription"]
        assert dispatched_tids[:2] == [1, 3]
        assert dispatched_jts[2:] == ["translation", "translation"]
        assert dispatched_tids[2:] == [2, 4]


class TestPerResourceCapacity:
    @pytest.mark.asyncio
    async def test_capacity_limits_dispatches_per_resource(
        self,
        scheduler: Scheduler,
        queue_manager: QueueManager,
        fake_worker: FakeWorker,
        override_batch_size,
        override_resources,
    ) -> None:
        override_resources(whisper=3)
        override_batch_size(transcription=5)
        for i in range(20):
            await queue_manager.add(make_transcription(i))
        await scheduler._dispatch_available()
        # batch_size=5, max_workers=3 → 3 workers dispatched with 5+5+5 jobs
        assert len(fake_worker.transcribe_calls) == 3
        assert scheduler.capacity_for("whisper") == 0
        # 5 jobs remain in transcription queue
        assert len(await queue_manager.get_all("transcription")) == 5

    @pytest.mark.asyncio
    async def test_no_dispatch_at_zero_capacity(
        self,
        scheduler: Scheduler,
        queue_manager: QueueManager,
        fake_worker: FakeWorker,
        override_batch_size,
        override_resources,
    ) -> None:
        override_resources(whisper=3)
        override_batch_size(transcription=5)
        for i in range(15):
            await queue_manager.add(make_transcription(i))
        await scheduler._dispatch_available()
        assert scheduler.capacity_for("whisper") == 0

        await queue_manager.add(make_transcription(99))
        dispatched = await scheduler._dispatch_available()
        assert dispatched == 0

    @pytest.mark.asyncio
    async def test_one_resource_full_does_not_block_another(
        self,
        scheduler: Scheduler,
        queue_manager: QueueManager,
        fake_worker: FakeWorker,
        override_batch_size,
        override_resources,
    ) -> None:
        """Whisper at capacity must not stop translation jobs from dispatching to ollama."""
        override_resources(whisper=1, ollama=2)
        override_batch_size(transcription=1, translation=1)

        await queue_manager.add(make_transcription(1))
        await queue_manager.add(make_translation(10))
        await queue_manager.add(make_translation(11))

        await scheduler._dispatch_available()

        assert scheduler.capacity_for("whisper") == 0
        assert scheduler.capacity_for("ollama") == 0
        assert len(fake_worker.transcribe_calls) == 1
        assert len(fake_worker.translate_calls) == 2


class TestActiveJobs:
    @pytest.mark.asyncio
    async def test_active_jobs_lists_all(
        self,
        scheduler: Scheduler,
        queue_manager: QueueManager,
        override_batch_size,
    ) -> None:
        override_batch_size(transcription=5)
        for i in range(3):
            await queue_manager.add(make_transcription(i))
        await scheduler._dispatch_available()
        assert len(scheduler.active_jobs) == 3
        assert all(j.status == "RUNNING" for j in scheduler.active_jobs)

    @pytest.mark.asyncio
    async def test_active_jobs_empty_initially(self, scheduler: Scheduler) -> None:
        assert scheduler.active_jobs == []

    @pytest.mark.asyncio
    async def test_active_teletask_ids_filtered_by_jobtype(
        self,
        scheduler: Scheduler,
        queue_manager: QueueManager,
        override_batch_size,
    ) -> None:
        override_batch_size(transcription=2, translation=2)
        await queue_manager.add(make_transcription(1))
        await queue_manager.add(make_transcription(2))
        await queue_manager.add(make_translation(99))
        await scheduler._dispatch_available()
        assert scheduler.active_teletask_ids("transcription") == {1, 2}
        assert scheduler.active_teletask_ids("translation") == {99}


class TestJobIndex:
    @pytest.mark.asyncio
    async def test_get_job_returns_dispatched_job(
        self, scheduler: Scheduler, queue_manager: QueueManager
    ) -> None:
        job = make_transcription(42)
        await queue_manager.add(job)
        await scheduler._dispatch_available()

        found = scheduler.get_job(job.id)
        assert found is not None
        assert found.id == job.id
        assert found.status == "RUNNING"

    @pytest.mark.asyncio
    async def test_get_job_returns_none_for_unknown_id(self, scheduler: Scheduler) -> None:
        assert scheduler.get_job("does-not-exist") is None

    @pytest.mark.asyncio
    async def test_worker_finished_removes_jobs_from_index(
        self, scheduler: Scheduler, queue_manager: QueueManager
    ) -> None:
        job = make_translation(99)
        await queue_manager.add(job)
        await scheduler._dispatch_available()

        worker_id = next(iter(scheduler._active["ollama"].keys()))
        finished_jobs = scheduler.worker_finished(worker_id)

        assert finished_jobs is not None
        assert len(finished_jobs) == 1
        assert finished_jobs[0].id == job.id
        assert scheduler.get_job(job.id) is None
        assert scheduler.active_jobs == []

    @pytest.mark.asyncio
    async def test_worker_finished_releases_claim_for_reenqueue(
        self, scheduler: Scheduler, queue_manager: QueueManager
    ) -> None:
        await queue_manager.add(make_transcription(88))
        await scheduler._dispatch_available()

        # While active, duplicate admission is blocked by in-flight dedupe.
        assert await queue_manager.add(make_transcription(88)) is False

        worker_id = next(iter(scheduler._active["whisper"].keys()))
        scheduler.worker_finished(worker_id)

        assert await queue_manager.add(make_transcription(88)) is True

    @pytest.mark.asyncio
    async def test_worker_finished_for_job_removes_owning_worker_batch(
        self,
        scheduler: Scheduler,
        queue_manager: QueueManager,
        override_batch_size,
    ) -> None:
        override_batch_size(transcription=2)
        job1 = make_transcription(101)
        job2 = make_transcription(102)
        await queue_manager.add(job1)
        await queue_manager.add(job2)
        await scheduler._dispatch_available()

        finished_jobs = scheduler.worker_finished_for_job(job1.id)
        assert finished_jobs is not None
        assert len(finished_jobs) == 2
        assert scheduler.get_job(job1.id) is None
        assert scheduler.get_job(job2.id) is None

    @pytest.mark.asyncio
    async def test_worker_finished_for_job_unknown_returns_none(
        self, scheduler: Scheduler
    ) -> None:
        assert scheduler.worker_finished_for_job("does-not-exist") is None

    @pytest.mark.asyncio
    async def test_get_worker_id_for_job_returns_dispatch_owner(
        self, scheduler: Scheduler, queue_manager: QueueManager
    ) -> None:
        job = make_transcription(77)
        await queue_manager.add(job)
        await scheduler._dispatch_available()

        worker_id = scheduler.get_worker_id_for_job(job.id)
        assert worker_id is not None
        assert worker_id.startswith("worker-")


class TestWake:
    @pytest.mark.asyncio
    async def test_worker_finished_sets_wake(self, scheduler: Scheduler) -> None:
        scheduler._wake.clear()
        scheduler.worker_finished("nonexistent")
        assert scheduler._wake.is_set()

    @pytest.mark.asyncio
    async def test_run_dispatches_and_wakes(
        self,
        scheduler: Scheduler,
        queue_manager: QueueManager,
        fake_worker: FakeWorker,
    ) -> None:
        await queue_manager.add(make_transcription(1))

        async def finish_after_delay() -> None:
            await asyncio.sleep(0.05)
            worker_id = next(iter(scheduler._active["whisper"].keys()))
            scheduler.worker_finished(worker_id)

        async def run_scheduler() -> None:
            task = asyncio.create_task(scheduler.run())
            await asyncio.sleep(0.02)
            asyncio.create_task(finish_after_delay())
            await asyncio.sleep(0.1)
            task.cancel()

        await run_scheduler()
        assert len(fake_worker.transcribe_calls) == 1
        assert scheduler.capacity_for("whisper") == RESOURCES["whisper"].max_workers
