"""
Tests for scheduler/scheduler.py — Scheduler
"""

import asyncio

import pytest
from app.models.dataclasses import (
    TranscriptionJob,
    TranscriptionParams,
    TranslationJob,
    TranslationParams,
)
from app.scheduler.queues import QueueManager
from app.scheduler.scheduler import Scheduler
from app.worker.worker_manager import WorkerManager
from app.worker.worker import Worker


def make_transcription(teletask_id: int = 1) -> TranscriptionJob:
    return TranscriptionJob(
        params=TranscriptionParams(teletask_id=teletask_id),
    )


def make_translation(
    teletask_id: int = 1, from_lang: str = "en", to_lang: str = "de"
) -> TranslationJob:
    return TranslationJob(
        params=TranslationParams(
            teletask_id=teletask_id, from_language=from_lang, to_language=to_lang
        ),
    )


class FakeWorker(Worker):
    """Records dispatched batches for assertions."""

    def __init__(self) -> None:
        self.transcribe_calls: list[list[TranscriptionJob]] = []
        self.translate_calls: list[list[TranslationJob]] = []
        self.dispatch_order: list[list[TranscriptionJob] | list[TranslationJob]] = []
        self.worker_ids: list[str] = []

    def transcribe(self, worker_id: str, jobs: list[TranscriptionJob]) -> None:
        self.worker_ids.append(worker_id)
        self.transcribe_calls.append(jobs)
        self.dispatch_order.append(jobs)

    def translate(self, worker_id: str, jobs: list[TranslationJob]) -> None:
        self.worker_ids.append(worker_id)
        self.translate_calls.append(jobs)
        self.dispatch_order.append(jobs)


@pytest.fixture
def queue_manager() -> QueueManager:
    return QueueManager()


@pytest.fixture
def fake_worker() -> FakeWorker:
    return FakeWorker()


@pytest.fixture
def scheduler(queue_manager: QueueManager, fake_worker: FakeWorker) -> Scheduler:
    return Scheduler(
        queue_manager=queue_manager,
        max_workers=3,
        batch_size=5,
        worker_manager=WorkerManager(
            transcribeWorker=fake_worker,
            translateWorker=fake_worker,
        ),
    )


class TestCapacity:
    @pytest.mark.asyncio
    async def test_initial_capacity(self, scheduler: Scheduler) -> None:
        assert scheduler.available_capacity == 3

    @pytest.mark.asyncio
    async def test_dispatch_reduces_capacity(
        self, scheduler: Scheduler, queue_manager: QueueManager
    ) -> None:
        await queue_manager.add(make_transcription(1))
        dispatched = await scheduler._dispatch_available()
        assert dispatched == 1
        assert scheduler.available_capacity == 2

    @pytest.mark.asyncio
    async def test_worker_finished_restores_capacity(
        self, scheduler: Scheduler, queue_manager: QueueManager
    ) -> None:
        await queue_manager.add(make_transcription(1))
        await scheduler._dispatch_available()
        worker_id = list(scheduler._active_workers.keys())[0]
        scheduler.worker_finished(worker_id)
        assert scheduler.available_capacity == 3

    @pytest.mark.asyncio
    async def test_worker_finished_unknown_returns_none(
        self, scheduler: Scheduler
    ) -> None:
        result = scheduler.worker_finished("nonexistent")
        assert result is None


class TestBatching:
    @pytest.mark.asyncio
    async def test_batch_respects_batch_size(
        self, scheduler: Scheduler, queue_manager: QueueManager, fake_worker: FakeWorker
    ) -> None:
        for i in range(8):
            await queue_manager.add(make_transcription(i))
        await scheduler._dispatch_available()
        # batch_size=5, so first worker gets 5 jobs, second gets 3
        assert len(fake_worker.transcribe_calls) == 2
        assert len(fake_worker.transcribe_calls[0]) == 5
        assert len(fake_worker.transcribe_calls[1]) == 3

    @pytest.mark.asyncio
    async def test_batch_same_category(
        self, scheduler: Scheduler, queue_manager: QueueManager, fake_worker: FakeWorker
    ) -> None:
        """All jobs in a batch must be from the same resource category."""
        for i in range(3):
            await queue_manager.add(make_transcription(i))
        await queue_manager.add(make_translation(10))
        await scheduler._dispatch_available()
        # First worker: 3 transcriptions, second worker: 1 translation
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
        assert len(fake_worker.transcribe_calls) == 0
        assert len(fake_worker.translate_calls) == 0


class TestPriority:
    @pytest.mark.asyncio
    async def test_whisper_priority_before_ollama_priority(
        self, scheduler: Scheduler, queue_manager: QueueManager, fake_worker: FakeWorker
    ) -> None:
        await queue_manager.add(make_translation(1), priority=True)
        await queue_manager.add(make_transcription(2), priority=True)
        await scheduler._dispatch_available()
        # whisper prio dispatched first
        assert len(fake_worker.transcribe_calls) == 1
        assert len(fake_worker.translate_calls) == 1
        assert fake_worker.transcribe_calls[0][0].params.teletask_id == 2

    @pytest.mark.asyncio
    async def test_priority_before_normal(
        self, scheduler: Scheduler, queue_manager: QueueManager, fake_worker: FakeWorker
    ) -> None:
        await queue_manager.add(make_transcription(1), priority=False)
        await queue_manager.add(make_transcription(2), priority=True)
        await scheduler._dispatch_available()
        # Both end up in one batch (same category), priority dequeued first
        assert len(fake_worker.transcribe_calls) == 1
        batch = fake_worker.transcribe_calls[0]
        assert batch[0].params.teletask_id == 2
        assert batch[1].params.teletask_id == 1

    @pytest.mark.asyncio
    async def test_whisper_priority_before_ollama_normal(
        self, scheduler: Scheduler, queue_manager: QueueManager, fake_worker: FakeWorker
    ) -> None:
        await queue_manager.add(make_translation(1), priority=False)
        await queue_manager.add(make_transcription(2), priority=True)
        await scheduler._dispatch_available()
        # whisper prio dispatched before ollama normal
        assert fake_worker.transcribe_calls[0][0].params.teletask_id == 2

    @pytest.mark.asyncio
    async def test_full_priority_order(
        self,
        queue_manager: QueueManager,
        fake_worker: FakeWorker,
    ) -> None:
        """With max_workers=4 and batch_size=1, each dispatch is one job, revealing exact order."""
        sched = Scheduler(
            queue_manager=queue_manager,
            max_workers=4,
            batch_size=1,
            worker_manager=WorkerManager(
                transcribeWorker=fake_worker,
                translateWorker=fake_worker,
            ),
        )
        # Add in reverse priority order
        await queue_manager.add(make_translation(4), priority=False)   # ollama normal (lowest)
        await queue_manager.add(make_transcription(3), priority=False) # whisper normal
        await queue_manager.add(make_translation(2), priority=True)    # ollama priority
        await queue_manager.add(make_transcription(1), priority=True)  # whisper priority (highest)

        await sched._dispatch_available()

        dispatched_ids = [call[0].params.teletask_id for call in fake_worker.dispatch_order]
        # Order: whisper prio(1), ollama prio(2), whisper normal(3), ollama normal(4)
        assert dispatched_ids == [1, 2, 3, 4]


class TestMaxWorkers:
    @pytest.mark.asyncio
    async def test_capacity_limits_dispatches(
        self, scheduler: Scheduler, queue_manager: QueueManager, fake_worker: FakeWorker
    ) -> None:
        """max_workers=3 means at most 3 workers, not 3 jobs."""
        for i in range(20):
            await queue_manager.add(make_transcription(i))
        await scheduler._dispatch_available()
        # batch_size=5, max_workers=3 → 3 workers dispatched with 5+5+5 jobs
        assert len(fake_worker.transcribe_calls) == 3
        assert scheduler.available_capacity == 0
        # 5 jobs remain in queue
        assert len(await queue_manager.get_all("whisper")) == 5

    @pytest.mark.asyncio
    async def test_no_dispatch_at_zero_capacity(
        self, scheduler: Scheduler, queue_manager: QueueManager, fake_worker: FakeWorker
    ) -> None:
        # Fill all worker slots
        for i in range(15):
            await queue_manager.add(make_transcription(i))
        await scheduler._dispatch_available()
        assert scheduler.available_capacity == 0

        # Add more jobs — dispatch should do nothing
        await queue_manager.add(make_transcription(99))
        dispatched = await scheduler._dispatch_available()
        assert dispatched == 0


class TestActiveJobs:
    @pytest.mark.asyncio
    async def test_active_jobs_lists_all(
        self, scheduler: Scheduler, queue_manager: QueueManager
    ) -> None:
        for i in range(3):
            await queue_manager.add(make_transcription(i))
        await scheduler._dispatch_available()
        assert len(scheduler.active_jobs) == 3
        assert all(j.status == "RUNNING" for j in scheduler.active_jobs)

    @pytest.mark.asyncio
    async def test_active_jobs_empty_initially(self, scheduler: Scheduler) -> None:
        assert scheduler.active_jobs == []


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

        worker_id = list(scheduler._active_workers.keys())[0]
        finished_jobs = scheduler.worker_finished(worker_id)

        assert finished_jobs is not None
        assert len(finished_jobs) == 1
        assert finished_jobs[0].id == job.id
        assert scheduler.get_job(job.id) is None
        assert scheduler.active_jobs == []

    @pytest.mark.asyncio
    async def test_worker_finished_for_job_removes_owning_worker_batch(
        self, scheduler: Scheduler, queue_manager: QueueManager
    ) -> None:
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
        self, scheduler: Scheduler, queue_manager: QueueManager, fake_worker: FakeWorker
    ) -> None:
        """run() should dispatch jobs, then wake when worker_finished is called."""
        await queue_manager.add(make_transcription(1))

        async def finish_after_delay() -> None:
            await asyncio.sleep(0.05)
            worker_id = list(scheduler._active_workers.keys())[0]
            scheduler.worker_finished(worker_id)

        async def run_scheduler() -> None:
            # Run one iteration by adding a job and letting it dispatch,
            # then cancel after the wake
            task = asyncio.create_task(scheduler.run())
            await asyncio.sleep(0.02)  # let it dispatch
            asyncio.create_task(finish_after_delay())
            await asyncio.sleep(0.1)  # let it wake and loop
            task.cancel()

        await run_scheduler()
        assert len(fake_worker.transcribe_calls) == 1
        assert scheduler.available_capacity == 3
