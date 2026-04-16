"""
Tests for scheduler/pipeline.py — PipelineCoordinator
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import app.scheduler.pipeline as pipeline_module
from app.scheduler.pipeline import PipelineCoordinator
from lib.models.jobs import TranscriptionJob, TranscriptionParams


def _transcription_job(teletask_id: int, priority: int = 0) -> TranscriptionJob:
    return TranscriptionJob(
        params=TranscriptionParams(teletask_id=teletask_id),
        priority=priority,
    )


def _step(
    name: str,
    job_type: str,
    factory,
    is_done,
    done_ids,
    depends_on: tuple[str, ...] = (),
):
    return SimpleNamespace(
        stage_name=name,
        job_type=job_type,
        factory=factory,
        is_done=is_done,
        done_ids=done_ids,
        depends_on=depends_on,
    )


@pytest.mark.asyncio
async def test_advance_enqueues_first_undone_step(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue_manager = SimpleNamespace(add_all=AsyncMock(return_value=1))
    coordinator = PipelineCoordinator(queue_manager=queue_manager)

    step_done = _step(
        name="scrape",
        job_type="scrape_lecture_data",
        factory=lambda tid, priority: [],
        is_done=lambda _tid: True,
        done_ids=lambda: set(),
    )

    step = _step(
        name="transcribe",
        job_type="transcription",
        factory=lambda tid, priority: [_transcription_job(tid, priority)],
        is_done=lambda _tid: False,
        done_ids=lambda: set(),
        depends_on=("scrape_lecture_data",),
    )
    monkeypatch.setattr(pipeline_module, "ordered_pipeline_specs", lambda: [step_done, step])

    result = await coordinator.advance(42, priority=1)

    assert result == ["transcribe"]
    queue_manager.add_all.assert_awaited_once()
    enqueued_jobs = queue_manager.add_all.await_args.args[0]
    assert len(enqueued_jobs) == 1
    assert enqueued_jobs[0].params.teletask_id == 42


@pytest.mark.asyncio
async def test_advance_returns_empty_when_all_steps_done(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue_manager = SimpleNamespace(add_all=AsyncMock(return_value=1))
    coordinator = PipelineCoordinator(queue_manager=queue_manager)

    step = _step(
        name="transcribe",
        job_type="transcription",
        factory=lambda tid, priority: [_transcription_job(tid, priority)],
        is_done=lambda _tid: True,
        done_ids=lambda: set(),
    )
    monkeypatch.setattr(pipeline_module, "ordered_pipeline_specs", lambda: [step])

    result = await coordinator.advance(42, priority=1)

    assert result == []
    queue_manager.add_all.assert_not_awaited()


@pytest.mark.asyncio
async def test_advance_with_no_jobs_does_not_enqueue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue_manager = SimpleNamespace(add_all=AsyncMock(return_value=1))
    coordinator = PipelineCoordinator(queue_manager=queue_manager)

    step = _step(
        name="transcribe",
        job_type="transcription",
        factory=lambda tid, priority: [],
        is_done=lambda _tid: False,
        done_ids=lambda: set(),
    )
    monkeypatch.setattr(pipeline_module, "ordered_pipeline_specs", lambda: [step])

    result = await coordinator.advance(42, priority=1)

    assert result == []
    queue_manager.add_all.assert_not_awaited()


@pytest.mark.asyncio
async def test_advance_enqueues_all_ready_dependency_siblings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue_manager = SimpleNamespace(add_all=AsyncMock(return_value=1))
    coordinator = PipelineCoordinator(queue_manager=queue_manager)

    transcribe_done = _step(
        name="transcribe",
        job_type="transcription",
        factory=lambda tid, priority: [],
        is_done=lambda _tid: True,
        done_ids=lambda: set(),
    )
    translate_ready = _step(
        name="translate",
        job_type="translation",
        factory=lambda tid, priority: [_transcription_job(tid, priority)],
        is_done=lambda _tid: False,
        done_ids=lambda: set(),
        depends_on=("transcription",),
    )
    summarize_ready = _step(
        name="summarize",
        job_type="summary",
        factory=lambda tid, priority: [_transcription_job(tid, priority)],
        is_done=lambda _tid: False,
        done_ids=lambda: set(),
        depends_on=("transcription",),
    )
    monkeypatch.setattr(
        pipeline_module,
        "ordered_pipeline_specs",
        lambda: [transcribe_done, translate_ready, summarize_ready],
    )

    result = await coordinator.advance(42, priority=1)

    assert result == ["translate", "summarize"]
    assert queue_manager.add_all.await_count == 2


@pytest.mark.asyncio
async def test_initialize_jobs_respects_dependencies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue_manager = SimpleNamespace(
        add_all=AsyncMock(side_effect=lambda jobs: len(jobs)),
        pending_teletask_ids=AsyncMock(return_value=set()),
    )
    scheduler = SimpleNamespace(active_teletask_ids=lambda _job_type: set())
    coordinator = PipelineCoordinator(queue_manager=queue_manager)

    scrape = _step(
        name="scrape",
        job_type="scrape_lecture_data",
        factory=lambda tid, priority: [_transcription_job(tid, priority)],
        is_done=lambda _tid: False,
        done_ids=lambda: {1, 2, 3},
    )
    transcribe = _step(
        name="transcribe",
        job_type="transcription",
        factory=lambda tid, priority: [_transcription_job(tid, priority)],
        is_done=lambda _tid: False,
        done_ids=lambda: {1, 2},
        depends_on=("scrape_lecture_data",),
    )
    translate = _step(
        name="translate",
        job_type="translation",
        factory=lambda tid, priority: [_transcription_job(tid, priority)],
        is_done=lambda _tid: False,
        done_ids=lambda: {1},
        depends_on=("transcription",),
    )
    summarize = _step(
        name="summarize",
        job_type="summary",
        factory=lambda tid, priority: [_transcription_job(tid, priority)],
        is_done=lambda _tid: False,
        done_ids=lambda: {2},
        depends_on=("transcription",),
    )
    monkeypatch.setattr(
        pipeline_module,
        "ordered_pipeline_specs",
        lambda: [scrape, transcribe, translate, summarize],
    )

    result = await coordinator.initialize_jobs({1, 2, 3}, scheduler)

    assert result == {
        "scrape": 0,
        "transcribe": 1,
        "translate": 1,
        "summarize": 1,
    }
    assert queue_manager.add_all.await_count == 3
    submitted_ids = {
        call.args[0][0].params.teletask_id
        for call in queue_manager.add_all.await_args_list
    }
    assert submitted_ids == {1, 2, 3}
