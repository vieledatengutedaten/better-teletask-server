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
):
    return SimpleNamespace(
        stage_name=name,
        job_type=job_type,
        factory=factory,
        is_done=is_done,
        done_ids=done_ids,
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
    )
    monkeypatch.setattr(pipeline_module, "ordered_pipeline_specs", lambda: [step_done, step])

    result = await coordinator.advance(42, priority=1)

    assert result == "transcribe"
    queue_manager.add_all.assert_awaited_once()
    enqueued_jobs = queue_manager.add_all.await_args.args[0]
    assert len(enqueued_jobs) == 1
    assert enqueued_jobs[0].params.teletask_id == 42


@pytest.mark.asyncio
async def test_advance_returns_none_when_all_steps_done(
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

    assert result is None
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

    assert result == "transcribe"
    queue_manager.add_all.assert_not_awaited()
