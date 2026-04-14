"""Declarative pipeline for teletask_id processing.

Each PipelineStep declares:
  - job_type:   which registered jobtype runs this step
  - factory:    (teletask_id, priority) -> list[BaseJob] to enqueue
  - is_done:    runtime per-id DB check (for advance())
  - done_ids:   bulk set of completed ids (for initialize_jobs())

Adding a new step = one entry in PIPELINE. Existing teletask_ids auto-pick
up the step on next initialize_jobs() call.
"""

from dataclasses import dataclass
from typing import Callable, TYPE_CHECKING

from lib.core.logger import logger
from lib.models.jobs import (
    BaseJob,
    JobType,
    ScrapeLectureDataJob,
    ScrapeLectureDataParams,
    TARGET_LANGUAGES,
    TranscriptionJob,
    TranscriptionParams,
    TranslationJob,
    TranslationParams,
)
from app.db.lectures import get_all_lecture_ids, get_language_of_lecture
from app.db.vtt_files import (
    get_all_original_vtt_ids,
    get_missing_translations,
    original_language_exists,
)
from app.scheduler.queues import QueueManager

if TYPE_CHECKING:
    from app.scheduler.scheduler import Scheduler


@dataclass(frozen=True)
class PipelineStep:
    name: str
    job_type: JobType
    factory: Callable[[int, int], list[BaseJob]]
    is_done: Callable[[int], bool]
    done_ids: Callable[[], set[int]]


def _scrape_factory(tid: int, priority: int) -> list[BaseJob]:
    return [
        ScrapeLectureDataJob(
            params=ScrapeLectureDataParams(teletask_id=tid),
            priority=priority,
        )
    ]


def _scrape_done_ids() -> set[int]:
    ids = get_all_lecture_ids() or []
    return set(ids)


def _scrape_is_done(tid: int) -> bool:
    return get_language_of_lecture(tid) is not None


def _transcribe_factory(tid: int, priority: int) -> list[BaseJob]:
    return [
        TranscriptionJob(
            params=TranscriptionParams(teletask_id=tid),
            priority=priority,
        )
    ]


def _transcribe_done_ids() -> set[int]:
    ids = get_all_original_vtt_ids() or []
    return set(ids)


def _transcribe_is_done(tid: int) -> bool:
    return original_language_exists(tid)


def _existing_translations() -> dict[int, set[str]]:
    """teletask_id -> set of languages that already have a non-original VTT."""
    rows = get_missing_translations() or []
    by_tid: dict[int, set[str]] = {}
    for tid, lang in rows:
        by_tid.setdefault(tid, set()).add(lang)
    return by_tid


def _translate_factory(tid: int, priority: int) -> list[BaseJob]:
    existing = _existing_translations().get(tid, set())
    return [
        TranslationJob(
            params=TranslationParams(
                teletask_id=tid, from_language="original", to_language=lang
            ),
            priority=priority,
        )
        for lang in TARGET_LANGUAGES
        if lang not in existing
    ]


def _translate_done_ids() -> set[int]:
    by_tid = _existing_translations()
    required = set(TARGET_LANGUAGES)
    return {tid for tid, langs in by_tid.items() if required.issubset(langs)}


def _translate_is_done(tid: int) -> bool:
    existing = _existing_translations().get(tid, set())
    return set(TARGET_LANGUAGES).issubset(existing)


PIPELINE: list[PipelineStep] = [
    PipelineStep(
        name="scrape",
        job_type="scrape_lecture_data",
        factory=_scrape_factory,
        is_done=_scrape_is_done,
        done_ids=_scrape_done_ids,
    ),
    PipelineStep(
        name="transcribe",
        job_type="transcription",
        factory=_transcribe_factory,
        is_done=_transcribe_is_done,
        done_ids=_transcribe_done_ids,
    ),
    PipelineStep(
        name="translate",
        job_type="translation",
        factory=_translate_factory,
        is_done=_translate_is_done,
        done_ids=_translate_done_ids,
    ),
]


class PipelineCoordinator:
    """Drives a teletask_id through PIPELINE one step at a time.

    advance(tid):       enqueue the first undone step for this id
    initialize_jobs():  bulk version — one DB query per step, enqueue each id
                        at its first undone step, skipping pending/active dups
    """

    def __init__(self, queue_manager: QueueManager) -> None:
        self.queue_manager: QueueManager = queue_manager

    async def advance(self, teletask_id: int, priority: int = 0) -> str | None:
        for step in PIPELINE:
            if step.is_done(teletask_id):
                continue
            jobs = step.factory(teletask_id, priority)
            if jobs:
                _ = await self.queue_manager.add_all(jobs)
            return step.name
        return None

    async def initialize_jobs(
        self,
        all_ids: set[int],
        scheduler: "Scheduler",
    ) -> dict[str, int]:
        candidates = set(all_ids)
        enqueued: dict[str, int] = {}

        for step in PIPELINE:
            done = step.done_ids()
            undone = candidates - done

            pending = await self.queue_manager.pending_teletask_ids(step.job_type)
            active = scheduler.active_teletask_ids(step.job_type)
            to_enqueue = undone - pending - active

            batch: list[BaseJob] = []
            for tid in to_enqueue:
                batch.extend(step.factory(tid, 0))
            count = await self.queue_manager.add_all(batch) if batch else 0
            enqueued[step.name] = count

            candidates = candidates & done

        logger.info(f"Pipeline initialize_jobs enqueued: {enqueued}")
        return enqueued


_coordinator: "PipelineCoordinator | None" = None


def set_coordinator(coordinator: PipelineCoordinator) -> None:
    global _coordinator
    _coordinator = coordinator


def get_coordinator() -> PipelineCoordinator:
    if _coordinator is None:
        raise RuntimeError("PipelineCoordinator not initialized")
    return _coordinator
