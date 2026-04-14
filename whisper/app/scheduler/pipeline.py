"""Pipeline coordinator for advancing teletask_ids through registered stages."""

from typing import TYPE_CHECKING

from lib.core.logger import logger
from lib.models.jobs import BaseJob
from app.scheduler.queues import QueueManager
from app.scheduler.registry import ordered_pipeline_specs

if TYPE_CHECKING:
    from app.scheduler.scheduler import Scheduler


class PipelineCoordinator:
    """Drives a teletask_id through ordered registry stages one step at a time.

    advance(tid):       enqueue the first undone step for this id
    initialize_jobs():  bulk version — one DB query per step, enqueue each id
                        at its first undone step, skipping pending/active dups
    """

    def __init__(self, queue_manager: QueueManager) -> None:
        self.queue_manager: QueueManager = queue_manager

    async def advance(self, teletask_id: int, priority: int = 0) -> str | None:
        for spec in ordered_pipeline_specs():
            if spec.is_done(teletask_id):
                continue

            jobs = spec.factory(teletask_id, priority)
            if jobs:
                _ = await self.queue_manager.add_all(jobs)
            return spec.stage_name
        return None

    async def initialize_jobs(
        self,
        all_ids: set[int],
        scheduler: "Scheduler",
    ) -> dict[str, int]:
        candidates = set(all_ids)
        enqueued: dict[str, int] = {}

        for spec in ordered_pipeline_specs():
            done = spec.done_ids()
            undone = candidates - done

            pending = await self.queue_manager.pending_teletask_ids(spec.job_type)
            active = scheduler.active_teletask_ids(spec.job_type)
            to_enqueue = undone - pending - active

            batch: list[BaseJob] = []
            for tid in to_enqueue:
                batch.extend(spec.factory(tid, 0))
            count = await self.queue_manager.add_all(batch) if batch else 0
            enqueued[spec.stage_name] = count

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
