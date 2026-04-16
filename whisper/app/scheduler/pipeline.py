"""Pipeline coordinator for advancing teletask_ids through registered stages."""

from typing import TYPE_CHECKING

from lib.core.logger import logger
from lib.models.jobs import BaseJob, JobType
from app.scheduler.queues import QueueManager
from app.scheduler.registry import ordered_pipeline_specs

if TYPE_CHECKING:
    from app.scheduler.scheduler import Scheduler


class PipelineCoordinator:
    """Drives teletask_ids through dependency-based registered stages.

    advance(tid):       enqueue all ready-and-undone steps for this id
    initialize_jobs():  bulk version — one DB query per step, enqueue each id
                        for each ready step, skipping pending/active dups
    """

    def __init__(self, queue_manager: QueueManager) -> None:
        self.queue_manager: QueueManager = queue_manager

    async def advance(self, teletask_id: int, priority: int = 0) -> list[str]:
        specs = ordered_pipeline_specs()
        done_by_type: dict[JobType, bool] = {
            spec.job_type: spec.is_done(teletask_id)
            for spec in specs
        }

        enqueued_steps: list[str] = []
        for spec in specs:
            if done_by_type[spec.job_type]:
                continue

            deps_done = all(done_by_type.get(dep, False) for dep in spec.depends_on)
            if not deps_done:
                continue

            jobs = spec.factory(teletask_id, priority)
            if jobs:
                added = await self.queue_manager.add_all(jobs)
                if added > 0:
                    enqueued_steps.append(spec.stage_name)
        return enqueued_steps

    async def initialize_jobs(
        self,
        all_ids: set[int],
        scheduler: "Scheduler",
    ) -> dict[str, int]:
        specs = ordered_pipeline_specs()
        done_by_type: dict[JobType, set[int]] = {
            spec.job_type: spec.done_ids()
            for spec in specs
        }

        enqueued: dict[str, int] = {}

        for spec in specs:
            eligible = set(all_ids)
            for dep in spec.depends_on:
                eligible &= done_by_type[dep]

            undone = eligible - done_by_type[spec.job_type]

            pending = await self.queue_manager.pending_teletask_ids(spec.job_type)
            active = scheduler.active_teletask_ids(spec.job_type)
            to_enqueue = undone - pending - active

            batch: list[BaseJob] = []
            for tid in to_enqueue:
                batch.extend(spec.factory(tid, 0))
            count = await self.queue_manager.add_all(batch) if batch else 0
            enqueued[spec.stage_name] = count

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
