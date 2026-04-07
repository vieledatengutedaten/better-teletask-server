from typing import Any


import uvicorn
import asyncio
import contextlib
from fastapi import FastAPI

# setup logging — must be imported before other modules to configure handlers
from app.core.logger import logger
from app.db.migrations import initDatabase
from app.db.vtt_files import getSmallestTeletaskID
from app.db.blacklist import get_missing_available_inbetween_ids
from app.models.dataclasses import TranscriptionJob, TranscriptionParams
from app.services.scraper import get_upper_ids
from app.scheduler.queues import queue_manager
from app.scheduler.scheduler import Scheduler, set_scheduler
from app.api.scheduling_routes import schedule_router
from app.api.worker_routes import worker_router


def _ids_to_transcription_jobs(ids: list[int]) -> list[TranscriptionJob]:
    return [TranscriptionJob(params=TranscriptionParams(teletask_id=tid)) for tid in ids]


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    initDatabase()

    # Enqueue upper IDs (forward)
    upper_ids = get_upper_ids()
    if upper_ids:
        _ = await queue_manager.add_all(
            jobs=_ids_to_transcription_jobs(ids=upper_ids),
            priority=True,
        )

    # Enqueue IDs below the smallest known (backward)
    smallest_id: int | None = getSmallestTeletaskID()
    if smallest_id is not None:
        backward_ids = list(range(smallest_id - 1, 0, -1))
        _ = await queue_manager.add_all(_ids_to_transcription_jobs(backward_ids))

    # Enqueue missing in-between IDs
    missing_ids: list[int] | None = get_missing_available_inbetween_ids()
    if missing_ids:
        _ = await queue_manager.add_all(
            _ids_to_transcription_jobs(sorted(missing_ids, reverse=True))
        )

    all_jobs = await queue_manager.get_all()
    logger.info(f"Application startup: {len(all_jobs)} transcription job(s) enqueued.")

    # Start scheduler
    scheduler = Scheduler(queue_manager=queue_manager)
    set_scheduler(scheduler)
    scheduler_task = asyncio.create_task(scheduler.run())
    yield

    scheduler_task.cancel()
    try:
        await scheduler_task
    except asyncio.CancelledError:
        pass
    logger.info("Application shutdown complete.")


app = FastAPI(lifespan=lifespan)
app.include_router(schedule_router, prefix="/schedule")
app.include_router(worker_router, prefix="/worker")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
