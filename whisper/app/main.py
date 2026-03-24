import uvicorn
import asyncio
import contextlib
from fastapi import FastAPI

# setup logging — must be imported before other modules to configure handlers
from app.core import logger
import logging

logger = logging.getLogger("btt_root_logger")

from app.db.migrations import initDatabase
from app.db.vtt_files import getSmallestTeletaskID
from app.db.blacklist import get_missing_available_inbetween_ids
from app.services.scraper import get_upper_ids
from app.workers.queues import (
    prio_queue,
    forward_queue,
    in_between_queue,
    backward_queue,
)
from app.workers.transcribe_worker import transcribe_worker
from app.workers.queue_updaters import (
    update_upper_ids_periodically,
    update_inbetween_ids_periodically,
)
from app.api.scheduling_routes import schedule_router


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    initDatabase()
    upper_ids = get_upper_ids()
    if upper_ids is not None:
        await forward_queue.replace(upper_ids)
    smallest_id = getSmallestTeletaskID()
    if smallest_id is not None:
        await backward_queue.replace(list(range(smallest_id - 1, 0, -1)))
    missing_ids = get_missing_available_inbetween_ids()
    if missing_ids:
        await in_between_queue.replace(sorted(missing_ids, reverse=True))

    logger.info("Application startup: Queues initialized.")
    logger.debug("Forward queue: %s", await forward_queue.get_all())
    logger.debug("In-between queue: %s", await in_between_queue.get_all())
    logger.debug("Backward queue: %s", await backward_queue.get_all())

    # Start background tasks
    transcribe_worker_task = asyncio.create_task(transcribe_worker())
    update_upper_ids_task = asyncio.create_task(update_upper_ids_periodically())
    update_inbetween_ids_task = asyncio.create_task(update_inbetween_ids_periodically())
    yield  # Yield control to start the server

    transcribe_worker_task.cancel()
    try:
        await transcribe_worker_task
    except asyncio.CancelledError:
        logger.error("Can not cancel transcribe worker task.")
    logger.info("Cancelled transcribe worker task.")

    update_upper_ids_task.cancel()
    update_inbetween_ids_task.cancel()
    try:
        await update_upper_ids_task
    except asyncio.CancelledError:
        logger.error("Can not cancel update upper IDs task.")
    logger.info("Cancelled update upper IDs task.")
    try:
        await update_inbetween_ids_task
    except asyncio.CancelledError:
        logger.error("Can not cancel update in-between IDs task.")
    logger.info("Cancelled update in-between IDs task.")

    logger.info("Application shutdown complete.")


app = FastAPI(lifespan=lifespan)
app.include_router(schedule_router, prefix="/schedule")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
