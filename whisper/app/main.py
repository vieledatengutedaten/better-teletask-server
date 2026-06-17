import uvicorn
import asyncio
import contextlib
from fastapi import Depends, FastAPI

# setup logging — must be imported before other modules to configure handlers
from lib.core.logger import logger
from app.db.migrations import initDatabase
from app.utils.discovery import get_teletask_ids, run_discovery_loop
from app.scheduler.pipeline import PipelineCoordinator, set_coordinator
from app.scheduler.queues import queue_manager
from app.scheduler.scheduler import Scheduler, set_scheduler
from lib.core.config import ENVIRONMENT
from app.api.scheduling_routes import schedule_router
from app.api.worker_routes import worker_router
from app.middleware.workerauth import verify_worker_token


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    initDatabase()

    scheduler = Scheduler(queue_manager=queue_manager)
    set_scheduler(scheduler)

    coordinator = PipelineCoordinator(queue_manager=queue_manager)
    set_coordinator(coordinator)

    teletask_ids = get_teletask_ids()
    counts = await coordinator.initialize_jobs(teletask_ids, scheduler)
    total = sum(counts.values())
    logger.info(
        f"Application startup: {len(teletask_ids)} teletask_id(s) in universe, {total} job(s) enqueued — {counts}"
    )

    upper_id_task = asyncio.create_task(run_discovery_loop(coordinator,scheduler))
    scheduler_task = asyncio.create_task(scheduler.run())
    yield

    scheduler_task.cancel()
    upper_id_task.cancel()
    try:
        await scheduler_task
    except asyncio.CancelledError:
        pass

    try:
        await upper_id_task
    except asyncio.CancelledError:
        pass
    logger.info("Application shutdown complete.")


app = FastAPI(lifespan=lifespan)

app.include_router(schedule_router, prefix="/schedule")
app.include_router(
    worker_router, prefix="/worker", dependencies=[Depends(verify_worker_token)]
)

if ENVIRONMENT == "dev":
    from app.api.admin_routes import admin_router

    app.include_router(admin_router, prefix="/admin")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
