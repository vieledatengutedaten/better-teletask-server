from fastapi import APIRouter

from lib.core.logger import logger
from lib.models.jobs import BaseJob
from lib.services.scraper import pingVideoByID
from app.scheduler.pipeline import get_coordinator
from app.scheduler.queues import queue_manager
from app.scheduler.registry import JOB_TYPES, RESOURCES
from app.scheduler.scheduler import get_scheduler

schedule_router = APIRouter()


@schedule_router.get("/queues")
async def get_queues():
    by_resource: dict[str, list[BaseJob]] = {r: [] for r in RESOURCES}
    for jt, spec in JOB_TYPES.items():
        jobs = await queue_manager.get_all(jt)
        by_resource[spec.resource].extend(jobs)
    try:
        active_jobs = get_scheduler().active_jobs
    except RuntimeError:
        active_jobs = []
    return {
        **{r: [j.model_dump() for j in jobs] for r, jobs in by_resource.items()},
        "active": [j.model_dump() for j in active_jobs],
    }


@schedule_router.get("/scheduler")
async def get_scheduler_state():
    try:
        scheduler = get_scheduler()
    except RuntimeError:
        return {
            "resources": {
                r: {
                    "max_workers": spec.max_workers,
                    "available_capacity": 0,
                    "active_workers": {},
                }
                for r, spec in RESOURCES.items()
            },
            "jobs_by_id": {},
        }

    return scheduler.snapshot()


@schedule_router.get("/ping")
async def ping_pong():
    return "pong"


@schedule_router.post("/prioritize/{teletask_id}")
async def prioritize_id(teletask_id: int):
    res = pingVideoByID(str(teletask_id))
    if res != "200":
        logger.error(f"ID {teletask_id} not available (response: {res}).")
        return {"message": f"ID {teletask_id} is not available."}

    removed = await queue_manager.remove_by_teletask_id(teletask_id)
    if removed:
        for job in removed:
            if job.status == "RUNNING":
                logger.info(f"ID {teletask_id} is currently being processed.")
                return {"message": f"ID {teletask_id} is currently being processed."}

    next_step = await get_coordinator().advance(teletask_id, priority=1)
    if next_step is None:
        logger.info(f"ID {teletask_id} pipeline already complete.")
        return {"message": f"ID {teletask_id} pipeline already complete."}

    logger.info(f"ID {teletask_id} prioritized at pipeline step {next_step}.")
    return {"message": f"ID {teletask_id} prioritized at step {next_step}."}
