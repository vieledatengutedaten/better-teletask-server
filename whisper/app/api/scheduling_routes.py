from fastapi import APIRouter

from app.core.logger import logger
from app.models.dataclasses import TranscriptionJob, TranscriptionParams
from app.services.scraper import pingVideoByID
from app.scheduler.queues import queue_manager
from app.scheduler.scheduler import get_scheduler

schedule_router = APIRouter()


@schedule_router.get("/queues")
async def get_queues():
    whisper_jobs = await queue_manager.get_all("whisper")
    ollama_jobs = await queue_manager.get_all("ollama")
    try:
        active_jobs = get_scheduler().active_jobs
    except RuntimeError:
        active_jobs = []
    return {
        "whisper": [j.model_dump() for j in whisper_jobs],
        "ollama": [j.model_dump() for j in ollama_jobs],
        "active": [j.model_dump() for j in active_jobs],
    }


@schedule_router.get("/ping")
async def ping_pong():
    return "pong"


@schedule_router.post("/prioritize/{teletask_id}")
async def prioritize_id(teletask_id: int):
    res = pingVideoByID(str(teletask_id))
    if res != "200":
        logger.error(f"ID {teletask_id} not available (response: {res}).")
        return {"message": f"ID {teletask_id} is not available."}

    # Remove from normal queues if already enqueued
    removed = await queue_manager.remove_by_teletask_id(teletask_id)
    if removed:
        # Check if any were already priority or running
        for job in removed:
            if job.status == "RUNNING":
                logger.info(f"ID {teletask_id} is currently being processed.")
                return {"message": f"ID {teletask_id} is currently being processed."}

    job = TranscriptionJob(params=TranscriptionParams(teletask_id=teletask_id))
    added = await queue_manager.add(job, priority=True)
    if not added:
        logger.info(f"ID {teletask_id} already in priority queue.")
        return {"message": f"ID {teletask_id} was already prioritized."}

    logger.info(f"ID {teletask_id} prioritized.")
    return {"message": f"ID {teletask_id} prioritized."}
