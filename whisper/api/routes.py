from fastapi import APIRouter

from services.scraper import pingVideoByID
from workers.queues import (
    prio_queue, forward_queue, in_between_queue, backward_queue,
    in_process_queue, multi_lock,
)

import logger
import logging
logger = logging.getLogger("btt_root_logger")

router = APIRouter()


@router.get("/queues")
async def get_queues():
    prio = await prio_queue.get_all()
    forward = await forward_queue.get_all()
    in_between = await in_between_queue.get_all()
    backward = await backward_queue.get_all()
    in_process = await in_process_queue.get_all()
    return {
        "priority_queue": prio,
        "forward_queue": forward,
        "in_between_queue": in_between,
        "backward_queue": backward,
        "in_process_queue": in_process
    }


@router.get("/ping")
async def ping_pong():
    return "pong"


@router.post("/prioritize/{id}")
async def prioritize_id(id: int):
    res = pingVideoByID(str(id))
    if res == "200":
        async with multi_lock([prio_queue, forward_queue, in_between_queue, backward_queue, in_process_queue]):
            if await in_process_queue.contains_unlocked(id):
                logger.info(f"ID {id} is currently being processed; cannot prioritize.")
                return {"message": f"ID {id} is currently being processed; cannot prioritize."}
            if await prio_queue.contains_unlocked(id):
                logger.info(f"ID {id} is already in priority queue.")
                return {"message": f"ID {id} was already prioritized."}
            if await forward_queue.contains_unlocked(id):
                await forward_queue.remove_unlocked(id)
                logger.info(f"Removed ID {id} from forward queue for prioritization.")
            if await in_between_queue.contains_unlocked(id):
                await in_between_queue.remove_unlocked(id)
                logger.info(f"Removed ID {id} from in-between queue for prioritization.")
            if await backward_queue.contains_unlocked(id):
                await backward_queue.remove_unlocked(id)
                logger.info(f"Removed ID {id} from backward queue for prioritization.")
            await prio_queue.add_unlocked(id)
            logger.info(f"Added ID {id} to priority queue.")
            return {"message": f"ID {id} prioritized."}
    else:
        logger.error(f"ID {id} cannot be prioritized as it is not available (response: {res}).")
        return {"message": f"ID {id} cannot be prioritized as it is not available."}
