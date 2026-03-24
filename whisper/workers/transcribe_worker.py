import asyncio
from typing import Optional

from db.vtt_files import original_language_exists
from services.scraper import pingVideoByID
from services.pipeline import transcribePipelineVideoByID
from workers.queues import (
    prio_queue,
    forward_queue,
    in_between_queue,
    backward_queue,
    in_process_queue,
    multi_lock,
)

import logger
import logging

logger = logging.getLogger("btt_root_logger")


async def get_id_for_worker() -> Optional[int]:
    """Get the next ID to process from the queues in order of priority."""
    logger.info("Getting ID for worker...")
    id = None
    async with multi_lock(
        [prio_queue, forward_queue, in_between_queue, backward_queue]
    ):
        logger.debug("Got locks for all queues")
        if await prio_queue.peek_unlocked() is not None:
            id = await prio_queue.dequeue_unlocked()
            logger.info(f"Found ID {id} in priority queue")
        elif await forward_queue.peek_unlocked() is not None:
            id = await forward_queue.dequeue_unlocked()
            logger.info(f"Found ID {id} in forward queue")
        elif await in_between_queue.peek_unlocked() is not None:
            id = await in_between_queue.dequeue_unlocked()
            logger.info(f"Found ID {id} in in-between queue")
        elif await backward_queue.peek_unlocked() is not None:
            id = await backward_queue.dequeue_unlocked()
            logger.info(f"Found ID {id} in backward queue")
    if id is not None:
        res = pingVideoByID(str(id))
        if original_language_exists(id):
            logger.info(f"ID {id} already has original language, skipping.")
            return await get_id_for_worker()
        elif res == "200":
            logger.info(
                f"Fetched ID {id} from queue and it is available for processing, starting worker"
            )
            await in_process_queue.add_unlocked(id)
            asyncio.create_task(remove_id_from_in_process(id))
            return id
        else:
            logger.info(f"ID {id} not HTTP 200 (response: {res}), trying next ID.")
            return await get_id_for_worker()
    else:
        return None


async def remove_id_from_in_process(id: int):
    """Remove an ID from the in-process queue after timeout."""
    logger.debug(f"ID {id} will be removed from in-process queue after timeout.")
    await asyncio.sleep(1200)  # wait 20 minutes
    logger.debug(f"Removing ID {id} from in-process queue.")
    await in_process_queue.remove(id)


async def transcribe_worker():
    """Worker that continuously processes IDs from the queues."""
    sleep_time = 40
    await asyncio.sleep(10)  # Initial delay before starting
    logger.info("Transcribe worker started.")
    while True:
        id = await get_id_for_worker()
        logger.info(f"Got ID for worker: {id}")
        if id is not None:
            logger.info(f"Transcribing ID: {id}")
            try:
                logger.debug(f"Starting transcription for ID {id} in separate thread.")
                await asyncio.to_thread(transcribePipelineVideoByID, str(id))
            except Exception as e:
                logger.error(f"Transcription failed for ID {id}: {e}")
        else:
            logger.info(
                f"No IDs available to transcribe, waiting {sleep_time} seconds..."
            )
            await asyncio.sleep(sleep_time)
