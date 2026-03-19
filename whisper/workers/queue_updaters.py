import asyncio

from services.scraper import get_upper_ids
from db.blacklist import get_missing_available_inbetween_ids
from workers.queues import (
    prio_queue, forward_queue, in_between_queue, backward_queue,
    in_process_queue, multi_lock,
)

import logging
logger = logging.getLogger("btt_root_logger")


async def update_upper_ids_periodically():
    """Periodically update the forward queue with new upper IDs."""
    sleep_time = 1200
    await asyncio.sleep(5)
    while True:
        logger.info("Updating upper IDs...")
        upper_ids = get_upper_ids()
        async with multi_lock([forward_queue, in_process_queue, prio_queue]):
            for uid in upper_ids:
                if not await forward_queue.contains_unlocked(uid) and not await in_process_queue.contains_unlocked(uid) and not await prio_queue.contains_unlocked(uid):
                    logger.info(f"Adding new upper ID to forward queue: {uid}")
                    await forward_queue.add_unlocked(uid)
        logger.info(f"Upper IDs update complete. Sleeping for {sleep_time // 60} minutes.")
        await asyncio.sleep(sleep_time)


async def update_inbetween_ids_periodically():
    """Periodically update the in-between queue with missing IDs."""
    sleep_time = 1200
    await asyncio.sleep(30)
    while True:
        logger.info("Updating in-between IDs...")
        missing_ids = get_missing_available_inbetween_ids()
        async with multi_lock([in_between_queue, backward_queue, in_process_queue, prio_queue]):
            for mid in missing_ids:
                if not await in_between_queue.contains_unlocked(mid) and not await in_process_queue.contains_unlocked(mid) and not await prio_queue.contains_unlocked(mid):
                    await in_between_queue.add_unlocked(mid)
                    if await backward_queue.contains_unlocked(mid):
                        logger.info(f"Removing ID {mid} from backward queue as it's now in in-between queue.")
                        await backward_queue.remove_unlocked(mid)
            await in_between_queue.sort_reverse_unlocked()
        logger.info(f"In-between IDs update complete. Sleeping for {sleep_time // 60} minutes.")
        await asyncio.sleep(sleep_time)
