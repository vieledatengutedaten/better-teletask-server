import asyncio

from app.db.blacklist import get_blacklisted_ids
from app.db.vtt_files import getHighestTeletaskID
from lib.core.logger import logger
from lib.services.scraper import pingVideoByID
from app.scheduler.pipeline import PipelineCoordinator
from app.scheduler.scheduler import Scheduler


def get_teletask_ids() -> set[int]:
    highest_vtt = getHighestTeletaskID()
    blacklisted = get_blacklisted_ids() or []
    highest_blacklist = max(blacklisted) if blacklisted else None

    candidates = [v for v in (highest_vtt, highest_blacklist) if v is not None]
    if not candidates:
        return set()

    biggest = max(candidates)
    return set(range(1, biggest + 1)) - set(blacklisted)


async def run_discovery_loop(
    coordinator: PipelineCoordinator, scheduler: Scheduler, interval_seconds: int = 600
):
    # await asyncio.sleep(20)  # Initial delay before first discovery
    while True:
        try:
            _ = await discover_new_teletask_ids(coordinator, scheduler)
        except Exception as e:
            logger.error(f"Error during discovery loop: {e}", exc_info=True)
        await asyncio.sleep(interval_seconds)


async def discover_new_teletask_ids(
    coordinator: PipelineCoordinator, scheduler: Scheduler
) -> list[int]:
    """Check for new teletask_ids beyond the current known universe."""
    new_ids = get_upper_ids()
    if new_ids:
        logger.info(f"Discovered new teletask IDs: {new_ids}")
        jobs = await coordinator.initialize_jobs(set(new_ids), scheduler)
        count = sum(jobs.values())
        logger.info(f"Enqueued {count} new job(s) for discovered teletask IDs: {jobs}")

    else:
        logger.info("No new teletask IDs discovered.")
    return new_ids


def get_upper_ids() -> list[int]:
    ids: list[int] = []
    unreachable_ids: list[int] = []
    highest: int | None = getHighestTeletaskID()
    if highest is None:
        return ids
    highest = highest + 1
    for i in range(1, 10):
        res = pingVideoByID(str(highest + i))
        if res == "200":
            ids.append(highest + i)
        if res == "401":
            logger.error(
                "Received 401 Unauthorized. Check your USERNAME_COOKIE environment variable.",
                extra={"id": highest + i},
            )
        if res == "403" or res == "404":
            logger.warning(
                f"Received {res} for ID {highest + i}.", extra={"id": highest + i}
            )
            unreachable_ids.append(highest + i)
    return ids
