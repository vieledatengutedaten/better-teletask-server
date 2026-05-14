"""Universe of teletask_ids the pipeline should process.

Today: 1..biggest known, minus blacklist. The biggest known id is whichever
is larger — the highest id with VTT data, or the highest id on the blacklist
(some blacklisted ids are newer than anything in vtt_files).
"""

from app.db.blacklist import get_blacklisted_ids
from app.db.vtt_files import getHighestTeletaskID
from lib.core.logger import logger
from lib.services.scraper import pingVideoByID


def get_teletask_ids() -> set[int]:
    highest_vtt = getHighestTeletaskID()
    blacklisted = get_blacklisted_ids() or []
    highest_blacklist = max(blacklisted) if blacklisted else None

    candidates = [v for v in (highest_vtt, highest_blacklist) if v is not None]
    if not candidates:
        return set()

    biggest = max(candidates)
    return set(range(1, biggest + 1)) - set(blacklisted)

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
            unreachable_ids.append(highest+i)
    return ids