"""Universe of teletask_ids the pipeline should process.

Today: 1..biggest known, minus blacklist. The biggest known id is whichever
is larger — the highest id with VTT data, or the highest id on the blacklist
(some blacklisted ids are newer than anything in vtt_files).
"""

from app.db.blacklist import get_blacklisted_ids
from app.db.vtt_files import getHighestTeletaskID


def get_teletask_ids() -> set[int]:
    highest_vtt = getHighestTeletaskID()
    blacklisted = get_blacklisted_ids() or []
    highest_blacklist = max(blacklisted) if blacklisted else None

    candidates = [v for v in (highest_vtt, highest_blacklist) if v is not None]
    if not candidates:
        return set()

    biggest = max(candidates)
    return set(range(1, biggest + 1)) - set(blacklisted)
