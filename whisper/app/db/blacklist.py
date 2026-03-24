from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.db.connection import get_session
from app.db.error_handling import db_operation
from app.db.schema import BlacklistIdRecord
from app.db.vtt_files import get_missing_inbetween_ids

from app.core import logger
import logging

logger = logging.getLogger("btt_root_logger")


@db_operation(
    success_message="Successfully added Teletask ID {teletaskid} to blacklist."
)
def add_id_to_blacklist(teletaskid, reason):
    with get_session() as session:
        insert_stmt = pg_insert(BlacklistIdRecord).values(
            lecture_id=teletaskid,
            reason=reason,
        )
        stmt = insert_stmt.on_conflict_do_update(
            index_elements=[BlacklistIdRecord.lecture_id],
            set_={
                "times_tried": BlacklistIdRecord.times_tried + 1,
                "reason": insert_stmt.excluded.reason,
            },
        )
        session.execute(stmt)


@db_operation(success_message="Successfully queried blacklisted IDs.")
def get_blacklisted_ids():
    with get_session() as session:
        rows = session.execute(select(BlacklistIdRecord.lecture_id)).all()
        return [row[0] for row in rows]


@db_operation(success_message="Successfully computed available missing in-between IDs.")
def get_missing_available_inbetween_ids():
    initial_ids = get_missing_inbetween_ids()
    blacklisted_ids = get_blacklisted_ids()
    if initial_ids is None:
        return []
    logger.debug(list(set(initial_ids) - set(blacklisted_ids)))
    return list(set(initial_ids) - set(blacklisted_ids))
