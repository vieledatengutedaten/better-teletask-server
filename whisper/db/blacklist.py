from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError

from db.connection import get_session
from db.schema import BlacklistIdRecord
from db.vtt_files import get_missing_inbetween_ids

import logger
import logging
logger = logging.getLogger("btt_root_logger")


def add_id_to_blacklist(teletaskid, reason):
    session = None
    try:
        session = get_session()
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
        session.commit()
        logger.info(f"Successfully added Teletask ID {teletaskid} to blacklist.")
    except SQLAlchemyError as error:
        logger.error(f"Error while connecting to PostgreSQL: {error}")
    finally:
        if session:
            session.close()


def get_blacklisted_ids():
    session = None
    try:
        session = get_session()
        rows = session.execute(select(BlacklistIdRecord.lecture_id)).all()
        return [row[0] for row in rows]
    except SQLAlchemyError as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
        return []
    finally:
        if session:
            session.close()


def get_missing_available_inbetween_ids():
    initial_ids = get_missing_inbetween_ids()
    blacklisted_ids = get_blacklisted_ids()
    if initial_ids is None:
        return []
    logger.debug(list(set(initial_ids) - set(blacklisted_ids)))
    return list(set(initial_ids) - set(blacklisted_ids))
