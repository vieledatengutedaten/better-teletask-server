import psycopg2
from db.connection import get_connection
from db.vtt_files import get_missing_inbetween_ids

import logging
logger = logging.getLogger("btt_root_logger")


def add_id_to_blacklist(teletaskid, reason):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO blacklist_ids (lecture_id, reason) VALUES (%s, %s) ON CONFLICT (lecture_id) DO UPDATE SET times_tried = blacklist_ids.times_tried + 1, reason = EXCLUDED.reason;",
            (teletaskid, reason),
        )
        conn.commit()
        logger.info(f"Successfully added Teletask ID {teletaskid} to blacklist.")
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while connecting to PostgreSQL: {error}")
    finally:
        if conn:
            cur.close()
            conn.close()


def get_blacklisted_ids():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT lecture_id FROM blacklist_ids;")
        rows = cur.fetchall()
        ids = [row[0] for row in rows]
        return ids
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
        return []
    finally:
        if conn:
            cur.close()
            conn.close()


def get_missing_available_inbetween_ids():
    initial_ids = get_missing_inbetween_ids()
    blacklisted_ids = get_blacklisted_ids()
    if initial_ids is None:
        return []
    logger.debug(list(set(initial_ids) - set(blacklisted_ids)))
    return list(set(initial_ids) - set(blacklisted_ids))
