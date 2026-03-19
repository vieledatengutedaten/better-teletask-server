import os
import psycopg2
from db.connection import get_connection
from models import VttFile
from config import OUTPUT_PATH, ASR_MODEL, COMPUTE_TYPE

import logging
logger = logging.getLogger("btt_root_logger")


def get_all_original_vtt_ids():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT lecture_id FROM vtt_files WHERE is_original_lang = TRUE;")
        rows = cur.fetchall()
        ids = [row[0] for row in rows]
        logger.debug(f"Fetched all original VTT IDs: {ids}")
        return ids
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
        return []
    finally:
        if conn:
            cur.close()
            conn.close()


def original_language_exists(teletaskid):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM vtt_files WHERE lecture_id = %s AND is_original_lang = TRUE;",
            (teletaskid,),
        )
        count = cur.fetchone()[0]
        return count > 0
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
        return False
    finally:
        if conn:
            cur.close()
            conn.close()


def save_vtt_as_blob(teletaskid, language, isOriginalLang):
    conn = None
    file_path = os.path.join(OUTPUT_PATH, str(teletaskid) + ".vtt")
    file_path_txt = os.path.join(OUTPUT_PATH, str(teletaskid) + ".txt")
    if not os.path.exists(file_path):
        logger.error(f"VTT file not found, cant put in database: {file_path}", extra={'id': teletaskid})
        return -1
    if not os.path.exists(file_path_txt):
        logger.error(f"TXT file not found, cant put in database: {file_path_txt}", extra={'id': teletaskid})
        return -1
    try:
        conn = get_connection()
        cur = conn.cursor()

        with open(file_path, "rb") as f:
            vtt_binary_data = f.read()

        with open(file_path_txt, "rb") as f:
            txt_binary_data = f.read()

        cur.execute(
            "INSERT INTO vtt_files (lecture_id, language, is_original_lang, vtt_data, txt_data, asr_model, compute_type) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id;",
            (
                teletaskid,
                language,
                isOriginalLang,
                vtt_binary_data,
                txt_binary_data,
                ASR_MODEL,
                COMPUTE_TYPE
            ),
        )

        vtt_file_id = cur.fetchone()[0]
        conn.commit()
        logger.info(f"Successfully saved '{file_path}' as BLOB", extra={'id': teletaskid})
        return vtt_file_id
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while connecting to PostgreSQL: {error}")
    finally:
        if conn:
            cur.close()
            conn.close()


def get_vtt_file_by_id(vtt_file_id) -> VttFile | None:
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT id, lecture_id, language, is_original_lang, vtt_data, txt_data, asr_model, compute_type, creation_date FROM vtt_files WHERE id = %s;",
            (vtt_file_id,),
        )
        row = cur.fetchone()
        if row:
            return VttFile(
                id=row[0],
                lecture_id=row[1],
                language=row[2],
                is_original_lang=row[3],
                vtt_data=bytes(row[4]),
                txt_data=bytes(row[5]),
                asr_model=row[6],
                compute_type=row[7],
                creation_date=row[8],
            )
        logger.info(f"No VTT file found with ID: {vtt_file_id}")
        return None
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
        return None
    finally:
        if conn:
            cur.close()
            conn.close()


def get_vtt_files_by_lecture_id(lecture_id) -> list[VttFile]:
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT id, lecture_id, language, is_original_lang, vtt_data, txt_data, asr_model, compute_type, creation_date FROM vtt_files WHERE lecture_id = %s;",
            (lecture_id,),
        )
        rows = cur.fetchall()
        return [
            VttFile(
                id=row[0],
                lecture_id=row[1],
                language=row[2],
                is_original_lang=row[3],
                vtt_data=bytes(row[4]),
                txt_data=bytes(row[5]),
                asr_model=row[6],
                compute_type=row[7],
                creation_date=row[8],
            )
            for row in rows
        ]
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
        return []
    finally:
        if conn:
            cur.close()
            conn.close()


def get_all_vtt_blobs() -> list[VttFile]:
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT id, lecture_id, language, is_original_lang, vtt_data, txt_data, asr_model, compute_type, creation_date FROM vtt_files ORDER BY id;"
        )
        rows = cur.fetchall()
        logger.info(f"Retrieved {len(rows)} VTT file(s) from database.")
        return [
            VttFile(
                id=row[0],
                lecture_id=row[1],
                language=row[2],
                is_original_lang=row[3],
                vtt_data=bytes(row[4]),
                txt_data=bytes(row[5]),
                asr_model=row[6],
                compute_type=row[7],
                creation_date=row[8],
            )
            for row in rows
        ]
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
        return []
    finally:
        if conn:
            cur.close()
            conn.close()


def get_original_language_by_id(teletaskid):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT language FROM vtt_files WHERE lecture_id = %s AND is_original_lang = TRUE;",
            (teletaskid,),
        )
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            logger.info(f"No original language found for Teletask ID: {teletaskid}")
            return None
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
        return None
    finally:
        if conn:
            cur.close()
            conn.close()


def get_original_vtt_by_id(teletaskid):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT vtt_data FROM vtt_files WHERE lecture_id = %s AND is_original_lang = TRUE;",
            (teletaskid,),
        )
        row = cur.fetchone()
        if row:
            vtt_data = row[0]
            vtt_bytes = bytes(vtt_data)
            vtt_content = vtt_bytes.decode("utf-8")
            return vtt_content
        else:
            logger.info(f"No original VTT found for Teletask ID: {teletaskid}")
            return None
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
        return None
    finally:
        if conn:
            cur.close()
            conn.close()


def getHighestTeletaskID():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT MAX(lecture_id) FROM vtt_files;")
        max_id = cur.fetchone()[0]
        logger.info(f"Highest Teletask ID in available in database: {max_id}")
        return max_id
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
    finally:
        if conn:
            cur.close()
            conn.close()


def getSmallestTeletaskID():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT MIN(lecture_id) FROM vtt_files;")
        max_id = cur.fetchone()[0]
        logger.info(f"Smallest Teletask ID in available in database: {max_id}")
        return max_id
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
    finally:
        if conn:
            cur.close()
            conn.close()


def get_missing_inbetween_ids():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(""" 
            WITH bounds AS (
            SELECT 
                MIN(lecture_id) AS min_id,
                MAX(lecture_id) AS max_id
            FROM vtt_files
            ),
            all_ids AS (
                SELECT generate_series(
                    (SELECT min_id FROM bounds),
                    (SELECT max_id FROM bounds)
                ) AS lecture_id
            )
            SELECT all_ids.lecture_id
            FROM all_ids
            LEFT JOIN vtt_files vf 
                ON all_ids.lecture_id = vf.lecture_id
            WHERE vf.lecture_id IS NULL
            ORDER BY all_ids.lecture_id;
        """)
        rows = cur.fetchall()
        ids = [row[0] for row in rows]
        return ids
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
    finally:
        if conn:
            cur.close()
            conn.close()


def get_missing_translations():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(""" 
            WITH all_ids AS( SELECT DISTINCT lecture_id FROM vtt_files )
            SELECT lecture_id, language FROM vtt_files
            WHERE is_original_lang = False
            ORDER BY lecture_id DESC;
        """)
        rows = cur.fetchall()
        id_lang_pairs = [(row[0], row[1]) for row in rows]
        return id_lang_pairs
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
    finally:
        if conn:
            cur.close()
            conn.close()
