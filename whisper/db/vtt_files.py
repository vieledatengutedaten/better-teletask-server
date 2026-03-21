import os

from sqlalchemy import desc, func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import SQLAlchemyError

from db.connection import get_connection
from db.schema import VttFileRecord
from models import VttFile
from config import OUTPUT_PATH, ASR_MODEL, COMPUTE_TYPE

import logger
import logging
logger = logging.getLogger("btt_root_logger")


def _to_vtt_file(record: VttFileRecord) -> VttFile:
    return VttFile(
        id=record.id,
        lecture_id=record.lecture_id,
        language=record.language,
        is_original_lang=record.is_original_lang,
        vtt_data=bytes(record.vtt_data),
        txt_data=bytes(record.txt_data),
        asr_model=record.asr_model,
        compute_type=record.compute_type,
        creation_date=record.creation_date,
    )


def get_all_original_vtt_ids():
    session = None
    try:
        session = get_connection()
        rows = session.execute(
            select(VttFileRecord.lecture_id).where(VttFileRecord.is_original_lang.is_(True))
        ).all()
        ids = [row[0] for row in rows]
        logger.debug(f"Fetched all original VTT IDs: {ids}")
        return ids
    except SQLAlchemyError as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
        return []
    finally:
        if session:
            session.close()


def original_language_exists(teletaskid):
    session = None
    try:
        session = get_connection()
        count = session.execute(
            select(func.count())
            .select_from(VttFileRecord)
            .where(
                VttFileRecord.lecture_id == teletaskid,
                VttFileRecord.is_original_lang.is_(True),
            )
        ).scalar_one()
        return count > 0
    except SQLAlchemyError as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
        return False
    finally:
        if session:
            session.close()


def save_vtt_as_blob(teletaskid, language, isOriginalLang):
    session = None
    file_path = os.path.join(OUTPUT_PATH, str(teletaskid) + ".vtt")
    file_path_txt = os.path.join(OUTPUT_PATH, str(teletaskid) + ".txt")
    if not os.path.exists(file_path):
        logger.error(f"VTT file not found, cant put in database: {file_path}", extra={"id": teletaskid})
        return -1
    if not os.path.exists(file_path_txt):
        logger.error(f"TXT file not found, cant put in database: {file_path_txt}", extra={"id": teletaskid})
        return -1
    try:
        session = get_connection()

        with open(file_path, "rb") as f:
            vtt_binary_data = f.read()

        with open(file_path_txt, "rb") as f:
            txt_binary_data = f.read()

        stmt = (
            pg_insert(VttFileRecord)
            .values(
                lecture_id=teletaskid,
                language=language,
                is_original_lang=isOriginalLang,
                vtt_data=vtt_binary_data,
                txt_data=txt_binary_data,
                asr_model=ASR_MODEL,
                compute_type=COMPUTE_TYPE,
            )
            .returning(VttFileRecord.id)
        )

        vtt_file_id = session.execute(stmt).scalar_one()
        session.commit()
        logger.info(f"Successfully saved '{file_path}' as BLOB", extra={"id": teletaskid})
        return vtt_file_id
    except SQLAlchemyError as error:
        logger.error(f"Error while connecting to PostgreSQL: {error}")
    finally:
        if session:
            session.close()


def get_vtt_file_by_id(vtt_file_id) -> VttFile | None:
    session = None
    try:
        session = get_connection()
        record = session.execute(
            select(VttFileRecord).where(VttFileRecord.id == vtt_file_id)
        ).scalar_one_or_none()
        if record:
            return _to_vtt_file(record)
        logger.info(f"No VTT file found with ID: {vtt_file_id}")
        return None
    except SQLAlchemyError as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
        return None
    finally:
        if session:
            session.close()


def get_vtt_files_by_lecture_id(lecture_id) -> list[VttFile]:
    session = None
    try:
        session = get_connection()
        records = session.execute(
            select(VttFileRecord).where(VttFileRecord.lecture_id == lecture_id)
        ).scalars().all()
        return [_to_vtt_file(record) for record in records]
    except SQLAlchemyError as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
        return []
    finally:
        if session:
            session.close()


def get_all_vtt_blobs() -> list[VttFile]:
    session = None
    try:
        session = get_connection()
        records = session.execute(
            select(VttFileRecord).order_by(VttFileRecord.id)
        ).scalars().all()
        logger.info(f"Retrieved {len(records)} VTT file(s) from database.")
        return [_to_vtt_file(record) for record in records]
    except SQLAlchemyError as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
        return []
    finally:
        if session:
            session.close()


def get_original_language_by_id(teletaskid):
    session = None
    try:
        session = get_connection()
        language = session.execute(
            select(VttFileRecord.language).where(
                VttFileRecord.lecture_id == teletaskid,
                VttFileRecord.is_original_lang.is_(True),
            )
        ).scalar_one_or_none()
        if language:
            return language
        logger.info(f"No original language found for Teletask ID: {teletaskid}")
        return None
    except SQLAlchemyError as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
        return None
    finally:
        if session:
            session.close()


def get_original_vtt_by_id(teletaskid):
    session = None
    try:
        session = get_connection()
        vtt_data = session.execute(
            select(VttFileRecord.vtt_data).where(
                VttFileRecord.lecture_id == teletaskid,
                VttFileRecord.is_original_lang.is_(True),
            )
        ).scalar_one_or_none()
        if vtt_data:
            return bytes(vtt_data).decode("utf-8")
        logger.info(f"No original VTT found for Teletask ID: {teletaskid}")
        return None
    except SQLAlchemyError as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
        return None
    finally:
        if session:
            session.close()


def getHighestTeletaskID():
    session = None
    try:
        session = get_connection()
        max_id = session.execute(select(func.max(VttFileRecord.lecture_id))).scalar_one()
        logger.info(f"Highest Teletask ID in available in database: {max_id}")
        return max_id
    except SQLAlchemyError as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
    finally:
        if session:
            session.close()


def getSmallestTeletaskID():
    session = None
    try:
        session = get_connection()
        min_id = session.execute(select(func.min(VttFileRecord.lecture_id))).scalar_one()
        logger.info(f"Smallest Teletask ID in available in database: {min_id}")
        return min_id
    except SQLAlchemyError as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
    finally:
        if session:
            session.close()


def get_missing_inbetween_ids():
    session = None
    try:
        session = get_connection()
        rows = session.execute(
            text(
                """
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
                """
            )
        ).all()
        return [row[0] for row in rows]
    except SQLAlchemyError as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
    finally:
        if session:
            session.close()


def get_missing_translations():
    session = None
    try:
        session = get_connection()
        rows = session.execute(
            select(VttFileRecord.lecture_id, VttFileRecord.language)
            .where(VttFileRecord.is_original_lang.is_(False))
            .order_by(desc(VttFileRecord.lecture_id))
        ).all()
        return [(row[0], row[1]) for row in rows]
    except SQLAlchemyError as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
    finally:
        if session:
            session.close()
