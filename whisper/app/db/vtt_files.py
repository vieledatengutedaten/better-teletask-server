import os

from sqlalchemy import desc, func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.db.connection import get_session
from app.db.error_handling import db_operation
from app.db.schema import VttFileRecord
from app.models import VttFile
from app.core.config import OUTPUT_PATH, ASR_MODEL, COMPUTE_TYPE

from app.core import logger
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


@db_operation(success_message="Successfully queried all original VTT IDs.")
def get_all_original_vtt_ids():
    with get_session() as session:
        rows = session.execute(
            select(VttFileRecord.lecture_id).where(
                VttFileRecord.is_original_lang.is_(True)
            )
        ).all()
        ids = [row[0] for row in rows]
        logger.debug(f"Fetched all original VTT IDs: {ids}")
        return ids


@db_operation(success_message="Successfully checked whether original language exists.")
def original_language_exists(teletaskid):
    with get_session() as session:
        count = session.execute(
            select(func.count())
            .select_from(VttFileRecord)
            .where(
                VttFileRecord.lecture_id == teletaskid,
                VttFileRecord.is_original_lang.is_(True),
            )
        ).scalar_one()
        return count > 0


@db_operation(
    success_message="Successfully saved VTT/TXT as BLOB for lecture ID {teletaskid}."
)
def save_vtt_as_blob(teletaskid, language, isOriginalLang):
    file_path = os.path.join(OUTPUT_PATH, str(teletaskid) + ".vtt")
    file_path_txt = os.path.join(OUTPUT_PATH, str(teletaskid) + ".txt")
    if not os.path.exists(file_path):
        logger.error(
            f"VTT file not found, cant put in database: {file_path}",
            extra={"id": teletaskid},
        )
        return -1
    if not os.path.exists(file_path_txt):
        logger.error(
            f"TXT file not found, cant put in database: {file_path_txt}",
            extra={"id": teletaskid},
        )
        return -1
    with get_session() as session:
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
    return vtt_file_id


@db_operation(success_message="Successfully queried VTT file by ID.")
def get_vtt_file_by_id(vtt_file_id) -> VttFile | None:
    with get_session() as session:
        record = session.execute(
            select(VttFileRecord).where(VttFileRecord.id == vtt_file_id)
        ).scalar_one_or_none()
        if record:
            return _to_vtt_file(record)
        logger.info(f"No VTT file found with ID: {vtt_file_id}")
        return None


@db_operation(success_message="Successfully queried VTT files by lecture ID.")
def get_vtt_files_by_lecture_id(lecture_id) -> list[VttFile]:
    with get_session() as session:
        records = (
            session.execute(
                select(VttFileRecord).where(VttFileRecord.lecture_id == lecture_id)
            )
            .scalars()
            .all()
        )
        return [_to_vtt_file(record) for record in records]


@db_operation(success_message="Successfully queried all VTT blobs.")
def get_all_vtt_blobs() -> list[VttFile]:
    with get_session() as session:
        records = (
            session.execute(select(VttFileRecord).order_by(VttFileRecord.id))
            .scalars()
            .all()
        )
        logger.info(f"Retrieved {len(records)} VTT file(s) from database.")
        return [_to_vtt_file(record) for record in records]


@db_operation(success_message="Successfully queried original language by lecture ID.")
def get_original_language_by_id(teletaskid):
    with get_session() as session:
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


@db_operation(success_message="Successfully queried original VTT by lecture ID.")
def get_original_vtt_by_id(teletaskid):
    with get_session() as session:
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


@db_operation(success_message="Successfully queried highest Teletask ID.")
def getHighestTeletaskID():
    with get_session() as session:
        max_id = session.execute(
            select(func.max(VttFileRecord.lecture_id))
        ).scalar_one()
        logger.info(f"Highest Teletask ID in available in database: {max_id}")
        return max_id


@db_operation(success_message="Successfully queried smallest Teletask ID.")
def getSmallestTeletaskID():
    with get_session() as session:
        min_id = session.execute(
            select(func.min(VttFileRecord.lecture_id))
        ).scalar_one()
        logger.info(f"Smallest Teletask ID in available in database: {min_id}")
        return min_id


@db_operation(success_message="Successfully queried missing in-between IDs.")
def get_missing_inbetween_ids():
    with get_session() as session:
        rows = session.execute(text("""
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
                """)).all()
        return [row[0] for row in rows]


@db_operation(success_message="Successfully queried missing translations.")
def get_missing_translations():
    with get_session() as session:
        rows = session.execute(
            select(VttFileRecord.lecture_id, VttFileRecord.language)
            .where(VttFileRecord.is_original_lang.is_(False))
            .order_by(desc(VttFileRecord.lecture_id))
        ).all()
        return [(row[0], row[1]) for row in rows]


@db_operation(
    success_message="Successfully queried VTT file by ID {lecture_id} and language {language}."
)
def get_vtt_by_id_and_lang(lecture_id, language):
    with get_session() as session:
        vtt_data = session.execute(
            select(VttFileRecord.vtt_data).where(
                VttFileRecord.lecture_id == lecture_id,
                VttFileRecord.language == language,
            )
        ).scalar_one_or_none()
        if vtt_data:
            return bytes(vtt_data).decode("utf-8")
        logger.info(
            f"No VTT file found for Teletask ID: {lecture_id} and language: {language}"
        )
        return None
