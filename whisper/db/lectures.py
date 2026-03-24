from datetime import datetime

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from db.connection import get_session
from db.error_handling import db_operation
from db.schema import (
    LectureDataRecord,
    LecturerDataRecord,
    SeriesDataRecord,
    VttFileRecord,
)
from models import SeriesData

import logger
import logging

logger = logging.getLogger("btt_root_logger")


@db_operation(success_message="Successfully queried series of VTT file.")
def get_series_of_vtt_file(vtt_file_id) -> SeriesData | None:
    with get_session() as session:
        row = session.execute(
            select(
                SeriesDataRecord.series_id,
                SeriesDataRecord.series_name,
                SeriesDataRecord.lecturer_ids,
            )
            .join(
                LectureDataRecord,
                LectureDataRecord.series_id == SeriesDataRecord.series_id,
            )
            .join(
                VttFileRecord,
                VttFileRecord.lecture_id == LectureDataRecord.lecture_id,
            )
            .where(VttFileRecord.id == vtt_file_id)
        ).first()
        if row:
            return SeriesData(
                series_id=row[0],
                series_name=row[1],
                lecturer_ids=row[2],
            )
        logger.info(f"No series found for VTT file ID: {vtt_file_id}")
        return None


@db_operation(success_message="Successfully queried all lecture IDs.")
def get_all_lecture_ids():
    with get_session() as session:
        rows = session.execute(select(LectureDataRecord.lecture_id)).all()
        ids = [row[0] for row in rows]
        logger.debug(f"Fetched all lecture IDs: {ids}")
        return ids


@db_operation(success_message="Successfully checked whether series ID exists.")
def series_id_exists(series_id):
    with get_session() as session:
        count = session.execute(
            select(func.count())
            .select_from(SeriesDataRecord)
            .where(SeriesDataRecord.series_id == series_id)
        ).scalar_one()
        return count > 0


@db_operation(success_message="Successfully checked whether lecturer ID exists.")
def lecturer_id_exists(lecturer_id):
    with get_session() as session:
        count = session.execute(
            select(func.count())
            .select_from(LecturerDataRecord)
            .where(LecturerDataRecord.lecturer_id == lecturer_id)
        ).scalar_one()
        return count > 0


@db_operation(
    success_message="Successfully added lecture data for Lecture ID {lecture_data[lecture_id]}."
)
def add_lecture_data(lecture_data):
    with get_session() as session:

        teletaskid = lecture_data["lecture_id"]
        lecturer_ids = lecture_data["lecturer_ids"]
        lecturer_names = lecture_data["lecturer_names"]
        lecture_date = datetime.strptime(lecture_data["date"], "%B %d, %Y").date()
        language = "en" if lecture_data["language"] == "English" else "de"
        duration = lecture_data["duration"]
        lecture_title = lecture_data["lecture_title"]
        series_id = lecture_data["series_id"]
        series_name = lecture_data["series_name"]
        url = lecture_data["url"]

        if lecture_date.month < 3 or lecture_date.month > 10:
            semester = f"WT {lecture_date.year-1}/{lecture_date.year}"
        else:
            semester = f"ST {lecture_date.year}"

        for lecturer_id, lecturer_name in zip(lecturer_ids, lecturer_names):
            exists = session.execute(
                select(func.count())
                .select_from(LecturerDataRecord)
                .where(LecturerDataRecord.lecturer_id == lecturer_id)
            ).scalar_one()
            if not exists:
                stmt = pg_insert(LecturerDataRecord).values(
                    lecturer_id=lecturer_id,
                    lecturer_name=lecturer_name,
                )
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=[LecturerDataRecord.lecturer_id]
                )
                session.execute(stmt)
                logger.info(
                    f"Added lecturer data for Lecturer ID {lecturer_id}.",
                    extra={"id": teletaskid},
                )

        series_exists = session.execute(
            select(func.count())
            .select_from(SeriesDataRecord)
            .where(SeriesDataRecord.series_id == series_id)
        ).scalar_one()

        if not series_exists:
            stmt = pg_insert(SeriesDataRecord).values(
                series_id=series_id,
                series_name=series_name,
                lecturer_ids=lecturer_ids,
            )
            stmt = stmt.on_conflict_do_nothing(
                index_elements=[SeriesDataRecord.series_id]
            )
            session.execute(stmt)
            logger.info(
                f"Added series data for Series ID {series_id}.",
                extra={"id": teletaskid},
            )
        else:
            session.execute(
                text("""
                    UPDATE series_data
                    SET lecturer_ids = array(
                        SELECT DISTINCT unnest(lecturer_ids || CAST(:lecturer_ids AS INTEGER[]))
                    )
                    WHERE series_id = :series_id;
                    """),
                {
                    "lecturer_ids": lecturer_ids,
                    "series_id": series_id,
                },
            )
            logger.info(
                f"Updated lecturer IDs for Series ID {series_id}.",
                extra={"id": teletaskid},
            )

        session.execute(
            pg_insert(LectureDataRecord).values(
                lecture_id=teletaskid,
                language=language,
                date=lecture_date,
                lecturer_ids=lecturer_ids,
                series_id=series_id,
                semester=semester,
                duration=duration,
                title=lecture_title,
                video_mp4=url,
            )
        )


@db_operation(success_message="Successfully queried lecture language.")
def get_language_of_lecture(teletaskid) -> str:
    with get_session() as session:
        language = session.execute(
            select(LectureDataRecord.language).where(
                LectureDataRecord.lecture_id == teletaskid
            )
        ).scalar_one_or_none()

        if language:
            return language
        logger.info(f"No lecture data found for Teletask ID: {teletaskid}")
        return None
