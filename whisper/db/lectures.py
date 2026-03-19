import psycopg2
from datetime import datetime
from db.connection import get_connection
from models import SeriesData

import logging
logger = logging.getLogger("btt_root_logger")


def get_series_of_vtt_file(vtt_file_id) -> SeriesData | None:
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT s.series_id, s.series_name, s.lecturer_ids
            FROM vtt_files vf
            JOIN lecture_data ld ON vf.lecture_id = ld.lecture_id
            JOIN series_data s ON ld.series_id = s.series_id
            WHERE vf.id = %s;
            """,
            (vtt_file_id,),
        )
        row = cur.fetchone()
        if row:
            return SeriesData(
                series_id=row[0],
                series_name=row[1],
                lecturer_ids=row[2],
            )
        logger.info(f"No series found for VTT file ID: {vtt_file_id}")
        return None
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
        return None
    finally:
        if conn:
            cur.close()
            conn.close()


def get_all_lecture_ids():
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT lecture_id FROM lecture_data;")
        rows = cur.fetchall()
        ids = [row[0] for row in rows]
        logger.debug(f"Fetched all lecture IDs: {ids}")
        return ids
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
        return []
    finally:
        if conn:
            cur.close()
            conn.close()


def series_id_exists(series_id):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM series_data WHERE series_id = %s;",
            (series_id,),
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


def lecturer_id_exists(lecturer_id):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM lecturer_data WHERE lecturer_id = %s;",
            (lecturer_id,),
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


def add_lecture_data(lecture_data):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        teletaskid = lecture_data['lecture_id']
        lecturer_ids = lecture_data['lecturer_ids']
        lecturer_names = lecture_data['lecturer_names']
        date = lecture_data['date']
        date = datetime.strptime(date, "%B %d, %Y")
        language = lecture_data['language']
        language = "en" if language == "English" else "de"
        duration = lecture_data['duration']
        lecture_title = lecture_data['lecture_title']
        series_id = lecture_data['series_id']
        series_name = lecture_data['series_name']
        url = lecture_data['url']

        if date.month < 3 or date.month > 10:
            semester = f"WT {date.year-1}/{date.year}"
        else:
            semester = f"ST {date.year}"

        for lecturer_id in lecturer_ids:
            if not lecturer_id_exists(lecturer_id):
                cur.execute(
                    "INSERT INTO lecturer_data (lecturer_id, lecturer_name) VALUES (%s, %s) ON CONFLICT (lecturer_id) DO NOTHING;",
                    (
                        lecturer_id,
                        lecturer_names[lecturer_ids.index(lecturer_id)]
                    ),
                )
                logger.info(f"Added lecturer data for Lecturer ID {lecturer_id}.", extra={'id': teletaskid})
                conn.commit()

        if not series_id_exists(series_id):
            cur.execute(
                "INSERT INTO series_data (series_id, series_name, lecturer_ids) VALUES (%s, %s, %s::INTEGER[]) ON CONFLICT (series_id) DO NOTHING;",
                (
                    series_id,
                    series_name,
                    lecturer_ids
                ),
            )
            logger.info(f"Added series data for Series ID {series_id}.", extra={'id': teletaskid})
            conn.commit()
        else:
            cur.execute(
                "UPDATE series_data SET lecturer_ids = array(SELECT DISTINCT unnest(lecturer_ids || CAST(%s AS INTEGER[]))) WHERE series_id = %s;",
                (
                    lecturer_ids,
                    series_id
                ),
            )
            logger.info(f"Updated lecturer IDs for Series ID {series_id}.", extra={'id': teletaskid})
            conn.commit()

        cur.execute(
            "INSERT INTO lecture_data (lecture_id, language, date, lecturer_ids, series_id, semester, duration, title, video_mp4) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);",
            (
                teletaskid,
                language,
                date,
                lecturer_ids,
                series_id,
                semester,
                duration,
                lecture_title,
                url
            ),
        )

        conn.commit()

    except (psycopg2.Error) as error:
        logger.error(f"Error while connecting to PostgreSQL: {error}")
    finally:
        if conn:
            cur.close()
            conn.close()


def get_language_of_lecture(teletaskid) -> str:
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT language FROM lecture_data WHERE lecture_id = %s;",
            (teletaskid,),
        )
        row = cur.fetchone()

        if row:
            return row[0]
        else:
            logger.info(f"No lecture data found for Teletask ID: {teletaskid}")
            return None
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
        return None
    finally:
        if conn:
            cur.close()
            conn.close()
