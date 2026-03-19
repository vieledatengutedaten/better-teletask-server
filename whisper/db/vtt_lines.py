import psycopg2
from psycopg2.extras import execute_values
from db.connection import get_connection
from models import VttLine, SearchResult

import logging
logger = logging.getLogger("btt_root_logger")


def bulk_insert_vtt_lines(vtt_lines: list[VttLine]):
    if not vtt_lines:
        return
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        values = [
            (line.vtt_file_id, line.series_id, line.language, line.lecturer_ids, line.line_number, line.ts_start, line.ts_end, line.content)
            for line in vtt_lines
        ]
        execute_values(
            cur,
            "INSERT INTO vtt_lines (vtt_file_id, series_id, language, lecturer_ids, line_number, ts_start, ts_end, content) VALUES %s",
            values,
            page_size=1000
        )
        conn.commit()
        logger.info(f"Bulk inserted {len(vtt_lines)} VTT lines.")
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while bulk inserting VTT lines: {error}")
    finally:
        if conn:
            cur.close()
            conn.close()


def search_vtt_lines(
    query: str,
    series_id: int | None = None,
    language: str | None = None,
    lecturer_id: int | None = None,
    lecture_id: int | None = None,
    threshold: float = 0.15,
    limit: int = 20,
) -> list[SearchResult]:
    """Fuzzy search vtt_lines using pg_trgm similarity.

    Optional filters:
        series_id   - restrict to a specific series
        language     - restrict to a language (e.g. 'en', 'de')
        lecturer_id  - restrict to lines whose lecturer_ids array contains this id
        lecture_id   - restrict to a specific lecture (via vtt_files)
        threshold    - minimum similarity score (0-1, lower = more results)
        limit        - max rows returned
    """
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        filters = ["similarity(vl.content, %s) >= %s"]
        params: list = [query, threshold]

        if series_id is not None:
            filters.append("vl.series_id = %s")
            params.append(series_id)

        if language is not None:
            filters.append("vl.language = %s")
            params.append(language)

        if lecturer_id is not None:
            filters.append("%s = ANY(vl.lecturer_ids)")
            params.append(lecturer_id)

        if lecture_id is not None:
            filters.append("vf.lecture_id = %s")
            params.append(lecture_id)

        where = " AND ".join(filters)
        params.append(limit)

        sql = f"""
            SELECT
                vl.vtt_file_id,
                vf.lecture_id,
                vl.series_id,
                sd.series_name,
                vl.language,
                vl.line_number,
                vl.ts_start,
                vl.ts_end,
                vl.content,
                similarity(vl.content, %s) AS similarity
            FROM vtt_lines vl
            JOIN vtt_files vf   ON vf.id = vl.vtt_file_id
            JOIN series_data sd ON sd.series_id = vl.series_id
            WHERE {where}
            ORDER BY similarity DESC
            LIMIT %s
        """

        # The first %s in the SELECT is for similarity(); prepend query again
        cur.execute(sql, [query] + params)
        rows = cur.fetchall()

        return [
            SearchResult(
                vtt_file_id=row[0],
                lecture_id=row[1],
                series_id=row[2],
                series_name=row[3],
                language=row[4],
                line_number=row[5],
                ts_start=row[6],
                ts_end=row[7],
                content=row[8],
                similarity=row[9],
            )
            for row in rows
        ]
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while searching VTT lines: {error}")
        return []
    finally:
        if conn:
            cur.close()
            conn.close()
