from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.db.connection import get_session
from app.db.error_handling import db_operation
from app.db.schema import VttLineRecord
from app.models import VttLine, SearchResult

from app.core.logger import logger


@db_operation(success_message="Successfully bulk inserted {vtt_lines_len} VTT lines.")
def bulk_insert_vtt_lines(vtt_lines: list[VttLine]):
    if not vtt_lines:
        return

    with get_session() as session:
        values = [
            {
                "vtt_file_id": line.vtt_file_id,
                "series_id": line.series_id,
                "language": line.language,
                "lecturer_ids": line.lecturer_ids,
                "line_number": line.line_number,
                "ts_start": line.ts_start,
                "ts_end": line.ts_end,
                "content": line.content,
            }
            for line in vtt_lines
        ]

        session.execute(pg_insert(VttLineRecord), values)


@db_operation(success_message="Successfully searched VTT lines.")
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
    with get_session() as session:
        filters = ["similarity(vl.content, :query_where) >= :threshold"]
        params: dict[str, object] = {
            "query_where": query,
            "query_select": query,
            "threshold": threshold,
            "limit": limit,
        }

        if series_id is not None:
            filters.append("vl.series_id = :series_id")
            params["series_id"] = series_id

        if language is not None:
            filters.append("vl.language = :language")
            params["language"] = language

        if lecturer_id is not None:
            filters.append(":lecturer_id = ANY(vl.lecturer_ids)")
            params["lecturer_id"] = lecturer_id

        if lecture_id is not None:
            filters.append("vf.lecture_id = :lecture_id")
            params["lecture_id"] = lecture_id

        where_clause = " AND ".join(filters)
        sql = text(f"""
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
                similarity(vl.content, :query_select) AS similarity
            FROM vtt_lines vl
            JOIN vtt_files vf   ON vf.id = vl.vtt_file_id
            JOIN series_data sd ON sd.series_id = vl.series_id
            WHERE {where_clause}
            ORDER BY similarity DESC
            LIMIT :limit
            """)

        rows = session.execute(sql, params).all()

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
