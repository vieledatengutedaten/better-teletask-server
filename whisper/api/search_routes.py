from fastapi import APIRouter, Header, HTTPException, Depends
from datetime import datetime
from models.dataclasses import SearchResult
from db.vtt_lines import search_vtt_lines

import logger
import logging

logger = logging.getLogger("btt_root_logger")


search_router = APIRouter()


# uses query params, for example "localhost:8000/search/fuzzy?q=drittes%20Semester"
@search_router.get("/fuzzy")
async def fuzzy_search(
    q: str,
    series_id: int | None = None,
    language: str | None = None,
    lecturer_id: int | None = None,
    lecture_id: int | None = None,
):
    # TODO handle further logic
    search_results: list[SearchResult] = search_vtt_lines(
        query=q,
        series_id=series_id,
        language=language,
        lecturer_id=lecturer_id,
        lecture_id=lecture_id,
    )
    return [result.model_dump() for result in search_results]
