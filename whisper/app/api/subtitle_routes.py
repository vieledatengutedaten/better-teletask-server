from app.db.vtt_files import get_vtt_by_id_and_lang
from app.db.vtt_files import get_original_vtt_by_id
from fastapi import APIRouter, HTTPException, Response
import httpx

from app.core.logger import logger


async def prioritize_lecture(id: int):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"http://localhost:8000/schedule/prioritize/{id}" #TODO parameterize host and port
            )
            _ = response.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to prioritize subtitle: {e}")


subtitle_router = APIRouter()


@subtitle_router.get("/{id}")
@subtitle_router.get("/{id}/{lang}")
async def get_subtitle(id: int, lang: str | None = None):
    if lang is None:
        vtt_file = get_original_vtt_by_id(id)
    else:
        vtt_file = get_vtt_by_id_and_lang(id, lang)

    if vtt_file is None:
        await prioritize_lecture(id)
        raise HTTPException(status_code=404, detail="Subtitle not found")

    return Response(
        content=vtt_file,
        media_type="text/vtt; charset=utf-8",
    )
