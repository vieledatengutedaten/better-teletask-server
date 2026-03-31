from app.db.vtt_files import get_vtt_by_id_and_lang
from app.db.vtt_files import get_original_vtt_by_id
from fastapi import APIRouter, Header, HTTPException, Depends, Response
from datetime import datetime
from app import db
import httpx

from app.core.logger import logger


async def verify_auth_header(authorization: str | None = Header(default=None)):
    return
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    token = authorization.replace("Bearer ", "").strip()
    if not token:
        raise HTTPException(status_code=401, detail="Invalid Authorization header")

    api_key = db.get_api_key_by_key(token)
    if not api_key:
        raise HTTPException(status_code=401, detail="Invalid API key")

    if api_key.status == "revoked":
        raise HTTPException(status_code=403, detail="API key has been revoked")
    elif api_key.status == "expired":
        raise HTTPException(status_code=403, detail="API key has expired")
    elif api_key.status != "active":
        raise HTTPException(status_code=403, detail="API key is not active")

    if (
        api_key.expiration_date
        and api_key.expiration_date.replace(tzinfo=None) < datetime.now()
    ):
        raise HTTPException(status_code=403, detail="API key has expired")


async def prioritize_lecture(id: int):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"http://localhost:8000/schedule/prioritize/{id}"
            )
            _ = response.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to prioritize subtitle: {e}")


subtitle_router = APIRouter(dependencies=[Depends(verify_auth_header)])


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
        headers={"Access-Control-Allow-Origin": "https://www.tele-task.de"},
    )
