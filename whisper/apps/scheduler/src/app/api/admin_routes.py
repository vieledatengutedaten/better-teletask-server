import asyncio
import json
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from lib.core.logger import logger
from app.scheduler.queues import queue_manager
from app.scheduler.scheduler import get_scheduler

admin_router = APIRouter()

# --- Broadcast mechanism ---

_connected_clients: set[WebSocket] = set()


async def _get_full_state() -> dict[str, Any]:
    try:
        scheduler = get_scheduler()
        scheduler_snapshot = scheduler.snapshot()
    except RuntimeError:
        scheduler_snapshot = {
            "max_workers": 0,
            "batch_size": 0,
            "available_capacity": 0,
            "active_worker_count": 0,
            "active_workers": {},
            "jobs_by_id": {},
        }
    queue_snapshot = await queue_manager.snapshot()
    return {"scheduler": scheduler_snapshot, "queues": queue_snapshot}


async def broadcast_state() -> None:
    """Send current state to all connected WebSocket clients."""
    if not _connected_clients:
        return
    state = await _get_full_state()
    payload = json.dumps(state, default=str)
    disconnected: list[WebSocket] = []
    for ws in _connected_clients:
        try:
            await ws.send_text(payload)
        except Exception:
            disconnected.append(ws)
    for ws in disconnected:
        _connected_clients.discard(ws)


# --- WebSocket endpoint ---


@admin_router.websocket("/ws")
async def admin_ws(websocket: WebSocket):
    await websocket.accept()
    _connected_clients.add(websocket)
    logger.info(f"Admin WS connected ({len(_connected_clients)} client(s))")
    try:
        # Send full state on connect
        state = await _get_full_state()
        await websocket.send_text(json.dumps(state, default=str))
        # Keep alive — wait for disconnect
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        _connected_clients.discard(websocket)
        logger.info(f"Admin WS disconnected ({len(_connected_clients)} client(s))")


# --- REST endpoints ---


class ConfigUpdate(BaseModel):
    max_workers: int | None = None
    batch_size: int | None = None


@admin_router.patch("/config")
async def update_config(update: ConfigUpdate):
    scheduler = get_scheduler()
    if update.max_workers is not None:
        scheduler.max_workers = update.max_workers
    if update.batch_size is not None:
        scheduler.batch_size = update.batch_size
    await broadcast_state()
    return scheduler.snapshot()


@admin_router.get("/state")
async def get_state():
    return await _get_full_state()
