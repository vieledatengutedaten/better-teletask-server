import asyncio

from lib.core.config import ENVIRONMENT


def fire_broadcast() -> None:
    """Schedule a broadcast to all admin WebSocket clients (fire-and-forget).

    No-op unless ENVIRONMENT is 'dev'.
    """
    if ENVIRONMENT != "dev":
        return

    from app.api.admin_routes import broadcast_state

    try:
        asyncio.create_task(broadcast_state())
    except RuntimeError:
        pass  # no running event loop yet
