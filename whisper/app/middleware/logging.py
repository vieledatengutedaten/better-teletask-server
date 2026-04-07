import time

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

from app.core.logger import access_logger


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Logs method, path, status code, and duration for every request."""

    async def dispatch(self, request: Request, call_next):
        start = time.time()
        response = await call_next(request)
        duration = time.time() - start
        # Prefer X-Forwarded-For (when behind a proxy), otherwise use request.client
        xff = request.headers.get("x-forwarded-for")
        if xff:
            client_ip = xff.split(",")[0].strip()
        else:
            client = request.client
            client_ip = client.host if client else "unknown"

        print(
            f"DAS IST PRINT {request.method} {request.url.path} {response.status_code} {duration:.3f}s from {client_ip}"
        )
        access_logger.info(
            "%s %s %s %d %.3fs",
            client_ip,
            request.method,
            request.url.path,
            response.status_code,
            duration,
        )
        return response
