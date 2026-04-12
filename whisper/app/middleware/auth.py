from datetime import datetime
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from lib.core.config import ENVIRONMENT

from app import db


class AuthMiddleware(BaseHTTPMiddleware):
    """Validates API key from the Authorization header on every request."""

    async def dispatch(self, request: Request, call_next):

        if (ENVIRONMENT == "dev"):
            return await call_next(request)

        authorization = request.headers.get("authorization")
        if not authorization:
            return JSONResponse(
                status_code=401, content={"detail": "Missing Authorization header"}
            )

        token = authorization.replace("Bearer ", "").strip()
        if not token:
            return JSONResponse(
                status_code=401, content={"detail": "Invalid Authorization header"}
            )

        api_key = db.get_api_key_by_key(token)
        if not api_key:
            return JSONResponse(status_code=401, content={"detail": "Invalid API key"})

        if api_key.status == "revoked":
            return JSONResponse(
                status_code=403, content={"detail": "API key has been revoked"}
            )
        elif api_key.status == "expired":
            return JSONResponse(
                status_code=403, content={"detail": "API key has expired"}
            )
        elif api_key.status != "active":
            return JSONResponse(
                status_code=403, content={"detail": "API key is not active"}
            )

        if (
            api_key.expiration_date
            and api_key.expiration_date.replace(tzinfo=None) < datetime.now()
        ):
            return JSONResponse(
                status_code=403, content={"detail": "API key has expired"}
            )

        return await call_next(request)
