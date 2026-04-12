from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.httpsredirect import HTTPSRedirectMiddleware

from lib.core.config import CORS_ORIGINS, HTTPS_REDIRECT
from app.middleware.auth import AuthMiddleware
from app.middleware.logging import RequestLoggingMiddleware


def register_middleware(app: FastAPI):
    """Register all middleware on the app.

    Order: last added = outermost (first to execute on request).
    Execution order: HTTPS redirect -> CORS -> Logging -> Auth -> route
    """

    app.add_middleware(AuthMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=CORS_ORIGINS,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    if HTTPS_REDIRECT:
        app.add_middleware(HTTPSRedirectMiddleware)

    app.add_middleware(RequestLoggingMiddleware)
