import uvicorn
from fastapi import FastAPI

from app.api.subtitle_routes import subtitle_router
from app.api.search_routes import search_router
from app.middleware import register_middleware

# setup logging — must be imported before other modules to configure handlers
from app.core.logger import logger  # noqa: F401


app = FastAPI()
register_middleware(app)


app.include_router(router=subtitle_router, prefix="/subtitle")
app.include_router(router=search_router, prefix="/search")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
