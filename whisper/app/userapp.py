from app.api.search_routes import search_router
import uvicorn
import asyncio
import contextlib
from fastapi import FastAPI
from app.api.subtitle_routes import subtitle_router
from app.api.scheduling_routes import schedule_router

# setup logging — must be imported before other modules to configure handlers
from app.core.logger import logger
app = FastAPI()

app.include_router(router=subtitle_router, prefix="/subtitle")
app.include_router(router=search_router, prefix="/search")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
