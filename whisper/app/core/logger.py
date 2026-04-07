import logging
import os
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import requests as _requests

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

ENVIRONMENT = os.getenv("ENVIRONMENT", "server")

# get corresponding logging level from environment variable and match it to logging module levels from logging
_LEVEL_NAME = os.getenv("LOGGING", "INFO").strip().upper()
LEVEL_NAME = getattr(logging, _LEVEL_NAME, logging.INFO)
print(
    f"Logger level set to: {logging.getLevelName(LEVEL_NAME)}, if you see too many or too few messages, adjust the LOGGING variable in the .env file."
)


# Implement own Self Formatter for id field
class SafeFormatter(logging.Formatter):

    def format(self, record):
        # If there is no string, assume its a global log
        if not hasattr(record, "id"):
            record.id = "GLOBAL"
        else:
            record.id = str(record.id)
        return super().format(record)


class SchedulerLogHandler(logging.Handler):
    """Sends log records to the scheduler VM via HTTP."""

    def __init__(
        self, scheduler_url: str, worker_id: str, job_id: str, level=logging.INFO
    ):
        super().__init__(level)
        self.url = f"{scheduler_url}/worker/{worker_id}/jobs/{job_id}/log"

    def emit(self, record):
        try:
            _requests.post(
                self.url,
                json={
                    "message": self.format(record),
                    "level": record.levelname.lower(),
                },
                timeout=5,
            )
        except Exception:
            pass  # don't recurse if the scheduler is down


String_Format = "[%(asctime)s %(levelname)s] [%(id)s] %(message)s"

formatter = SafeFormatter(String_Format, datefmt="%Y-%m-%d %H:%M:%S")

root_logger = logging.getLogger("btt_root_logger")
root_logger.setLevel(logging.DEBUG)
root_logger.propagate = False


if ENVIRONMENT == "worker":
    # Worker mode: send logs to scheduler, also print to stderr
    SCHEDULER_URL = os.environ.get("SCHEDULER_URL", "")
    WORKER_ID = os.environ.get("WORKER_ID", "")
    JOB_ID = os.environ.get("JOB_ID", "")

    if SCHEDULER_URL and JOB_ID:
        scheduler_handler = SchedulerLogHandler(
            SCHEDULER_URL,
            WORKER_ID or JOB_ID,
            JOB_ID,
        )
        scheduler_handler.setLevel(logging.INFO)
        scheduler_handler.setFormatter(formatter)
        root_logger.addHandler(scheduler_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(LEVEL_NAME)
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)

else:
    # Server mode: file + stream logging as before
    BASE_DIR = Path(__file__).parent.parent.parent
    LOG_FILE_PATH = BASE_DIR / "logs"
    LOG_FILE_PATH.mkdir(parents=True, exist_ok=True)

    file_handler = logging.FileHandler(
        LOG_FILE_PATH / "whisper.log", mode="a", encoding="utf-8"
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(LEVEL_NAME)
    stream_handler.setFormatter(formatter)
    root_logger.addHandler(stream_handler)

    access_logger = logging.getLogger("btt_access_logger")
    access_logger.setLevel(logging.DEBUG)
    access_logger.propagate = False
    rotating_file_handler = TimedRotatingFileHandler(
        LOG_FILE_PATH / "access.log",
        encoding="utf-8",
        when="midnight",
        backupCount=14,
        interval=1,
        utc=True,
    )
    access_logger.addHandler(rotating_file_handler)

logger = logging.getLogger("btt_root_logger")

logger.info("Logger initialized.")
