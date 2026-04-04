import logging
import os
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

# get corresponding logging level from environment variable and match it to logging module levels from logging
LEVEL_NAME = os.getenv("LOGGING", "INFO").strip().upper()
LEVEL_NAME = getattr(logging, LEVEL_NAME, logging.INFO)
print(
    f"Logger level set to: {logging.getLevelName(LEVEL_NAME)}, if you see too many or too few messages, adjust the LOGGING variable in the .env file."
)


# Ensure the logs directory exists
BASE_DIR = Path(__file__).parent.parent.parent
LOG_FILE_PATH = BASE_DIR / "logs"
LOG_FILE_PATH.mkdir(parents=True, exist_ok=True)


# Implement own Self Formatter for id field
class SafeFormatter(logging.Formatter):

    def format(self, record):
        # If there is no string, assume its a global log
        if not hasattr(record, "id"):
            record.id = "GLOBAL"
        else:
            record.id = str(record.id)
        return super().format(record)


String_Format = "[%(asctime)s %(levelname)s] [%(id)s] %(message)s"

formatter = SafeFormatter(String_Format, datefmt="%Y-%m-%d %H:%M:%S")

root_logger = logging.getLogger("btt_root_logger")
root_logger.setLevel(logging.DEBUG)  # This must let everything pass
root_logger.propagate = False  # Prevent duplicate logging to Python's root logger


file_handler = logging.FileHandler(
    LOG_FILE_PATH / "whisper.log", mode="a", encoding="utf-8"
)
file_handler.setLevel(
    logging.INFO
)  # Everything to file, could have multiple file handlers with different levels
file_handler.setFormatter(formatter)
root_logger.addHandler(file_handler)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(LEVEL_NAME)  # Set its specific filter to WARNING
stream_handler.setFormatter(formatter)
root_logger.addHandler(stream_handler)

access_logger = logging.getLogger("btt_access_logger")
access_logger.setLevel(logging.INFO)
access_logger.propagate = False  # Prevent duplicate logging to Python's root logger
rotating_file_handler = TimedRotatingFileHandler(
    LOG_FILE_PATH / "access.log",
    encoding="utf-8",
    when="midnight",
    backupCount=14,
    interval=1,
    utc=True,
)

logger = logging.getLogger("btt_root_logger")

logger.info("Logger initialized.")
