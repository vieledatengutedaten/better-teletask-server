from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from db.connection import engine
from db.schema import Base

import logger
import logging
logger = logging.getLogger("btt_root_logger")


def initDatabase():
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm;"))
            Base.metadata.create_all(bind=conn)
        logger.info("Initialized database schema.")
    except SQLAlchemyError as error:
        logger.error(f"Error while connecting to PostgreSQL: {error}")


def clearDatabase():
    try:
        with engine.begin() as conn:
            Base.metadata.drop_all(bind=conn)
    except SQLAlchemyError as error:
        logger.error(f"Error while connecting to PostgreSQL: {error}")
