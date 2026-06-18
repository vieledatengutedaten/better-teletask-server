from sqlalchemy import text

from app.db.connection import engine
from app.db.error_handling import db_operation
from app.db.schema import Base

from lib.core.logger import logger


@db_operation(success_message="Initialized database schema.")
def initDatabase():
    with engine.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm;"))
        Base.metadata.create_all(bind=conn)


@db_operation(success_message="Cleared database schema.")
def clearDatabase():
    with engine.begin() as conn:
        Base.metadata.drop_all(bind=conn)
