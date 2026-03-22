from contextlib import contextmanager
from typing import Iterator
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from config import DB_NAME, DB_USER, DB_PASS, DB_HOST, DB_PORT


def _build_database_url() -> str:
    user = quote_plus(DB_USER or "")
    password = quote_plus(DB_PASS or "")
    host = DB_HOST or "localhost"
    port = DB_PORT or "5432"
    database = DB_NAME or "postgres"
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"


DATABASE_URL = _build_database_url()

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True, # keep alive ping for idle connections to prevent timeouts and errors
    future=True,
)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False, # control single operations inside of a transaction manually
    autocommit=False, # explicitly commit so we can also rollback
    expire_on_commit=False, # keep ORM objects in memory after commit
    future=True,
)


@contextmanager
def get_session() -> Iterator[Session]:
    """Yield a SQLAlchemy session with automatic commit/rollback/close."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

