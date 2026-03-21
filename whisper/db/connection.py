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
    pool_pre_ping=True,
    future=True,
)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    expire_on_commit=False,
    future=True,
)


def get_session() -> Session:
    """Create and return a new SQLAlchemy session."""
    return SessionLocal()

