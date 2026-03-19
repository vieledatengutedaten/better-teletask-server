import psycopg2
from config import DB_NAME, DB_USER, DB_PASS, DB_HOST, DB_PORT


def get_connection():
    """Create and return a new database connection."""
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT
    )
