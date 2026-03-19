import psycopg2
from db.connection import get_connection
from models import ApiKey

import logging
logger = logging.getLogger("btt_root_logger")


def add_api_key(api_key, person_name, person_email):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO api_keys (api_key, person_name, person_email) VALUES (%s, %s, %s) ON CONFLICT (api_key) DO NOTHING;",
            (api_key, person_name, person_email),
        )
        conn.commit()
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while connecting to PostgreSQL: {error}")
    finally:
        if conn:
            cur.close()
            conn.close()


def get_api_key_by_key(api_key) -> ApiKey | None:
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT api_key, person_name, person_email, creation_date, expiration_date, status FROM api_keys WHERE api_key = %s;",
            (api_key,),
        )
        row = cur.fetchone()
        if row:
            return ApiKey(
                api_key=row[0],
                person_name=row[1],
                person_email=row[2],
                creation_date=row[3],
                expiration_date=row[4],
                status=row[5],
            )
        else:
            logger.info(f"No API key found: {api_key}")
            return None
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
        return None
    finally:
        if conn:
            cur.close()
            conn.close()


def get_api_key_by_name(person_name) -> list[ApiKey] | None:
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT api_key, person_name, person_email, creation_date, expiration_date, status FROM api_keys WHERE person_name = %s;",
            (person_name,),
        )
        rows = cur.fetchall()
        api_keys = [
            ApiKey(
                api_key=row[0],
                person_name=row[1],
                person_email=row[2],
                creation_date=row[3],
                expiration_date=row[4],
                status=row[5],
            )
            for row in rows
        ]
        if api_keys:
            return api_keys
        else:
            logger.info(f"No API key found for person name: {person_name}")
            return None
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
        return None
    finally:
        if conn:
            cur.close()
            conn.close()


def get_all_api_keys() -> list[ApiKey]:
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT api_key, person_name, person_email, creation_date, expiration_date, status FROM api_keys;")
        rows = cur.fetchall()
        return [
            ApiKey(
                api_key=row[0],
                person_name=row[1],
                person_email=row[2],
                creation_date=row[3],
                expiration_date=row[4],
                status=row[5],
            )
            for row in rows
        ]
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
        return []
    finally:
        if conn:
            cur.close()
            conn.close()


def remove_api_key(api_key):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM api_keys WHERE api_key = %s;",
            (api_key,)
        )
        conn.commit()
        logger.info(f"Successfully removed API key: {api_key}")
    except (Exception, psycopg2.Error) as error:
        logger.error(f"Error while connecting to PostgreSQL: {error}")
    finally:
        if conn:
            cur.close()
            conn.close()
