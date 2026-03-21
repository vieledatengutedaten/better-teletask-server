from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError

from db.connection import get_session
from db.schema import ApiKeyRecord
from models import ApiKey

import logger
import logging
logger = logging.getLogger("btt_root_logger")


def _to_api_key(record: ApiKeyRecord) -> ApiKey:
    return ApiKey(
        id=record.id,
        api_key=record.api_key,
        person_name=record.person_name,
        person_email=record.person_email,
        creation_date=record.creation_date,
        expiration_date=record.expiration_date,
        status=record.status,
    )


def add_api_key(api_key, person_name, person_email):
    session = None
    try:
        session = get_session()
        stmt = insert(ApiKeyRecord).values(
            api_key=api_key,
            person_name=person_name,
            person_email=person_email,
        )
        stmt = stmt.on_conflict_do_nothing(index_elements=[ApiKeyRecord.api_key])
        session.execute(stmt)
        session.commit()
    except SQLAlchemyError as error:
        logger.error(f"Error while connecting to PostgreSQL: {error}")
    finally:
        if session:
            session.close()


def get_api_key_by_key(api_key) -> ApiKey | None:
    session = None
    try:
        session = get_session()
        record = session.execute(
            select(ApiKeyRecord).where(ApiKeyRecord.api_key == api_key)
        ).scalar_one_or_none()
        if record:
            return _to_api_key(record)
        logger.info(f"No API key found: {api_key}")
        return None
    except SQLAlchemyError as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
        return None
    finally:
        if session:
            session.close()


def get_api_key_by_name(person_name) -> list[ApiKey] | None:
    session = None
    try:
        session = get_session()
        records = session.execute(
            select(ApiKeyRecord).where(ApiKeyRecord.person_name == person_name)
        ).scalars().all()

        api_keys = [_to_api_key(record) for record in records]
        if api_keys:
            return api_keys
        logger.info(f"No API key found for person name: {person_name}")
        return None
    except SQLAlchemyError as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
        return None
    finally:
        if session:
            session.close()


def get_all_api_keys() -> list[ApiKey]:
    session = None
    try:
        session = get_session()
        records = session.execute(select(ApiKeyRecord)).scalars().all()
        return [_to_api_key(record) for record in records]
    except SQLAlchemyError as error:
        logger.error(f"Error while querying PostgreSQL: {error}")
        return []
    finally:
        if session:
            session.close()


def remove_api_key(api_key):
    session = None
    try:
        session = get_session()
        session.execute(
            delete(ApiKeyRecord).where(ApiKeyRecord.api_key == api_key)
        )
        session.commit()
        logger.info(f"Successfully removed API key: {api_key}")
    except SQLAlchemyError as error:
        logger.error(f"Error while connecting to PostgreSQL: {error}")
    finally:
        if session:
            session.close()
