from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert

from app.db.connection import get_session
from app.db.error_handling import db_operation
from app.db.schema import ApiKeyRecord
from app.models import ApiKey

from app.core import logger
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


@db_operation(success_message="Successfully added API key.")
def add_api_key(api_key, person_name, person_email):
    with get_session() as session:
        stmt = insert(ApiKeyRecord).values(
            api_key=api_key,
            person_name=person_name,
            person_email=person_email,
        )
        stmt = stmt.on_conflict_do_nothing(index_elements=[ApiKeyRecord.api_key])
        session.execute(stmt)


@db_operation(success_message="Successfully queried API key by key.")
def get_api_key_by_key(api_key) -> ApiKey | None:
    with get_session() as session:
        record = session.execute(
            select(ApiKeyRecord).where(ApiKeyRecord.api_key == api_key)
        ).scalar_one_or_none()
        if record:
            return _to_api_key(record)
        logger.info(f"No API key found: {api_key}")
        return None


@db_operation(success_message="Successfully queried API keys by person name.")
def get_api_key_by_name(person_name) -> list[ApiKey] | None:
    with get_session() as session:
        records = (
            session.execute(
                select(ApiKeyRecord).where(ApiKeyRecord.person_name == person_name)
            )
            .scalars()
            .all()
        )

        api_keys = [_to_api_key(record) for record in records]
        if api_keys:
            return api_keys
        logger.info(f"No API key found for person name: {person_name}")
        return None


@db_operation(success_message="Successfully queried all API keys.")
def get_all_api_keys() -> list[ApiKey]:
    with get_session() as session:
        records = session.execute(select(ApiKeyRecord)).scalars().all()
        return [_to_api_key(record) for record in records]


@db_operation(success_message="Successfully removed API key: {api_key}")
def remove_api_key(api_key):
    with get_session() as session:
        session.execute(delete(ApiKeyRecord).where(ApiKeyRecord.api_key == api_key))
