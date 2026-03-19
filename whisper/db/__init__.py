from db.connection import get_connection
from db.migrations import initDatabase, clearDatabase
from db.lectures import (
    get_series_of_vtt_file,
    get_all_lecture_ids,
    series_id_exists,
    lecturer_id_exists,
    add_lecture_data,
    get_language_of_lecture,
)
from db.vtt_files import (
    get_all_original_vtt_ids,
    original_language_exists,
    save_vtt_as_blob,
    get_vtt_file_by_id,
    get_vtt_files_by_lecture_id,
    get_all_vtt_blobs,
    get_original_language_by_id,
    get_original_vtt_by_id,
    getHighestTeletaskID,
    getSmallestTeletaskID,
    get_missing_inbetween_ids,
    get_missing_translations,
)
from db.vtt_lines import bulk_insert_vtt_lines, search_vtt_lines
from db.api_keys import (
    add_api_key,
    get_api_key_by_key,
    get_api_key_by_name,
    get_all_api_keys,
    remove_api_key,
)
from db.blacklist import (
    add_id_to_blacklist,
    get_blacklisted_ids,
    get_missing_available_inbetween_ids,
)

__all__ = [
    # connection
    "get_connection",
    # migrations
    "initDatabase",
    "clearDatabase",
    # lectures
    "get_series_of_vtt_file",
    "get_all_lecture_ids",
    "series_id_exists",
    "lecturer_id_exists",
    "add_lecture_data",
    "get_language_of_lecture",
    # vtt_files
    "get_all_original_vtt_ids",
    "original_language_exists",
    "save_vtt_as_blob",
    "get_vtt_file_by_id",
    "get_vtt_files_by_lecture_id",
    "get_all_vtt_blobs",
    "get_original_language_by_id",
    "get_original_vtt_by_id",
    "getHighestTeletaskID",
    "getSmallestTeletaskID",
    "get_missing_inbetween_ids",
    "get_missing_translations",
    # vtt_lines
    "bulk_insert_vtt_lines",
    "search_vtt_lines",
    # api_keys
    "add_api_key",
    "get_api_key_by_key",
    "get_api_key_by_name",
    "get_all_api_keys",
    "remove_api_key",
    # blacklist
    "add_id_to_blacklist",
    "get_blacklisted_ids",
    "get_missing_available_inbetween_ids",
]
