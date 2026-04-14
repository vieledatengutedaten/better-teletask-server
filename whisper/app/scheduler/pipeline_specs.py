"""Pipeline callbacks used by registry JobTypeSpec entries."""

from lib.models.jobs import (
    BaseJob,
    ScrapeLectureDataJob,
    ScrapeLectureDataParams,
    TARGET_LANGUAGES,
    TranscriptionJob,
    TranscriptionParams,
    TranslationJob,
    TranslationParams,
)
from app.db.lectures import get_all_lecture_ids, get_language_of_lecture
from app.db.vtt_files import (
    get_all_original_vtt_ids,
    get_missing_translations,
    original_language_exists,
)


def scrape_factory(tid: int, priority: int) -> list[BaseJob]:
    return [
        ScrapeLectureDataJob(
            params=ScrapeLectureDataParams(teletask_id=tid),
            priority=priority,
        )
    ]


def scrape_done_ids() -> set[int]:
    ids = get_all_lecture_ids() or []
    return set(ids)


def scrape_is_done(tid: int) -> bool:
    return get_language_of_lecture(tid) is not None


def transcribe_factory(tid: int, priority: int) -> list[BaseJob]:
    return [
        TranscriptionJob(
            params=TranscriptionParams(teletask_id=tid),
            priority=priority,
        )
    ]


def transcribe_done_ids() -> set[int]:
    ids = get_all_original_vtt_ids() or []
    return set(ids)


def transcribe_is_done(tid: int) -> bool:
    return original_language_exists(tid)


def _existing_translations() -> dict[int, set[str]]:
    """teletask_id -> set of languages that already have a non-original VTT."""
    rows = get_missing_translations() or []
    by_tid: dict[int, set[str]] = {}
    for tid, lang in rows:
        by_tid.setdefault(tid, set()).add(lang)
    return by_tid


def translate_factory(tid: int, priority: int) -> list[BaseJob]:
    existing = _existing_translations().get(tid, set())
    return [
        TranslationJob(
            params=TranslationParams(
                teletask_id=tid, from_language="original", to_language=lang
            ),
            priority=priority,
        )
        for lang in TARGET_LANGUAGES
        if lang not in existing
    ]


def translate_done_ids() -> set[int]:
    by_tid = _existing_translations()
    required = set(TARGET_LANGUAGES)
    return {tid for tid, langs in by_tid.items() if required.issubset(langs)}


def translate_is_done(tid: int) -> bool:
    existing = _existing_translations().get(tid, set())
    return set(TARGET_LANGUAGES).issubset(existing)
