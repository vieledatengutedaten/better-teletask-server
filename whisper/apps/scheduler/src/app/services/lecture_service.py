from lib.core.logger import logger
from app.db.lectures import add_lecture_data, get_language_of_lecture
from lib.services.scraper import scrape_mp4_url_from_teletaskid, scrape_lecture_data
from requests.models import Response


def fetch_and_store_lecture_data(id: int, response: Response | None = None) -> None:
    """Scrape lecture metadata and store it in the DB if not already present."""
    if get_language_of_lecture(id) is not None:
        logger.debug("Lecture data already exists for ID", extra={"id": id})
        return

    lecture_data = scrape_lecture_data(id, response)
    if lecture_data is not None:
        add_lecture_data(lecture_data)


def get_mp4_url_and_ensure_lecture_data(
    id: int, response: Response | None = None
) -> str:
    """Scrape mp4 URL and ensure lecture metadata is stored in the DB."""
    url = scrape_mp4_url_from_teletaskid(id, response)
    if url:
        fetch_and_store_lecture_data(id, response)
    return url
