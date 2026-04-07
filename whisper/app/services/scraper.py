import re
from typing import Any
import requests
from requests.models import Response, HTTPError
from bs4 import BeautifulSoup
import json
import os
from pathlib import Path

from sqlalchemy.sql import elements

from app.core.config import USERNAME_COOKIE, BASE_URL
from app.db.vtt_files import getHighestTeletaskID

from app.core.logger import logger

CHAIN_PEM = Path(__file__).parent / "chain.pem"


def fetchBody(id) -> Response:
    cookies = {"username": USERNAME_COOKIE}
    url = BASE_URL + str(id)
    logger.info("requesting " + url, extra={"id": id})
    response = requests.get(url, cookies=cookies, verify=str(CHAIN_PEM))
    response.raise_for_status()
    return response


def fetchMP4(id, response) -> str:
    soup = BeautifulSoup(response.text, "html.parser")
    player = soup.find(id="player")

    if player and player.has_attr("configuration"):
        config_json = player["configuration"]
        logger.debug(
            "Trying to fetch podcast.mp4 from fallbackStreams for ID ", extra={"id": id}
        )
        try:
            config = json.loads(config_json)
            fallbackStreams = config.get("fallbackStream")
            if fallbackStreams is not None:
                for key, url in fallbackStreams.items():
                    if url and url.endswith("podcast.mp4"):
                        logger.info(
                            f"Found first podcast.mp4 from FallbackStream URL: {url}",
                            extra={"id": id},
                        )
                        return url

                for key, url in fallbackStreams.items():
                    if url and url.endswith(".mp4"):
                        logger.info(
                            f"Found .mp4 URL from FallbackStream: {url}",
                            extra={"id": id},
                        )
                        return url
            else:
                logger.info(
                    "'fallbackStreams' key not found in configuration.",
                    extra={"id": id},
                )
        except json.JSONDecodeError:
            logger.error("Configuration attribute is not valid JSON:", extra={"id": id})
            logger.error(config_json, extra={"id": id})

        logger.info(
            "did not find podcast.mp4 in fallbackStreams, trying to fetch .mp4 from streams",
            extra={"id": id},
        )

        try:
            config = json.loads(config_json)
            streams = config.get("streams")
            if streams is not None:
                for stream in streams:
                    for key, url in stream.items():
                        if url and url.endswith("podcast.mp4"):
                            logger.info(
                                f"Found podcast.mp4 URL in streams: {url}",
                                extra={"id": id},
                            )
                            return url
                sd_urls = [stream.get("sd") for stream in streams if "sd" in stream]

                for url in sd_urls:
                    if (
                        url.endswith("video.mp4")
                        or url.endswith("CameraMicrophone.mp4")
                        or url.endswith("video_complete.mp4")
                    ):
                        logger.info(
                            f"Found video.mp4 URL in sd streams: {url}",
                            extra={"id": id},
                        )
                        return url
                for url in sd_urls:
                    if url.endswith(".mp4"):
                        logger.info(
                            f"Found first .mp4 URL in sd streams: {url}",
                            extra={"id": id},
                        )
                        return url
                for stream in streams:
                    for key, url in stream.items():
                        if url and url.endswith(".mp4"):
                            logger.info(
                                f"Found first .mp4 URL in streams: {url}",
                                extra={"id": id},
                            )
                            return url
            else:
                logger.warning(
                    "'streams' key not found in configuration.", extra={"id": id}
                )
        except json.JSONDecodeError:
            logger.error(
                f"Configuration attribute is not valid JSON: {config_json}",
                extra={"id": id},
            )
    else:
        logger.error(
            "Element with id 'player' and attribute 'configuration' not found, cant find mp4 URL",
            extra={"id": id},
        )

    logger.error("No mp4 URL found", extra={"id": id})
    return ""


def pingVideoByID(id) -> str:
    try:
        response = fetchBody(id)
    except HTTPError as e:
        logger.error(f"Error fetching body: {e}", extra={"id": id})
        return ""
    if response.status_code == 200:
        logger.info("Code 200, Video exists", extra={"id": id})
        return "200"
    elif response.status_code == 404:
        logger.info("Code 404, not available yet", extra={"id": id})
        return "404"
    elif response.status_code == 401:
        logger.info(
            "Code 401, not allowed, please use a session cookie", extra={"id": id}
        )
        return "401"
    elif response.status_code == 403:
        logger.info("Code 403, access forbidden", extra={"id": id})
        return "403"
    else:
        logger.warning(
            f"Received unexpected status code {response.status_code} when pinging video.",
            extra={"id": id},
        )
        return str(response.status_code)


def get_upper_ids() -> list[int]:
    ids: list[int] = []
    unreachable_ids: list[int] = []
    highest: int | None = getHighestTeletaskID()
    if highest is None:
        return ids
    highest = highest + 1
    for i in range(1, 10):
        res = pingVideoByID(str(highest + i))
        if res == "200":
            ids.append(highest + i)
        if res == "401":
            logger.error(
                "Received 401 Unauthorized. Check your USERNAME_COOKIE environment variable.",
                extra={"id": highest + i},
            )
        if res == "403" or res == "404":
            logger.warning(
                f"Received {res} for ID {highest + i}.", extra={"id": highest + i}
            )
            unreachable_ids.append(highest+i)
    return ids


def scrape_mp4_url_from_teletaskid(id, response=None) -> str:
    """Scrape the mp4 URL for a teletask ID. Pure scraping, no DB access."""
    if response is None:
        try:
            response = fetchBody(id)
        except HTTPError as e:
            logger.error(f"Error fetching body:{e}", extra={"id": id})
            return ""

    return fetchMP4(id, response)


def scrape_lecture_data(
    id: int, response: Response | None = None
) -> dict[str, Any] | None:
    """
    Scrape lecture metadata from the tele-task page.
    Returns: dict with lecturer_ids, date, language, duration, title, series info, or None on failure.
    """
    if response is None:
        try:
            response = fetchBody(id)
        except HTTPError as e:
            logger.error("Error fetching body:", extra={"id": id})
            return None

    soup = BeautifulSoup(response.text, "html.parser")
    lecture_info_div = soup.find("img", class_="box nopad lecture-img").parent

    if not lecture_info_div:
        logger.error("Div not found to scrape lecture data.", extra={"id": id})
        return None

    lecture_name = lecture_info_div.find("h3").get_text()
    h5 = lecture_info_div.find("h5")
    series_name = None
    series_id = None
    if h5:
        a = h5.find("a", href=True)
        if a:
            series_name = a.get_text(strip=True)
            m = re.search(r"/series/(\d+)", a["href"])
            series_id = int(m.group(1)) if m else None

    lectures = lecture_info_div.find_all("a", href=re.compile(r"^/lecturer/"))
    lecturer_names = []
    lecturer_ids = []
    for lect in lectures:
        lecturer_names.append(lect.get_text(strip=True))
        m = re.search(r"/lecturer/(\d+)", lect["href"])
        lecturer_ids.append(int(m.group(1)) if m else None)

    inner = lecture_info_div.decode_contents()

    def find_field(label):
        m = re.search(rf"{re.escape(label)}:\s*(.*?)\s*<br", inner, re.I | re.S)
        return m.group(1).strip() if m else None

    date = find_field("Date")
    language = find_field("Language")
    duration = find_field("Duration")

    lecture_data = {
        "lecture_id": id,
        "lecturer_ids": lecturer_ids,
        "lecturer_names": lecturer_names,
        "date": date,
        "language": language,
        "duration": duration,
        "lecture_title": lecture_name,
        "series_id": series_id,
        "series_name": series_name,
    }
    logger.debug(f"Fetched lecture data: {lecture_data}", extra={"id": id})
    return lecture_data
