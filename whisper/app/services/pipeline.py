from app.core.logger import logger

from app.core.config import INPUT_PATH
from app.services.lecture_service import get_mp4_url_and_ensure_lecture_data
from app.services.downloader import downloadMP4, convert_to_mp3, remove_all_id_files
from app.db.lectures import get_language_of_lecture
from app.db.vtt_files import save_vtt_as_blob
from app.db.migrations import initDatabase

from requests.models import HTTPError


def transcribePipelineVideoByID(id: int):
    # lazyload whisper here to avoid the annoying waiting time
    from app.services.whisper_asr import transcribeVideoByID

    url = get_mp4_url_and_ensure_lecture_data(id)

    if url == "":
        logger.error("No mp4 URL found, cannot transcribe", extra={"id": id})
        return -1
    else:
        try:
            logger.info(
                f"Trying to directly convert to mp3 from URL: {url}", extra={"id": id}
            )
            convert_to_mp3(url, str(INPUT_PATH / f"{id}.mp3"))  # intentional bug
        except Exception as e:
            logger.error(
                f"Trying to download mp4 and convert locally: {e}",
                extra={"id": id},
                exc_info=True,
            )
            try:
                downloadMP4(url, id)
                convert_to_mp3(
                    str(INPUT_PATH / f"{id}.mp4"), str(INPUT_PATH / f"{id}.mp3")
                )
            except Exception as e2:
                logger.error(
                    f"Could not convert video to mp3, aborting transcribtion for this lecture.",
                    extra={"id": id},
                )
                return -1

        try:
            language = transcribeVideoByID(id)
        except FileNotFoundError as e:
            logger.error(f"ERROR: Could not transcribe video {e}.", extra={"id": id})
            return -1

        try:
            _ = save_vtt_as_blob(id, language, True)
        except Exception as e:
            logger.error(f"Could not save VTT to database {e}.", extra={"id": id})
            return -1

        logger.info(
            f"ID: {id} Transcription and saving completed successfully, removing source files."
        )
        remove_all_id_files(id)
        return 0
