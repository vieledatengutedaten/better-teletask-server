# setup logging
from app.core.logger import logger
logger.propagate = False

import whisperx
import os
from whisperx.utils import get_writer

from app.core.config import ASR_MODEL, COMPUTE_TYPE, INPUT_PATH, OUTPUT_PATH, DEVICE
from app.db.lectures import get_language_of_lecture

device = DEVICE
model = whisperx.load_model(ASR_MODEL, device=device, compute_type=COMPUTE_TYPE)


def transcribeVideoByID(id) -> str:
    file_path = os.path.join(INPUT_PATH, id + ".mp3")

    # fail early if input audio doesn't exist
    if not os.path.exists(file_path):
        logger.error(f"input audio file not found: {file_path}", extra={"id": id})
        raise FileNotFoundError(f"input audio file not found: {file_path}")

    language = None
    try:
        language = get_language_of_lecture(int(id))
        logger.info(f"Fetched language from database: {language}", extra={"id": id})
    except Exception as e:
        logger.warning(f"Could not fetch language from database. {e}", extra={"id": id})

    if language is None:
        logger.info(
            f"No language found in database, defaulting to auto detection from whisperx.",
            extra={"id": id},
        )
        language = None

    audio = whisperx.load_audio(file_path)

    result = model.transcribe(audio, batch_size=4, language=language)
    logger.debug(
        f"Transcription result segments: {result['segments']}", extra={"id": id}
    )

    # Save the language before alignment
    language = result.get("language")

    logger.info(
        "The language after transcription is " + str(language), extra={"id": id}
    )

    model_a, metadata = whisperx.load_align_model(language_code=language, device=device)

    aligned_result = whisperx.align(
        result["segments"],
        model_a,
        metadata,
        audio,
        device=device,
        return_char_alignments=False,
    )
    logger.debug(
        f"Aligned result segments: {aligned_result['segments']}", extra={"id": id}
    )

    # Add language back to aligned result for the writer
    aligned_result["language"] = language

    os.makedirs(OUTPUT_PATH, exist_ok=True)

    # Save as a VTT file
    vtt_writer = get_writer("vtt", OUTPUT_PATH)
    vtt_writer(
        aligned_result,
        file_path,
        {"max_line_width": None, "max_line_count": None, "highlight_words": False},
    )

    txt_writer = get_writer("txt", OUTPUT_PATH)
    txt_writer(
        aligned_result,
        file_path,
        {"max_line_width": None, "max_line_count": None, "highlight_words": False},
    )

    return language


if __name__ == "__main__":
    id = "11401"
    transcribeVideoByID(id)
