import os
import requests
from app.core.logger import logger
import logging
from tqdm import tqdm
import ffmpeg

from app.core.config import INPUT_PATH, OUTPUT_PATH

def downloadMP4(url, id):
    try:
        mp4_response = requests.get(url, stream=True, verify="chain.pem")
        mp4_response.raise_for_status()
        total_size = int(mp4_response.headers.get("content-length", 0))
        dest = INPUT_PATH + str(id) + ".mp4"
        with open(dest, "wb") as f, tqdm(
            desc="Downloading " + dest,
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
            for chunk in mp4_response.iter_content(chunk_size=8192):
                f.write(chunk)
                bar.update(len(chunk))
        logging.info("Download complete:" + dest, extra={"id": id})
    except Exception as e:
        logging.error(f"Error downloading mp4: {e}", extra={"id": id})
        raise


def convert_to_mp3(source, out_mp3):
    """Convert a source (URL or local file) to MP3 using ffmpeg-python."""
    try:
        (
            ffmpeg.input(source)
            .output(out_mp3, format="mp3", acodec="libmp3lame", **{"q:a": 2}, vn=None)
            .overwrite_output()
            .global_args("-hide_banner", "-loglevel", "error")
            .run(capture_stdout=True, capture_stderr=True)
        )
        logger.info(f"Saved MP3 to {out_mp3}")
    except ffmpeg.Error as e:
        err = e.stderr.decode() if getattr(e, "stderr", None) else str(e)
        logger.error(f"Failed converting to MP3: {err}")
        raise


def remove_all_id_files(id):
    """Remove all temporary files (vtt, txt, mp3, mp4) for a given ID."""
    vtt_path = os.path.join(OUTPUT_PATH, str(id) + ".vtt")
    txt_path = os.path.join(OUTPUT_PATH, str(id) + ".txt")
    mp3_path = os.path.join(INPUT_PATH, str(id) + ".mp3")
    mp4_path = os.path.join(INPUT_PATH, str(id) + ".mp4")

    for file_path in [vtt_path, txt_path, mp3_path, mp4_path]:
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.info(f"Removed file: {file_path}")
            except Exception as e:
                logger.error(f"Error removing file {file_path}: {e}")
        else:
            logger.debug(f"File not found, cannot remove: {file_path}")
