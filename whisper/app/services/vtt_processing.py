import webvtt
from app.db.vtt_files import get_vtt_file_by_id
from app.db.vtt_lines import bulk_insert_vtt_lines
from app.db.lectures import get_series_of_vtt_file, get_lecturer_ids_of_lecture
from app.models import VttFile, VttLine

from app.core.logger import logger


# HH:MM:SS.mmm
def timestamp_to_ms(timestamp: str) -> int:
    parts = timestamp.split(":")
    if len(parts) == 3:
        h, m, rest = parts
    else:
        h = 0
        m = parts[0]
        rest = parts[1]
    s, ms = rest.split(".")
    total_ms = (int(h) * 3600 + int(m) * 60 + int(s)) * 1000 + int(ms)
    return total_ms


def save_vtt_lines(vtt_id: int):
    vtt_file: VttFile = get_vtt_file_by_id(vtt_id)

    if not vtt_file:
        logger.error(f"VTT file with ID {vtt_id} not found.")
        return

    series = get_series_of_vtt_file(vtt_id)
    lecturer_ids = get_lecturer_ids_of_lecture(vtt_file.lecture_id)

    vtt_lines: list[VttLine] = []
    for i, caption in enumerate(webvtt.from_string(vtt_file.vtt_data.decode("utf-8"))):
        line = VttLine(
            id=None,
            vtt_file_id=vtt_file.id,
            series_id=series.series_id if series else 0,
            language=vtt_file.language,
            lecturer_ids=lecturer_ids,
            line_number=i + 1,
            ts_start=timestamp_to_ms(caption.start),
            ts_end=timestamp_to_ms(caption.end),
            content=caption.text,
        )
        vtt_lines.append(line)

    bulk_insert_vtt_lines(vtt_lines)


if __name__ == "__main__":
    save_vtt_lines(1)
    save_vtt_lines(2)
