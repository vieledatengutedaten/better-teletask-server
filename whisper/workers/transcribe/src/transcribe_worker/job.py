"""Example CLI for transcription job arguments."""

import argparse
from collections.abc import Sequence
import json
from typing import Any, TextIO, cast

import requests
from pydantic import TypeAdapter
from lib.core.logger import logger
from lib.models.jobs.transcription import TranscriptionParams
from lib.models.jobs.base import BatchPayload, JobPayload
from transcribe_worker.whisper_asr import prepare_model, transcribeVideoByID
from lib.worker_client import fetch_worker_batch, report_worker_finished

from lib.core.config import INPUT_PATH
from transcribe_worker.downloader import downloadMP4, convert_to_mp3


def _run_single_job(
    job: JobPayload[TranscriptionParams],
    worker_id: str,
    scheduler_url: str | None,
    model: Any,
) -> bool:
    try:
        url = job.params.mp4_url
        id = job.params.teletask_id
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
        transcribeVideoByID(job.params.teletask_id, model, job.params.language)
    except Exception as exc:
        logger.error(
            f"Failed to transcribe lecture {job.params.lecture_id}: {exc}",
            extra={"id": job.params.lecture_id},
        )
    return True


def run_transcription(
    jobs: Sequence[JobPayload[TranscriptionParams]],
    worker_id: str,
    scheduler_url: str | None,
) -> bool:
    success = True
    # TODO we just assume same parameters for batch, might want to validate this and/or support multiple models in one batch
    # TODO cpu for testing
    model = prepare_model(
        jobs[0].params.asr_model, "cpu", compute_type=jobs[0].params.compute_type
    )
    for job in jobs:
        if not _run_single_job(job, worker_id, scheduler_url, model):
            success = False

    finished_ok = report_worker_finished(worker_id, scheduler_url)
    return success


TRANSCRIBE_BATCH_ADAPTER = TypeAdapter(BatchPayload[TranscriptionParams])


def _validate_batch(
    payload: object,
) -> tuple[str, list[JobPayload[TranscriptionParams]]]:
    try:
        batch = TRANSCRIBE_BATCH_ADAPTER.validate_python(payload)
        return batch.worker_id, batch.jobs
    except Exception as exc:
        raise ValueError(f"Invalid transcription jobs payload: {exc}") from exc


def _load_jobs(
    jobs_file: TextIO,
) -> tuple[str, list[JobPayload[TranscriptionParams]]]:
    return _validate_batch(cast(object, json.load(jobs_file)))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Transcription job batch CLI")
    _ = parser.add_argument(
        "--worker-id",
        type=str,
        default=None,
        help="Worker id; the batch is fetched from the scheduler over HTTP.",
    )
    _ = parser.add_argument("--scheduler-url", type=str, default=None)
    _ = parser.add_argument(
        "--jobs-file",
        type=argparse.FileType("r", encoding="utf-8"),
        default=None,
        help="Local batch JSON (debugging); used instead of fetching when given.",
    )
    return parser


def main() -> None:
    parsed = build_parser().parse_args()
    scheduler_url = cast(str | None, parsed.scheduler_url)
    worker_id_arg = cast(str | None, parsed.worker_id)
    jobs_file = cast("TextIO | None", parsed.jobs_file)

    print(
        f"Parsed args: worker_id={worker_id_arg}, scheduler_url={scheduler_url}, jobs_file={jobs_file}"
    )
    try:
        if jobs_file is not None:
            worker_id, jobs = _load_jobs(jobs_file)
        elif worker_id_arg:
            raw = fetch_worker_batch(worker_id_arg, scheduler_url)
            worker_id, jobs = _validate_batch(raw)
            print(f"Fetched batch for worker {worker_id_arg}: {len(jobs)} jobs")
        else:
            logger.error("Provide --worker-id (to fetch from scheduler) or --jobs-file")
            raise SystemExit(2)
    except ValueError as exc:
        logger.error(str(exc))
        raise SystemExit(2) from exc
    except requests.RequestException as exc:
        logger.error(f"Failed to fetch batch for worker {worker_id_arg}: {exc}")
        raise SystemExit(2) from exc

    print("running transcription")
    run_transcription(
        jobs=jobs,
        worker_id=worker_id,
        scheduler_url=scheduler_url,
    )


if __name__ == "__main__":
    main()
