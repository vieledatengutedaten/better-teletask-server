"""Example CLI for transcription job arguments."""

import argparse
from collections.abc import Sequence
import json
from typing import TextIO, cast

from pydantic import TypeAdapter
from lib.models.jobs.transcription import TranscriptionParams
from lib.models.jobs.base import BatchPayload, JobPayload
from .utils import report_worker_finished



def _run_single_job(
    job: JobPayload[TranscriptionParams],
    worker_id: str,
    scheduler_url: str | None,
) -> bool:
    _ = job
    _ = worker_id
    _ = scheduler_url
    return True


def run_transcription(
    jobs: Sequence[JobPayload[TranscriptionParams]],
    worker_id: str,
    scheduler_url: str | None
) -> bool:
    success = True
    for job in jobs:
        if not _run_single_job(job, worker_id, scheduler_url):
            success = False

    finished_ok = report_worker_finished(worker_id, scheduler_url)
    return success

TRANSCRIBE_BATCH_ADAPTER = TypeAdapter(BatchPayload[TranscriptionParams])

def _load_jobs(jobs_file: TextIO):
    payload = cast(object, json.loads(jobs_file))
    try:
        batch = TRANSCRIBE_BATCH_ADAPTER.validate_python(payload)
        return batch.worker_id, batch.jobs
    except Exception as exc:
        raise ValueError(f"Invalid transcription jobs payload: {exc}") from exc




def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scrape job batch CLI")
    _ = parser.add_argument(
        "--jobs-file",
        type=argparse.FileType("r", encoding="utf-8"),
        required=True,
    )
    _ = parser.add_argument("--scheduler-url", type=str, default=None)
    return parser


def main() -> None:
    parsed = build_parser().parse_args()

    jobs_file = cast(TextIO, parsed.jobs_file)
    scheduler_url = cast(str | None, parsed.scheduler_url)
    try:
        worker_id, jobs = _load_jobs(jobs_file)
        print(jobs)
    except ValueError as exc:
        print(str(exc))
        raise SystemExit(2) from exc
    
    if not worker_id:
        print("Missing worker_id in jobs payload")
        raise SystemExit(2)
    
    run_transcription(
        jobs=jobs,
        worker_id=worker_id,
        scheduler_url=scheduler_url,
    )
    



if __name__ == "__main__":
    main()
