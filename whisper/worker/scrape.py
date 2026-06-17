"""Scrape worker helpers and CLI for batch scrape jobs."""

import argparse
import json
from collections.abc import Sequence
from typing import TextIO, cast

import requests
from pydantic import TypeAdapter, ValidationError

from lib.core.logger import logger
from lib.models.jobs import JobPayload, BatchPayload
from lib.models.jobs.scrape import (
    ScrapeLectureDataParams,
    ScrapeLectureDataResult,
)
from lib.services.scraper import scrape_lecture_data
from worker.utils import (
    fetch_worker_batch,
    log_to_scheduler,
    report_job_failed,
    report_job_result,
    report_worker_finished,
)

SCRAPE_BATCH_ADAPTER = TypeAdapter(BatchPayload[ScrapeLectureDataParams])


def _run_single_job(
    job: JobPayload[ScrapeLectureDataParams],
    worker_id: str,
    scheduler_url: str | None,
) -> bool:
    with log_to_scheduler(scheduler_url, worker_id, job.job_id):
        lecture_data = scrape_lecture_data(job.params.teletask_id)
        if lecture_data is None:
            return report_job_failed(
                worker_id,
                job.job_id,
                "Could not scrape lecture data for teletask_id",
                scheduler_url,
            )

        try:
            result = ScrapeLectureDataResult(
                job_id=job.job_id,
                success=True,
                lecture_data=lecture_data,
            )
        except Exception as exc:
            return report_job_failed(
                worker_id,
                job.job_id,
                f"Failed to validate scrape result: {exc}",
                scheduler_url,
            )

        return report_job_result(
            worker_id,
            job.job_id,
            result.model_dump(),
            scheduler_url,
        )


def run_scrape(
    jobs: Sequence[JobPayload[ScrapeLectureDataParams]],
    worker_id: str,
    scheduler_url: str | None = None,
) -> bool:
    success = True
    for job in jobs:
        if not _run_single_job(job, worker_id, scheduler_url):
            success = False
            break

    finished_ok = report_worker_finished(
        worker_id=worker_id, scheduler_url=scheduler_url
    )
    return success and finished_ok


def _validate_batch(
    payload: object,
) -> tuple[str, list[JobPayload[ScrapeLectureDataParams]]]:
    try:
        batch = SCRAPE_BATCH_ADAPTER.validate_python(payload)
        return batch.worker_id, batch.jobs
    except ValidationError as exc:
        raise ValueError(f"Invalid scrape jobs payload: {exc}") from exc


def _load_jobs(
    jobs_file: TextIO,
) -> tuple[str, list[JobPayload[ScrapeLectureDataParams]]]:
    return _validate_batch(cast(object, json.load(jobs_file)))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scrape job batch CLI")
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

    try:
        if jobs_file is not None:
            worker_id, jobs = _load_jobs(jobs_file)
        elif worker_id_arg:
            raw = fetch_worker_batch(worker_id_arg, scheduler_url)
            worker_id, jobs = _validate_batch(raw)
        else:
            logger.error("Provide --worker-id (to fetch from scheduler) or --jobs-file")
            raise SystemExit(2)
    except ValueError as exc:
        logger.error(str(exc))
        raise SystemExit(2) from exc
    except requests.RequestException as exc:
        logger.error(f"Failed to fetch batch for worker {worker_id_arg}: {exc}")
        raise SystemExit(2) from exc

    run_scrape(
        jobs=jobs,
        worker_id=worker_id,
        scheduler_url=scheduler_url,
    )

if __name__ == "__main__":
    main()

