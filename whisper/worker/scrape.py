"""Scrape worker helpers and CLI for batch scrape jobs."""

import argparse
import json
from collections.abc import Sequence
from typing import TextIO, cast

from pydantic import TypeAdapter, ValidationError

from lib.core.logger import logger
from lib.models.jobs import JobPayload, BatchPayload
from lib.models.jobs.scrape import (
    ScrapeLectureDataParams,
    ScrapeLectureDataResult,
)
from lib.services.scraper import scrape_lecture_data
from worker.utils import (
    log_to_scheduler,
    report_job_failed,
    report_job_result,
    report_worker_finished,
)

SCRAPE_BATCH_ADAPTER = TypeAdapter(BatchPayload[ScrapeLectureDataParams])
SCRAPE_JOB_LIST_ADAPTER = TypeAdapter(list[JobPayload[ScrapeLectureDataParams]])


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


def _load_jobs(
    jobs_file: TextIO,
) -> tuple[str | None, list[JobPayload[ScrapeLectureDataParams]]]:
    payload = cast(object, json.load(jobs_file))
    try:
        if isinstance(payload, dict) and "jobs" in payload:
            batch = SCRAPE_BATCH_ADAPTER.validate_python(payload)
            return batch.worker_id, batch.jobs
        return None, SCRAPE_JOB_LIST_ADAPTER.validate_python(payload)
    except ValidationError as exc:
        raise ValueError(f"Invalid scrape jobs payload: {exc}") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scrape job batch CLI")
    _ = parser.add_argument(
        "--jobs-file",
        type=argparse.FileType("r", encoding="utf-8"),
        required=True,
    )
    _ = parser.add_argument("--scheduler-url", type=str, default=None)
    _ = parser.add_argument("--worker-id", type=str, default=None)
    return parser


def main() -> None:
    parsed = build_parser().parse_args()
    worker_id = cast(str | None, parsed.worker_id)
    scheduler_url = cast(str | None, parsed.scheduler_url)
    jobs_file = cast(TextIO, parsed.jobs_file)

    try:
        batch_worker_id, jobs = _load_jobs(jobs_file)
        print(jobs)
    except ValueError as exc:
        logger.error(str(exc))
        raise SystemExit(2) from exc

    if batch_worker_id:
        worker_id = batch_worker_id
    if not worker_id:
        logger.error("Missing worker_id in jobs payload or --worker-id")
        raise SystemExit(2)


    ok = run_scrape(
        jobs=jobs,
        worker_id=worker_id,
        scheduler_url=scheduler_url,
    )

    print(
        json.dumps(
            {
                "worker_id": worker_id,
                "job_count": len(jobs),
                "success": ok,
            },
            ensure_ascii=True,
        )
    )
    if not ok:
        raise SystemExit(1)

if __name__ == "__main__":
    main()

