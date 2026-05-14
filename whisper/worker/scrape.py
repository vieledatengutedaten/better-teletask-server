"""Scrape worker helpers and CLI for batch scrape jobs."""

import argparse
import json
import logging
from math import log
import os
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TextIO, cast, override

import requests
from pydantic import TypeAdapter, ValidationError

from lib.core.logger import formatter, logger
from lib.models.jobs import LogLevel
from lib.models.jobs.scrape import ScrapeLectureDataJob, ScrapeLectureDataResult
from lib.services.scraper import scrape_lecture_data
from lib.models.messages import LogMessage


SCRAPE_JOB_LIST_ADAPTER = TypeAdapter(list[ScrapeLectureDataJob])


class SchedulerPostHandler(logging.Handler):
	def __init__(self, scheduler_url: str, worker_id: str, job_id: str) -> None:
		super().__init__(level=logging.INFO)
		self.url = f"{scheduler_url}/worker/{worker_id}/jobs/{job_id}/log"

	@override
	def emit(self, record: logging.LogRecord) -> None:
		try:
			level_name = record.levelname.lower()
			level: LogLevel
			if level_name in ("debug", "info", "warning", "error", "critical"):
				level = level_name
			else:
				level = "info"
			payload = LogMessage(
				message=record.getMessage(),
				level=level,
			).model_dump()

			print(payload)

			_ = requests.post(
				self.url,
				json=payload,
				timeout=5,
			)
		except Exception:
			pass


_worker_log_enabled: ContextVar[bool] = ContextVar("worker_log_enabled", default=False)


class WorkerLogFilter(logging.Filter):
	def __init__(self, suppress_others: bool = False):
		super().__init__()
		self.suppress_others = suppress_others

	@override
	def filter(self, record: logging.LogRecord) -> bool:
		is_enabled = _worker_log_enabled.get()
		if self.suppress_others:
			# If this is one of the standard handlers, suppress if worker logging is active
			return not is_enabled
		return is_enabled


@contextmanager
def _log_to_scheduler(
	scheduler_url: str | None,
	worker_id: str,
	job_id: str,
) -> Generator[None,None,None]:

	scheduler_url = _resolve_scheduler_url(scheduler_url)

	if not scheduler_url:
		yield
		return

	handler = SchedulerPostHandler(scheduler_url, worker_id, job_id)
	handler.setFormatter(formatter)
	handler.addFilter(WorkerLogFilter(suppress_others=False))
	logger.addHandler(handler)

	# Suppress normal logging for this stack
	suppressors = []
	for h in logger.handlers:
		if h is not handler:
			f = WorkerLogFilter(suppress_others=True)
			h.addFilter(f)
			suppressors.append((h, f))

	token = _worker_log_enabled.set(True)
	try:
		yield
	finally:
		_worker_log_enabled.reset(token)
		for h, f in suppressors:
			h.removeFilter(f)
		logger.removeHandler(handler)
		handler.close()


def _resolve_scheduler_url(scheduler_url: str | None) -> str:
	if scheduler_url:
		return scheduler_url.rstrip("/")
	env_url = os.environ.get("SCHEDULER_URL")
	if env_url:
		return env_url.rstrip("/")
	return "http://127.0.0.1:8000"


def _post_json(url: str, payload: dict[str, object]) -> bool:
	try:
		response = requests.post(url, json=payload, timeout=10)
		response.raise_for_status()
		return True
	except requests.RequestException as exc:
		logger.error(f"Error posting worker callback to {url}: {exc}")
		return False


def report_worker_finished(worker_id: str, scheduler_url: str | None = None) -> bool:
	base_url = _resolve_scheduler_url(scheduler_url)
	url = f"{base_url}/worker/{worker_id}/finished"
	return _post_json(url, {})

def _report_failed(
	job: ScrapeLectureDataJob,
	worker_id: str,
	reason: str,
	scheduler_url: str | None = None,
) -> bool:
	base_url = _resolve_scheduler_url(scheduler_url)
	url = f"{base_url}/worker/{worker_id}/jobs/{job.id}/failed"
	return _post_json(url, {"reason": reason})


def _report_result(
	job: ScrapeLectureDataJob,
	worker_id: str,
	result: ScrapeLectureDataResult,
	scheduler_url: str | None,
) -> bool:
	base_url = _resolve_scheduler_url(scheduler_url)
	url = f"{base_url}/worker/{worker_id}/jobs/{job.id}/result"
	return _post_json(url, result.model_dump())


def _run_single_job(
	job: ScrapeLectureDataJob,
	worker_id: str,
	scheduler_url: str | None,
) -> bool:
	with _log_to_scheduler(scheduler_url, worker_id, job.id):
		lecture_data = scrape_lecture_data(job.params.teletask_id)
		if lecture_data is None:
			return _report_failed(
				job,
				worker_id,
				"Could not scrape lecture data for teletask_id",
				scheduler_url,
			)

		try:
			result = ScrapeLectureDataResult.model_validate(
				{
					"job_id": job.id,
					"success": True,
					**lecture_data,
				}
			)
		except Exception as exc:
			return _report_failed(
				job,
				worker_id,
				f"Failed to validate scrape result: {exc}",
				scheduler_url,
			)

		return _report_result(job, worker_id, result, scheduler_url)


def run_scrape(
	jobs: Sequence[ScrapeLectureDataJob],
	worker_id: str,
	scheduler_url: str | None = None,
) -> bool:
	success = True
	for job in jobs:
		job.worker_id = worker_id
		if not _run_single_job(job, worker_id, scheduler_url):
			success = False
			break

	finished_ok = report_worker_finished(worker_id=worker_id, scheduler_url=scheduler_url)
	return success and finished_ok


def _load_jobs(jobs_file: TextIO) -> list[ScrapeLectureDataJob]:
	payload = cast(object, json.load(jobs_file))
	try:
		return SCRAPE_JOB_LIST_ADAPTER.validate_python(payload)
	except ValidationError as exc:
		raise ValueError(f"Invalid scrape jobs payload: {exc}") from exc


def build_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(description="Scrape job batch CLI")
	_ = parser.add_argument("--worker-id", type=str, required=True)
	_ = parser.add_argument(
		"--jobs-file",
		type=argparse.FileType("r", encoding="utf-8"),
		required=True,
	)
	_ = parser.add_argument("--scheduler-url", type=str, default=None)
	return parser


def main() -> None:
	parsed = build_parser().parse_args()
	worker_id = cast(str, parsed.worker_id)
	scheduler_url = cast(str | None, parsed.scheduler_url)
	jobs_file = cast(TextIO, parsed.jobs_file)

	try:
		jobs = _load_jobs(jobs_file)
	except ValueError as exc:
		logger.error(str(exc))
		raise SystemExit(2) from exc

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

