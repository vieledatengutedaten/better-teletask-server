"""Worker helpers shared across job CLIs."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar
import logging
import os
from typing import cast, override

import requests

from lib.core.config import VM_WORKER_AUTH_TOKEN
from lib.core.logger import formatter, logger
from lib.models.jobs import LogLevel
from lib.models.messages import LogMessage


_worker_log_enabled: ContextVar[bool] = ContextVar("worker_log_enabled", default=False)


def worker_auth_headers() -> dict[str, str]:
	"""Bearer header carrying the shared worker token, empty when unset."""
	if VM_WORKER_AUTH_TOKEN:
		return {"Authorization": f"Bearer {VM_WORKER_AUTH_TOKEN}"}
	return {}


class SchedulerPostHandler(logging.Handler):
	def __init__(self, scheduler_url: str, worker_id: str, job_id: str) -> None:
		super().__init__(level=logging.INFO)
		self.url = f"{scheduler_url}/worker/{worker_id}/jobs/{job_id}/log"

	@override
	def emit(self, record: logging.LogRecord) -> None:
		try:
			print(f"Emitting log record to scheduler: {record.getMessage()}")
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

			_ = requests.post(
				self.url,
				json=payload,
				headers=worker_auth_headers(),
				timeout=5,
			)
		except Exception:
			pass


class WorkerLogFilter(logging.Filter):
	def __init__(self, suppress_others: bool = False):
		super().__init__()
		self.suppress_others = suppress_others

	@override
	def filter(self, record: logging.LogRecord) -> bool:
		is_enabled = _worker_log_enabled.get()
		if self.suppress_others:
			return not is_enabled
		return is_enabled


@contextmanager
def log_to_scheduler(
	scheduler_url: str | None,
	worker_id: str,
	job_id: str,
) -> Generator[None, None, None]:
	scheduler_url = resolve_scheduler_url(scheduler_url)

	if not scheduler_url:
		yield
		return

	handler = SchedulerPostHandler(scheduler_url, worker_id, job_id)
	handler.setFormatter(formatter)
	handler.addFilter(WorkerLogFilter(suppress_others=False))
	logger.addHandler(handler)

	suppressors = []
	for handler_item in logger.handlers:
		if handler_item is not handler:
			log_filter = WorkerLogFilter(suppress_others=True)
			handler_item.addFilter(log_filter)
			suppressors.append((handler_item, log_filter))

	token = _worker_log_enabled.set(True)
	try:
		yield
	finally:
		_worker_log_enabled.reset(token)
		for handler_item, log_filter in suppressors:
			handler_item.removeFilter(log_filter)
		logger.removeHandler(handler)
		handler.close()


def resolve_scheduler_url(scheduler_url: str | None) -> str:
	if scheduler_url:
		return scheduler_url.rstrip("/")
	env_url = os.environ.get("SCHEDULER_URL")
	if env_url:
		return env_url.rstrip("/")
	return "http://127.0.0.1:8000"


def fetch_worker_batch(
	worker_id: str, scheduler_url: str | None = None
) -> dict[str, object]:
	"""Pull this worker's job batch from the scheduler (BatchPayload form)."""
	base_url = resolve_scheduler_url(scheduler_url)
	url = f"{base_url}/worker/{worker_id}/jobs"
	response = requests.get(url, headers=worker_auth_headers(), timeout=10)
	response.raise_for_status()
	return cast(dict[str, object], response.json())


def post_json(url: str, payload: dict[str, object]) -> bool:
	try:
		response = requests.post(url, json=payload, headers=worker_auth_headers(), timeout=10)
		response.raise_for_status()
		return True
	except requests.RequestException as exc:
		logger.error(f"Error posting worker callback to {url}: {exc}")
		return False


def report_worker_finished(worker_id: str, scheduler_url: str | None = None) -> bool:
	base_url = resolve_scheduler_url(scheduler_url)
	url = f"{base_url}/worker/{worker_id}/finished"
	return post_json(url, {})


def report_job_failed(
	worker_id: str,
	job_id: str,
	reason: str,
	scheduler_url: str | None = None,
) -> bool:
	base_url = resolve_scheduler_url(scheduler_url)
	url = f"{base_url}/worker/{worker_id}/jobs/{job_id}/failed"
	return post_json(url, {"reason": reason})


def report_job_result(
	worker_id: str,
	job_id: str,
	payload: dict[str, object],
	scheduler_url: str | None,
) -> bool:
	base_url = resolve_scheduler_url(scheduler_url)
	url = f"{base_url}/worker/{worker_id}/jobs/{job_id}/result"
	return post_json(url, payload)
