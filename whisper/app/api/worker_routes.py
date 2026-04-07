from typing import Annotated, cast
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from app.core.logger import logger
from app.models.dataclasses import (
    Job,
    LogLevel,
    SchedulerStatuses,
)
from app.scheduler.job_handlers import get_job_handler
from app.scheduler.scheduler import Scheduler, get_scheduler


worker_router = APIRouter()

SchedulerDep = Annotated[Scheduler, Depends(get_scheduler)]


class StatusUpdate(BaseModel):
    status: SchedulerStatuses


class LogMessage(BaseModel):
    message: str
    level: LogLevel = "info"


class FailureReport(BaseModel):
    reason: str


def _require_worker_owns_job(scheduler: Scheduler, worker_id: str, job_id: str) -> Job:
    job = _require_job(scheduler, job_id)
    owner = scheduler.get_worker_id_for_job(job_id)
    if owner is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} is not active")
    if owner != worker_id:
        raise HTTPException(
            status_code=409,
            detail=f"Job {job_id} belongs to worker {owner}, not {worker_id}",
        )
    return job


def _parse_request_body(raw_body: object) -> dict[str, object]:
    if not isinstance(raw_body, dict):
        raise HTTPException(status_code=400, detail="Result payload must be an object")
    raw_body_dict = cast(dict[object, object], raw_body)
    return {str(key): value for key, value in raw_body_dict.items()}


async def _submit_result_common(
    scheduler: Scheduler,
    job: Job,
    body: dict[str, object],
    worker_id: str,
) -> dict[str, str]:
    handler = get_job_handler(job.job_type)
    result = handler.parse_result(body)
    await handler.handle_result(job, result)

    job.status = "COMPLETED"
    return {"message": f"Result accepted for job {job.id}"}


async def _report_failure_common(
    scheduler: Scheduler,
    job: Job,
    reason: str,
    worker_id: str,
) -> dict[str, str]:
    job.status = "FAILED"  # TODO what to do with failed jobs
    handler = get_job_handler(job.job_type)
    await handler.handle_failed(job, reason)
    _ = scheduler.worker_finished(worker_id)
    return {"message": f"Failure recorded for job {job.id}"}


def _require_job(scheduler: Scheduler, job_id: str) -> Job:
    job = scheduler.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return job


@worker_router.post("/{worker_id}/jobs/{job_id}/status")
async def update_status_v2(
    worker_id: str,
    job_id: str,
    body: StatusUpdate,
    scheduler: SchedulerDep,
):
    job = _require_worker_owns_job(scheduler, worker_id, job_id)
    job.status = body.status
    logger.info(f"Job {job_id} status updated to {body.status} by {worker_id}")
    return {"message": f"Status updated to {body.status}"}


@worker_router.post("/{worker_id}/jobs/{job_id}/log")
async def append_log_v2(
    worker_id: str,
    job_id: str,
    body: LogMessage,
    scheduler: SchedulerDep,
):
    _ = _require_worker_owns_job(scheduler, worker_id, job_id)
    getattr(logger, body.level)(body.message, extra={"id": f"{worker_id}:{job_id}"})


@worker_router.post("/{worker_id}/jobs/{job_id}/result")
async def submit_result_v2(
    worker_id: str,
    job_id: str,
    request: Request,
    scheduler: SchedulerDep,
):
    job = _require_worker_owns_job(scheduler, worker_id, job_id)
    body = _parse_request_body(cast(object, await request.json()))
    return await _submit_result_common(
        scheduler=scheduler,
        job=job,
        body=body,
        worker_id=worker_id,
    )


@worker_router.post("/{worker_id}/jobs/{job_id}/failed")
async def report_failure_v2(
    worker_id: str,
    job_id: str,
    body: FailureReport,
    scheduler: SchedulerDep,
):
    job = _require_worker_owns_job(scheduler, worker_id, job_id)
    return await _report_failure_common(
        scheduler=scheduler,
        job=job,
        reason=body.reason,
        worker_id=worker_id,
    )


@worker_router.post("/{worker_id}/finished")
async def report_worker_finished(worker_id: str, scheduler: SchedulerDep):
    jobs = scheduler.worker_finished(worker_id)
    if jobs is None:
        return {"message": f"Worker {worker_id} already finished or unknown"}
    return {"message": f"Worker {worker_id} finished", "job_count": len(jobs)}
