"""Central registry for jobtypes and resources.

Adding a new jobtype = three edits:
  1. Define Job/Params/Result classes in lib/models/dataclasses.py
  2. Write a JobHandler subclass in app/scheduler/job_handlers.py
  3. Add a JobTypeSpec entry to JOB_TYPES below

Adding a new resource = one edit:
  1. Add to ResourceType literal in dataclasses.py and to RESOURCES below

The scheduler, queues, and worker manager read everything from this registry.
No isinstance chains, no hardcoded priority tuples elsewhere.
"""

import os
from dataclasses import dataclass

from lib.models.jobs import (
    BaseJob,
    JobResultBase,
    JobType,
    ResourceType,
    ScrapeLectureDataJob,
    ScrapeLectureDataResult,
    TranscriptionJob,
    TranscriptionResult,
    TranslationJob,
    TranslationResult,
)
from app.scheduler.job_handlers import JobHandler, get_job_handler


@dataclass(frozen=True)
class JobTypeSpec:
    job_type: JobType
    resource: ResourceType
    job_cls: type[BaseJob]
    result_cls: type[JobResultBase]
    handler: JobHandler
    batch_size: int       # how many jobs of this type per worker invocation
    base_priority: int    # higher = scheduler picks this jobtype first


@dataclass(frozen=True)
class ResourceSpec:
    resource: ResourceType
    max_workers: int      # concurrent worker slots for this resource


JOB_TYPES: dict[JobType, JobTypeSpec] = {
    "scrape_lecture_data": JobTypeSpec(
        job_type="scrape_lecture_data",
        resource="cpu",
        job_cls=ScrapeLectureDataJob,
        result_cls=ScrapeLectureDataResult,
        handler=get_job_handler("scrape_lecture_data"),
        batch_size=10,
        base_priority=200,
    ),
    "transcription": JobTypeSpec(
        job_type="transcription",
        resource="whisper",
        job_cls=TranscriptionJob,
        result_cls=TranscriptionResult,
        handler=get_job_handler("transcription"),
        batch_size=2,
        base_priority=100,
    ),
    "translation": JobTypeSpec(
        job_type="translation",
        resource="ollama",
        job_cls=TranslationJob,
        result_cls=TranslationResult,
        handler=get_job_handler("translation"),
        batch_size=4,
        base_priority=50,
    ),
}


_DEFAULT_MAX_WORKERS: dict[ResourceType, int] = {
    "whisper": 2,
    "ollama": 3,
    "cpu": 8,
}


def _max_workers_for(resource: ResourceType) -> int:
    env_key = f"{resource.upper()}_MAX_WORKERS"
    raw = os.environ.get(env_key)
    if raw is None:
        return _DEFAULT_MAX_WORKERS[resource]
    try:
        return int(raw)
    except ValueError:
        return _DEFAULT_MAX_WORKERS[resource]


RESOURCES: dict[ResourceType, ResourceSpec] = {
    r: ResourceSpec(resource=r, max_workers=_max_workers_for(r))
    for r in _DEFAULT_MAX_WORKERS
}


def spec_for(job_type: JobType) -> JobTypeSpec:
    return JOB_TYPES[job_type]


def resource_spec_for(resource: ResourceType) -> ResourceSpec:
    return RESOURCES[resource]
