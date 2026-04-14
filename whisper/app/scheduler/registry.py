"""Central registry for jobtypes and resources.

Adding a new jobtype = four edits:
  1. Define Job/Params/Result classes in lib/models/dataclasses.py
  2. Write a JobHandler subclass in app/scheduler/job_handlers.py
    3. Define factory/is_done/done_ids callbacks in app/scheduler/pipeline_specs.py
    4. Add a JobTypeSpec entry to JOB_TYPES below

Adding a new resource = one edit:
  1. Add to ResourceType literal in dataclasses.py and to RESOURCES below

The scheduler, queues, and worker manager read everything from this registry.
The pipeline coordinator also reads workflow order and completion callbacks here.
"""

import os
from collections.abc import Callable
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
from app.scheduler.job_handlers import (
    JobHandler,
    ScrapeLectureDataJobHandler,
    TranscriptionJobHandler,
    TranslationJobHandler,
)
from app.scheduler.pipeline_specs import (
    scrape_done_ids,
    scrape_factory,
    scrape_is_done,
    transcribe_done_ids,
    transcribe_factory,
    transcribe_is_done,
    translate_done_ids,
    translate_factory,
    translate_is_done,
)


@dataclass(frozen=True)
class JobTypeSpec:
    job_type: JobType
    resource: ResourceType
    job_cls: type[BaseJob]
    result_cls: type[JobResultBase]
    handler: JobHandler
    batch_size: int       # how many jobs of this type per worker invocation
    base_priority: int    # higher = scheduler picks this jobtype first
    stage_name: str
    stage_order: int
    factory: Callable[[int, int], list[BaseJob]]
    is_done: Callable[[int], bool]
    done_ids: Callable[[], set[int]]


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
        handler=ScrapeLectureDataJobHandler(),
        batch_size=10,
        base_priority=200,
        stage_name="scrape",
        stage_order=10,
        factory=scrape_factory,
        is_done=scrape_is_done,
        done_ids=scrape_done_ids,
    ),
    "transcription": JobTypeSpec(
        job_type="transcription",
        resource="whisper",
        job_cls=TranscriptionJob,
        result_cls=TranscriptionResult,
        handler=TranscriptionJobHandler(),
        batch_size=2,
        base_priority=100,
        stage_name="transcribe",
        stage_order=20,
        factory=transcribe_factory,
        is_done=transcribe_is_done,
        done_ids=transcribe_done_ids,
    ),
    "translation": JobTypeSpec(
        job_type="translation",
        resource="ollama",
        job_cls=TranslationJob,
        result_cls=TranslationResult,
        handler=TranslationJobHandler(),
        batch_size=4,
        base_priority=50,
        stage_name="translate",
        stage_order=30,
        factory=translate_factory,
        is_done=translate_is_done,
        done_ids=translate_done_ids,
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


def ordered_pipeline_specs() -> list[JobTypeSpec]:
    return sorted(JOB_TYPES.values(), key=lambda spec: spec.stage_order)


def resource_spec_for(resource: ResourceType) -> ResourceSpec:
    return RESOURCES[resource]
