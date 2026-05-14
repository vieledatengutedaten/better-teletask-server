from collections.abc import Sequence
from typing import TypeVar, cast

from lib.models.jobs import BaseJob, JobParamsBase, JobType

TJob = TypeVar("TJob", bound=BaseJob)
TParams = TypeVar("TParams", bound=JobParamsBase)


def require_jobs(
    jobs: Sequence[BaseJob],
    job_cls: type[TJob],
    expected_job_type: JobType,
) -> list[TJob]:
    wrong: list[BaseJob] = []
    for job in jobs:
        if not isinstance(job, job_cls) or job.job_type != expected_job_type:
            wrong.append(job)

    if wrong:
        preview = ", ".join(
            f"{type(job).__name__}(id={job.id!r}, job_type={job.job_type!r})"
            for job in wrong[:5]
        )
        more = "" if len(wrong) <= 5 else f" (+{len(wrong) - 5} more)"
        raise TypeError(
            f"Expected all jobs to be {job_cls.__name__} with job_type={expected_job_type!r}, but got {len(wrong)}/{len(jobs)} invalid: {preview}{more}"
        )

    # After the checks above, this cast is safe.
    return cast(list[TJob], list(jobs))


def require_params(
    params: Sequence[JobParamsBase],
    params_cls: type[TParams],
    expected_job_type: JobType,
) -> list[TParams]:
    wrong: list[JobParamsBase] = []
    for item in params:
        if not isinstance(item, params_cls):
            wrong.append(item)

    if wrong:
        preview = ", ".join(type(item).__name__ for item in wrong[:5])
        more = "" if len(wrong) <= 5 else f" (+{len(wrong) - 5} more)"
        raise TypeError(
            f"Expected all params to be {params_cls.__name__} for job_type={expected_job_type!r}, but got {len(wrong)}/{len(params)} invalid: {preview}{more}"
        )

    # After the checks above, this cast is safe.
    return cast(list[TParams], list(params))
