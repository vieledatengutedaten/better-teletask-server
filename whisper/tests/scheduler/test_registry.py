"""
Tests for scheduler/registry.py dependency graph validation.
"""

from dataclasses import replace
from typing import cast

import pytest

from app.scheduler.registry import JOB_TYPES, validate_job_graph
from lib.models.jobs import JobType


def test_validate_job_graph_accepts_current_registry() -> None:
    validate_job_graph(JOB_TYPES)


def test_validate_job_graph_rejects_unknown_dependency() -> None:
    broken = dict(JOB_TYPES)
    broken["translation"] = replace(
        broken["translation"],
        depends_on=(cast(JobType, "nonexistent_job"),),
    )

    with pytest.raises(ValueError, match="Unknown dependency"):
        validate_job_graph(broken)


def test_validate_job_graph_rejects_cycle() -> None:
    broken = dict(JOB_TYPES)
    broken["transcription"] = replace(
        broken["transcription"],
        depends_on=("translation",),
    )
    broken["translation"] = replace(
        broken["translation"],
        depends_on=("transcription",),
    )

    with pytest.raises(ValueError, match="Cyclic"):
        validate_job_graph(broken)
