from typing import Literal, override

from pydantic import Field

from lib.models.jobs.base import BaseJob, JobParamsBase, JobResultBase, JobType


class ScrapeLectureDataParams(JobParamsBase):
    teletask_id: int


class ScrapeLectureDataResult(JobResultBase):
    job_type: Literal["scrape_lecture_data"] = "scrape_lecture_data"
    lecture_id: int
    lecturer_ids: list[int | None] = Field(default_factory=list)
    lecturer_names: list[str] = Field(default_factory=list)
    date: str | None = None
    language: str | None = None
    duration: str | None = None
    lecture_title: str
    series_id: int | None = None
    series_name: str | None = None


class ScrapeLectureDataJob(BaseJob):
    job_type: JobType = "scrape_lecture_data"
    params: ScrapeLectureDataParams

    @override
    def model_post_init(self, __context: object) -> None:
        if not self.id:
            self.id = f"sc-{self.params.teletask_id}"
