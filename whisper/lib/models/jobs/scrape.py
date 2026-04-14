from typing import Literal, override

from pydantic import BaseModel

from lib.models.jobs.base import BaseJob, JobResultBase, JobType


class ScrapeLectureDataParams(BaseModel):
    teletask_id: int


class ScrapeLectureDataResult(JobResultBase):
    job_type: Literal["scrape_lecture_data"] = "scrape_lecture_data"


class ScrapeLectureDataJob(BaseJob):
    job_type: JobType = "scrape_lecture_data"
    params: ScrapeLectureDataParams

    @override
    def model_post_init(self, __context: object) -> None:
        if not self.id:
            self.id = f"sc-{self.params.teletask_id}"
