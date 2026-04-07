from typing import override
from app.worker.worker import Worker
from app.models.dataclasses import TranscriptionJob, TranslationJob


class LocalWorker(Worker):

    @override
    def transcribe(self, worker_id: str, jobs: list[TranscriptionJob]) -> None:
        return

    @override
    def translate(self, worker_id: str, jobs: list[TranslationJob]) -> None:
        return
