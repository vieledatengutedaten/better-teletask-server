from abc import ABC, abstractmethod
from typing import override
from app.models.dataclasses import TranscriptionJob, TranslationJob


class Worker(ABC):

    @abstractmethod
    def transcribe(self, worker_id: str, jobs: list[TranscriptionJob]) -> None:
        pass

    @abstractmethod
    def translate(self, worker_id: str, jobs: list[TranslationJob]) -> None:
        pass


class MockWorker(Worker):

    @override
    def transcribe(self, worker_id: str, jobs: list[TranscriptionJob]) -> None:
        return

    @override
    def translate(self, worker_id: str, jobs: list[TranslationJob]) -> None:
        return