from abc import ABC, abstractmethod
from app.models.dataclasses import TranscriptionJob, TranslationJob


class Worker(ABC):

    @abstractmethod
    def transcribe(self, worker_id: str, jobs: list[TranscriptionJob]) -> None:
        pass

    @abstractmethod
    def translate(self, worker_id: str, jobs: list[TranslationJob]) -> None:
        pass
