from app.worker.worker import MockWorker, Worker
from lib.models.dataclasses import TranslationJob, TranscriptionJob
from app.worker.local.local_worker import LocalWorker

class WorkerManager:
    transcribeWorker: Worker
    translateWorker: Worker

    def __init__(self, transcribeWorker: Worker | None = None, translateWorker: Worker | None = None):
        self.transcribeWorker = transcribeWorker or LocalWorker()
        self.translateWorker = translateWorker or LocalWorker()

    def translate(self, worker_id: str, jobs: list[TranslationJob]) -> None:
        self.translateWorker.translate(worker_id=worker_id, jobs=jobs)

    def transcribe(self, worker_id: str, jobs: list[TranscriptionJob]) -> None:
        self.transcribeWorker.transcribe(worker_id=worker_id, jobs=jobs)

class MockWorkerManager(WorkerManager):

    def __init__(self):
        super().__init__(transcribeWorker=MockWorker(), translateWorker=MockWorker())