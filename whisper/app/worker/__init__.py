from .slurm.slurm_worker import SlurmWorker
from .local.local_worker import LocalWorker

__all__ = ["SlurmWorker", "LocalWorker"]
