import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

# --- General ---
ENVIRONMENT: str = os.environ.get("ENVIRONMENT", "vm").lower()  # dev, vm, worker

# --- Database ---
DB_NAME = os.environ.get("POSTGRES_DB")
DB_USER = os.environ.get("POSTGRES_USER")
DB_PASS = os.environ.get("POSTGRES_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
SLURM_USER_TOKEN = os.environ.get("SLURM_USER_TOKEN")

# --- Whisper / ASR ---
ASR_MODEL = os.environ.get("ASR_MODEL")
COMPUTE_TYPE = os.environ.get("COMPUTE_TYPE", "int8")
DEVICE = os.environ.get("DEVICE", "cuda")

# --- Paths ---
# Data dirs are resolved relative to the runtime working directory (or DATA_DIR),
# not the source tree, so they keep working once the package is installed in a venv.
SCRIPT_DIR = Path(os.environ.get("DATA_DIR", ".")).resolve()
VTT_DEST_FOLDER = os.environ.get("VTT_DEST_FOLDER", "output/")
RECORDING_SOURCE_FOLDER = os.environ.get("RECORDING_SOURCE_FOLDER", "input/")
INPUT_PATH = SCRIPT_DIR / RECORDING_SOURCE_FOLDER
OUTPUT_PATH = SCRIPT_DIR / VTT_DEST_FOLDER
INPUT_PATH.mkdir(parents=True, exist_ok=True)
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

# --- Auth ---
USERNAME_COOKIE = os.environ.get("USERNAME_COOKIE")
VM_WORKER_AUTH_TOKEN = os.environ.get("VM_WORKER_AUTH_TOKEN")

# --- Middleware ---
CORS_ORIGINS = [
    h.strip()
    for h in os.environ.get("CORS_ORIGINS", "https://www.tele-task.de").split(",")
]
HTTPS_REDIRECT = os.environ.get("HTTPS_REDIRECT", "false").lower() == "true"

# --- URLs ---
BASE_URL = "https://www.tele-task.de/lecture/video/"
# URL SLURM compute nodes use to reach this scheduler (job pull + callbacks).
SCHEDULER_URL = os.environ.get("SCHEDULER_URL", "http://127.0.0.1:8000")

# --- Translation / Ollama ---
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "gpt-oss:120b-cloud")
