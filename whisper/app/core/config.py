import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

# --- Database ---
DB_NAME = os.environ.get("POSTGRES_DB")
DB_USER = os.environ.get("POSTGRES_USER")
DB_PASS = os.environ.get("POSTGRES_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")

# --- Whisper / ASR ---
ASR_MODEL = os.environ.get("ASR_MODEL")
COMPUTE_TYPE = os.environ.get("COMPUTE_TYPE")
DEVICE = os.environ.get("DEVICE", "cuda")

# --- Paths ---
SCRIPT_DIR = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
VTT_DEST_FOLDER = os.environ.get("VTT_DEST_FOLDER", "output/")
RECORDING_SOURCE_FOLDER = os.environ.get("RECORDING_SOURCE_FOLDER", "input/")
INPUT_PATH = os.path.join(SCRIPT_DIR, RECORDING_SOURCE_FOLDER)
OUTPUT_PATH = os.path.join(SCRIPT_DIR, VTT_DEST_FOLDER)

# --- Auth ---
USERNAME_COOKIE = os.environ.get("USERNAME_COOKIE")

# --- URLs ---
BASE_URL = "https://www.tele-task.de/lecture/video/"

# --- Translation / Ollama ---
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "gpt-oss:120b-cloud")
