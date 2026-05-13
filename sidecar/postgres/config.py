"""Lecture de la configuration depuis .env"""
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../../.env"))

POSTGRES_DSN: str = os.environ["POSTGRES_DSN"]
SIDECAR_PORT: int = int(os.getenv("POSTGRES_SIDECAR_PORT", "8001"))
MAX_ROWS: int = int(os.getenv("MAX_ROWS", "10000").replace("_", ""))
