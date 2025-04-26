from pathlib import Path
from src.config import settings

Path(settings.MODEL_DIR).mkdir(parents=True, exist_ok=True)
Path(settings.CACHE_DIR).mkdir(parents=True, exist_ok=True)