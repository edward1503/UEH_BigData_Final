"""
Central Configuration Module - Optimized for Big Data Projects
"""

import sys
import os
import logging
from pathlib import Path
from typing import Dict, Any, List
from dotenv import load_dotenv  # Sử dụng python-dotenv
import json
# Load environment variables từ file .env nếu có
load_dotenv()

class ProjectConfig:
    """Singleton class quản lý toàn bộ cấu hình dự án"""
    
    def __init__(self):
        # ---------------------------
        # 1. Path Configuration
        # ---------------------------
        self.PROJECT_ROOT = self._get_project_root()
        self.CACHE_DIR = self._create_dir(self.PROJECT_ROOT / "cache")
        self.DATA_DIR = self._create_dir(self.PROJECT_ROOT / "data")
        
        # Raw data paths
        self.RAW_DATA_DIR = self._create_dir(self.DATA_DIR / "raw")
        self.INTERIM_DIR = self._create_dir(self.DATA_DIR / "interim")
        self.PROCESSED_DIR = self._create_dir(self.DATA_DIR / "processed")
        
        # Model artifacts
        self.MODEL_DIR = self._create_dir(self.PROJECT_ROOT / "models")
        
        # ---------------------------
        # 2. Data Collection Config
        # ---------------------------
        self.SEASONS: List[int] = list(
            range(
                int(os.getenv("START_YEAR", 2021)), 
                int(os.getenv("END_YEAR", 2024)) + 1
            )
        )
        
        self.F1_API_RETRY_CONFIG: Dict[str, Any] = {
            "max_retries": int(os.getenv("F1_MAX_RETRIES", 3)),
            "delay": int(os.getenv("F1_RETRY_DELAY", 10)),
            "backoff": int(os.getenv("F1_RETRY_BACKOFF", 2))
        }
        
        # ---------------------------
        # 3. Spark & ML Config
        # ---------------------------
        self.SPARK_CONFIG: Dict[str, str] = {
            "spark.app.name": os.getenv("SPARK_APP_NAME", "F1-Analytics"),
            "spark.executor.memory": os.getenv("SPARK_EXECUTOR_MEM", "4g"),
            "spark.driver.memory": os.getenv("SPARK_DRIVER_MEM", "4g"),
            "spark.sql.shuffle.partitions": os.getenv("SPARK_SHUFFLE_PART", "8")
        }
        
        self.MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
        
        # ---------------------------
        # 4. Data Validation Config
        # ---------------------------
        self.SCHEMA_CONFIG_PATH = self.PROJECT_ROOT / "config" / "schemas"
        self.MIN_RECORDS_PER_FILE = int(os.getenv("MIN_RECORDS", 100))
        
        # ---------------------------
        # 5. Logging & Monitoring
        # ---------------------------
        self.LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
        self.DATA_VERSION = os.getenv("DATA_VERSION", "1.0.0")

    @staticmethod
    def _get_project_root() -> Path:
        """Tự động detect project root folder"""
        current_path = Path(__file__).resolve()
        while current_path.name != 'src' and current_path.parent != current_path:
            current_path = current_path.parent
        return current_path.parent  # Trỏ về folder gốc của project

    def _create_dir(self, path: Path) -> Path:
        """Tạo thư mục nếu chưa tồn tại và trả về Path object"""
        try:
            path.mkdir(parents=True, exist_ok=True)
            logging.debug(f"Created directory: {path}")
            return path
        except PermissionError as e:
            logging.critical(f"Permission denied for {path}: {str(e)}")
            raise
        except Exception as e:
            logging.critical(f"Failed to create {path}: {str(e)}")
            raise

    @property
    def feature_schema(self) -> Dict:
        """Load feature schema từ file config"""
        schema_file = self.SCHEMA_CONFIG_PATH / "feature_schema.json"
        try:
            with open(schema_file) as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Failed to load schema: {str(e)}")
            return {}

# Khởi tạo config singleton
settings = ProjectConfig()
