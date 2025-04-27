"""
Configuration Module - Central config for F1 Analytics project

Mô-đun này quản lý tất cả cấu hình cho dự án, bao gồm:
- Đường dẫn thư mục dữ liệu
- Cấu hình cho FastF1 API
- Cấu hình cho Spark
- Tham số cho các mô hình ML
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Any
from dotenv import load_dotenv

# Load biến môi trường từ file .env (nếu có)
load_dotenv()

# ====================== PATH CONFIGURATION ======================
# Xác định đường dẫn gốc dự án
BASE_DIR = Path(__file__).resolve().parent

# Các thư mục dữ liệu
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
MODELS_DIR = DATA_DIR / "models"
CACHE_DIR = DATA_DIR / "cache"
LOG_DIR = DATA_DIR / "logs"

# Tạo thư mục nếu chưa tồn tại
for dir_path in [RAW_DATA_DIR, PROCESSED_DATA_DIR, MODELS_DIR, CACHE_DIR, LOG_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# ====================== LOGGING CONFIGURATION ======================
# Cấu hình logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_FILE = LOG_DIR / "f1_analytics.log"

# Thiết lập logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# ====================== F1 DATA CONFIGURATION ======================
# Các mùa giải cần thu thập dữ liệu
SEASONS: List[int] = list(range(
    int(os.getenv("START_YEAR", "2021")),
    int(os.getenv("END_YEAR", "2025")) + 1
))

# Cấu hình FastF1 API
F1_API_CONFIG: Dict[str, Any] = {
    "retry_attempts": int(os.getenv("F1_RETRY_ATTEMPTS", "3")),
    "retry_delay": int(os.getenv("F1_RETRY_DELAY", "5")),
    "cache_enabled": os.getenv("F1_CACHE_ENABLED", "True").lower() == "true",
    "min_records_per_event": int(os.getenv("MIN_RECORDS_PER_EVENT", "20"))
}

# Danh sách session types cần thu thập
SESSION_TYPES = ["R", "Q"]  # Race và Qualifying

# ====================== SPARK CONFIGURATION ======================
# Cấu hình Spark cho xử lý dữ liệu lớn
SPARK_CONFIG: Dict[str, str] = {
    "spark.app.name": "F1Analytics",
    "spark.executor.memory": os.getenv("SPARK_EXECUTOR_MEMORY", "4g"),
    "spark.driver.memory": os.getenv("SPARK_DRIVER_MEMORY", "4g"),
    "spark.sql.shuffle.partitions": os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"),
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.execution.arrow.pyspark.enabled": "true"
}

# ====================== MODEL CONFIGURATION ======================
# Tham số cho các mô hình ML
MODEL_PARAMS: Dict[str, Dict[str, Any]] = {
    "random_forest": {
        "n_estimators": int(os.getenv("RF_N_ESTIMATORS", "100")),
        "max_depth": int(os.getenv("RF_MAX_DEPTH", "10")),
        "random_state": 42
    },
    "gradient_boosting": {
        "n_estimators": int(os.getenv("GB_N_ESTIMATORS", "100")),
        "learning_rate": float(os.getenv("GB_LEARNING_RATE", "0.1")),
        "max_depth": int(os.getenv("GB_MAX_DEPTH", "5")),
        "random_state": 42
    }
}

# ====================== FEATURE CONFIGURATION ======================
# Cấu hình cho feature engineering
FEATURE_CONFIG: Dict[str, Any] = {
    "window_sizes": [3, 5, 10],  # Kích thước cửa sổ cho các tính năng rolling
    "target_variable": "position",
    "categorical_features": ["driver_id", "team", "track_id", "compound"],
    "numerical_features": [
        "lap_time", "sector1_time", "sector2_time", "sector3_time", 
        "speed_trap", "tyre_life"
    ]
}

# ====================== MLFLOW CONFIGURATION ======================
# Cấu hình MLflow cho tracking experiments
MLFLOW_CONFIG: Dict[str, str] = {
    "tracking_uri": os.getenv("MLFLOW_TRACKING_URI", ""),
    "experiment_name": os.getenv("MLFLOW_EXPERIMENT_NAME", "f1-analytics")
}

# ====================== HELPER FUNCTIONS ======================
def get_path_for_season(year: int, create: bool = True) -> Path:
    """
    Tạo và trả về đường dẫn cho một mùa giải cụ thể
    
    Args:
        year: Năm của mùa giải
        create: Có tạo thư mục nếu chưa tồn tại không
        
    Returns:
        Path: Đường dẫn đến thư mục của mùa giải
    """
    season_dir = RAW_DATA_DIR / str(year)
    if create and not season_dir.exists():
        season_dir.mkdir(parents=True)
    return season_dir

def get_spark_session_config() -> Dict[str, str]:
    """
    Trả về cấu hình Spark cho session hiện tại
    
    Returns:
        Dict[str, str]: Cấu hình Spark
    """
    # Điều chỉnh cấu hình dựa trên môi trường
    config = SPARK_CONFIG.copy()
    
    # Thêm cấu hình đặc biệt cho môi trường production nếu cần
    if os.getenv("ENVIRONMENT") == "production":
        config["spark.executor.instances"] = os.getenv("SPARK_EXECUTOR_INSTANCES", "2")
        
    return config
