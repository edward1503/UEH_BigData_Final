"""
Central Configuration Module - Optimized Path Handling
"""

import os
from pathlib import Path

class ProjectConfig:
    def __init__(self):
        self.PROJECT_ROOT = self._find_project_root()
        self._init_paths()
        
    def _find_project_root(self) -> Path:
        """Tự động xác định thư mục gốc project"""
        current_dir = Path(__file__).resolve()
        while current_dir.name != 'src' and current_dir.parent != current_dir:
            current_dir = current_dir.parent
        return current_dir.parent  # Lên một cấp từ thư mục src

    def _init_paths(self):
        """Khởi tạo tất cả đường dẫn quan trọng"""
        self.DATA_DIR = self.PROJECT_ROOT / "data"
        self.RAW_DATA_DIR = self.DATA_DIR / "raw"
        self.INTERIM_DIR = self.DATA_DIR / "interim"
        self.PROCESSED_DIR = self.DATA_DIR / "processed"
        
        self.MODEL_DIR = self.PROJECT_ROOT / "models"
        self.CACHE_DIR = self.PROJECT_ROOT / "cache"
        
        # Tạo thư mục nếu chưa tồn tại
        self._create_dirs()

    def _create_dirs(self):
        """Tạo các thư mục quan trọng"""
        self.RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.INTERIM_DIR.mkdir(parents=True, exist_ok=True)
        self.PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        self.MODEL_DIR.mkdir(parents=True, exist_ok=True)
        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Khởi tạo instance duy nhất
settings = ProjectConfig()
