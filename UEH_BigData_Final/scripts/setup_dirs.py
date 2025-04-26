"""
Setup Directories Script - Safe Initialization
"""

import sys
from pathlib import Path

def main():
    # Thêm project root vào Python path
    project_root = Path(__file__).resolve().parent.parent
    sys.path.insert(0, str(project_root))

    try:
        from src.config.settings import settings
        print("🔄 Đã khởi tạo cấu hình đường dẫn thành công!")
        print(f"📁 Project Root: {settings.PROJECT_ROOT}")
    except ImportError as e:
        print(f"❌ Lỗi import: {str(e)}")
        raise

if __name__ == "__main__":
    main()
