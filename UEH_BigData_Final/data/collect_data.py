"""
F1 Data Collection Script

Script này sử dụng F1Collector để thu thập dữ liệu F1 từ FastF1 API
và lưu vào thư mục raw.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Dict, Any

# Thêm thư mục gốc vào sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import config
from data.collector import F1Collector

# Thiết lập logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(config.LOG_DIR / "collect_data.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("collect_data")

def collect_data(years: List[int] = None, gp_name: str = None) -> Dict[int, Any]:
    """
    Thu thập dữ liệu F1
    
    Args:
        years: Danh sách năm cần thu thập, None để sử dụng cấu hình mặc định
        gp_name: Tên Grand Prix cần thu thập, None để thu thập tất cả
        
    Returns:
        Dict chứa kết quả thu thập
    """
    if years is None:
        years = config.SEASONS
        
    logger.info(f"Starting data collection for years: {years}")
    
    collector = F1Collector(cache_enabled=config.F1_API_CONFIG["cache_enabled"])
    results = {}
    
    for year in years:
        logger.info(f"Collecting data for {year}")
        
        if gp_name:
            # Thu thập dữ liệu cho một Grand Prix cụ thể
            logger.info(f"Collecting data for {gp_name} {year}")
            event_results = collector.collect_event_data(year, gp_name)
            results[year] = {gp_name: event_results}
        else:
            # Thu thập dữ liệu cho toàn bộ mùa giải
            season_results = collector.collect_season_data(year)
            results[year] = season_results
            
    logger.info("Data collection completed")
    return results

def main():
    """Hàm chính"""
    parser = argparse.ArgumentParser(description="Collect F1 data")
    parser.add_argument("--years", type=int, nargs="+", help="Years to collect data for")
    parser.add_argument("--gp", type=str, help="Grand Prix name to collect data for")
    parser.add_argument("--cache", action="store_true", help="Enable FastF1 cache")
    
    args = parser.parse_args()
    
    # Cập nhật cấu hình cache nếu cần
    if args.cache is not None:
        config.F1_API_CONFIG["cache_enabled"] = args.cache
        
    # Thu thập dữ liệu
    collect_data(args.years, args.gp)

if __name__ == "__main__":
    main()
