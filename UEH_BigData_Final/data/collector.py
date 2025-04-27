"""
F1 Data Collector Module - Raw Data Collection

Module này chỉ chịu trách nhiệm:
1. Thu thập dữ liệu thô từ FastF1 API
2. Lưu trữ dữ liệu thô theo cấu trúc chuẩn
3. Xử lý lỗi và retry logic
"""

import fastf1
import pandas as pd
import logging
import time
import json
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
from functools import wraps

# Import cấu hình từ module config
import config

# Thiết lập logger
logger = logging.getLogger(__name__)

def retry_decorator(max_attempts: int = 3, delay: int = 5):
    """Decorator để retry các API calls khi gặp lỗi"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    if attempts == max_attempts:
                        logger.error(f"Failed after {max_attempts} attempts: {str(e)}")
                        raise
                    logger.warning(f"Attempt {attempts} failed: {str(e)}. Retrying in {delay} seconds...")
                    time.sleep(delay)
        return wrapper
    return decorator

class F1Collector:
    """Class thu thập dữ liệu thô F1 từ FastF1 API"""
    
    def __init__(self, cache_enabled: bool = True):
        """Khởi tạo collector với cache"""
        self.cache_dir = config.CACHE_DIR
        
        # Bật cache nếu được yêu cầu
        if cache_enabled:
            fastf1.Cache.enable_cache(str(self.cache_dir))
            logger.info(f"FastF1 cache enabled at {self.cache_dir}")
        
        # Lấy cấu hình API từ config
        self.retry_attempts = config.F1_API_CONFIG["retry_attempts"]
        self.retry_delay = config.F1_API_CONFIG["retry_delay"]
        
        logger.info("F1Collector initialized successfully")
    
    @retry_decorator(max_attempts=3, delay=5)
    def get_event_schedule(self, year: int) -> pd.DataFrame:
        """Lấy lịch các sự kiện F1 cho một năm cụ thể"""
        logger.info(f"Fetching event schedule for {year}")
        schedule = fastf1.get_event_schedule(year)
        
        # Lọc các sự kiện chính thức
        official_events = schedule[
            (schedule['EventFormat'] == 'conventional') &
            (schedule['EventName'].str.contains('Grand Prix')) &
            (~schedule['EventName'].str.contains('Testing|Sprint', case=False))
        ]
        
        logger.info(f"Found {len(official_events)} official events for {year}")
        return official_events
    
    @retry_decorator(max_attempts=3, delay=5)
    def get_session(self, year: int, gp_name: str, session_type: str) -> Optional[fastf1.core.Session]:
        """Lấy dữ liệu session từ FastF1"""
        logger.info(f"Loading {session_type} session for {gp_name} {year}")
        try:
            session = fastf1.get_session(year, gp_name, session_type)
            session.load()
            
            # Kiểm tra xem session có dữ liệu không
            if not hasattr(session, 'laps') or session.laps.empty:
                logger.warning(f"No lap data for {gp_name} {year} {session_type}")
                return None
                
            return session
        except Exception as e:
            logger.error(f"Error loading session {session_type} for {gp_name} {year}: {str(e)}")
            raise
    
    def save_raw_data(self, data: Any, year: int, gp_name: str, data_type: str) -> Path:
        """Lưu dữ liệu thô vào file"""
        if data is None:
            logger.warning(f"No {data_type} data to save for {gp_name} {year}")
            return None
            
        # Tạo thư mục cho năm
        year_dir = config.get_path_for_season(year)
        
        # Chuẩn hóa tên GP
        gp_name_safe = gp_name.lower().replace(' ', '_').replace('-', '_').replace('/', '_')
        
        # Tạo đường dẫn file
        file_path = year_dir / f"{gp_name_safe}_{data_type}.parquet"
        
        # Lưu file theo loại dữ liệu
        if isinstance(data, pd.DataFrame):
            data.to_parquet(file_path, index=False)
            logger.info(f"Saved {len(data)} records to {file_path}")
        else:
            # Lưu metadata
            with open(year_dir / f"{gp_name_safe}_{data_type}_metadata.json", 'w') as f:
                json.dump({
                    "collected_at": datetime.now().isoformat(),
                    "year": year,
                    "event_name": gp_name,
                    "data_type": data_type
                }, f)
            
        return file_path
    
    def collect_event_data(self, year: int, gp_name: str, session_types: List[str] = None) -> Dict[str, Path]:
        """Thu thập toàn bộ dữ liệu thô cho một sự kiện"""
        if session_types is None:
            session_types = config.SESSION_TYPES
            
        result_paths = {}
        
        for session_type in session_types:
            try:
                # Lấy session
                session = self.get_session(year, gp_name, session_type)
                if not session:
                    continue
                    
                # Lưu dữ liệu lap thô
                if hasattr(session, 'laps') and not session.laps.empty:
                    lap_path = self.save_raw_data(session.laps, year, gp_name, f"{session_type.lower()}_laps")
                    result_paths[f"{session_type.lower()}_laps"] = lap_path
                
                # Lưu dữ liệu driver thô
                if hasattr(session, 'drivers') and session.drivers:
                    driver_data = pd.DataFrame([
                        {
                            'driver_number': d,
                            'driver_code': session.get_driver(d)['Abbreviation'],
                            'first_name': session.get_driver(d)['FirstName'],
                            'last_name': session.get_driver(d)['LastName'],
                            'team_name': session.get_driver(d)['TeamName']
                        }
                        for d in session.drivers
                    ])
                    driver_path = self.save_raw_data(driver_data, year, gp_name, f"{session_type.lower()}_drivers")
                    result_paths[f"{session_type.lower()}_drivers"] = driver_path
                
                # Lưu dữ liệu kết quả thô
                if hasattr(session, 'results') and not session.results.empty:
                    results_path = self.save_raw_data(session.results, year, gp_name, f"{session_type.lower()}_results")
                    result_paths[f"{session_type.lower()}_results"] = results_path
                    
                # Lưu dữ liệu thời tiết nếu có
                if hasattr(session, 'weather_data') and not session.weather_data.empty:
                    weather_path = self.save_raw_data(session.weather_data, year, gp_name, f"{session_type.lower()}_weather")
                    result_paths[f"{session_type.lower()}_weather"] = weather_path
                    
            except Exception as e:
                logger.error(f"Error collecting {session_type} data for {gp_name} {year}: {str(e)}")
                continue
                
        return result_paths
    
    def collect_season_data(self, year: int) -> Dict[str, Dict[str, Path]]:
        """Thu thập dữ liệu thô cho toàn bộ mùa giải"""
        logger.info(f"Starting raw data collection for {year} season")
        
        # Lấy lịch sự kiện
        schedule = self.get_event_schedule(year)
        if schedule.empty:
            logger.warning(f"No events found for {year}")
            return {}
            
        results = {}
        
        # Thu thập dữ liệu cho từng sự kiện
        for _, event in schedule.iterrows():
            gp_name = event['EventName']
            logger.info(f"Collecting raw data for {gp_name} {year}")
            
            event_results = self.collect_event_data(year, gp_name)
            results[gp_name] = event_results
            
        logger.info(f"Completed raw data collection for {year} season")
        return results

# Hàm tiện ích để sử dụng từ CLI
def collect_data_for_year(year: int) -> Dict[str, Dict[str, Path]]:
    """Hàm tiện ích để thu thập dữ liệu thô cho một năm"""
    collector = F1Collector(cache_enabled=config.F1_API_CONFIG["cache_enabled"])
    return collector.collect_season_data(year)

if __name__ == "__main__":
    # Ví dụ sử dụng từ command line
    import sys
    
    if len(sys.argv) > 1:
        year = int(sys.argv[1])
        collect_data_for_year(year)
    else:
        print("Usage: python -m f1_analytics.data.collector <year>")
