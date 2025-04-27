"""
F1 Data Collector Module

Module này chịu trách nhiệm:
1. Thu thập dữ liệu từ FastF1 API
2. Xử lý dữ liệu thô
3. Lưu trữ dữ liệu theo cấu trúc chuẩn
4. Xử lý lỗi và retry logic
"""

import fastf1
import pandas as pd
import numpy as np
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union, Any
from datetime import datetime
from functools import wraps

# Import cấu hình từ module config
import config

# Thiết lập logger
logger = logging.getLogger(__name__)

def retry_decorator(max_attempts: int = 3, delay: int = 5):
    """
    Decorator để retry các API calls khi gặp lỗi
    
    Args:
        max_attempts: Số lần thử tối đa
        delay: Số giây chờ giữa các lần thử
    """
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
    """
    Class thu thập và xử lý dữ liệu F1 từ FastF1 API
    """
    
    def __init__(self, cache_enabled: bool = True):
        """
        Khởi tạo collector với cache
        
        Args:
            cache_enabled: Bật/tắt cache cho FastF1
        """
        self.cache_dir = config.CACHE_DIR
        
        # Bật cache nếu được yêu cầu
        if cache_enabled:
            fastf1.Cache.enable_cache(str(self.cache_dir))
            logger.info(f"FastF1 cache enabled at {self.cache_dir}")
        
        # Lấy cấu hình API từ config
        self.retry_attempts = config.F1_API_CONFIG["retry_attempts"]
        self.retry_delay = config.F1_API_CONFIG["retry_delay"]
        self.min_records = config.F1_API_CONFIG["min_records_per_event"]
        
        logger.info("F1Collector initialized successfully")
    
    @retry_decorator(max_attempts=3, delay=5)
    def get_event_schedule(self, year: int) -> pd.DataFrame:
        """
        Lấy lịch các sự kiện F1 cho một năm cụ thể
        
        Args:
            year: Năm cần lấy lịch
            
        Returns:
            DataFrame chứa lịch các sự kiện
        """
        logger.info(f"Fetching event schedule for {year}")
        schedule = fastf1.get_event_schedule(year)
        
        # Lọc các sự kiện chính thức (loại bỏ Testing, Sprint)
        official_events = schedule[
            (schedule['EventFormat'] == 'conventional') &
            (schedule['EventName'].str.contains('Grand Prix')) &
            (~schedule['EventName'].str.contains('Testing|Sprint', case=False))
        ]
        
        logger.info(f"Found {len(official_events)} official events for {year}")
        return official_events
    
    @retry_decorator(max_attempts=3, delay=5)
    def get_session(self, year: int, gp_name: str, session_type: str) -> Optional[fastf1.core.Session]:
        """
        Lấy dữ liệu session từ FastF1
        
        Args:
            year: Năm của sự kiện
            gp_name: Tên Grand Prix
            session_type: Loại session (R=Race, Q=Qualifying)
            
        Returns:
            Session object hoặc None nếu có lỗi
        """
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
    
    def process_lap_data(self, session: fastf1.core.Session) -> pd.DataFrame:
        """
        Xử lý dữ liệu lap từ session
        
        Args:
            session: Session object từ FastF1
            
        Returns:
            DataFrame đã xử lý
        """
        if not hasattr(session, 'laps') or session.laps.empty:
            logger.warning("Session has no lap data")
            return pd.DataFrame()
            
        # Lấy dữ liệu lap
        laps = session.laps.copy()
        
        # Thêm metadata
        laps['year'] = session.event.year
        laps['event_name'] = session.event.EventName
        laps['track_name'] = session.event.Location
        laps['session_type'] = session.name
        laps['session_date'] = session.date
        
        # Chuyển đổi các cột thời gian từ timedelta sang seconds
        # Thêm PitInTime và PitOutTime vào danh sách
        time_cols = ['LapTime', 'Sector1Time', 'Sector2Time', 'Sector3Time', 'PitInTime', 'PitOutTime']
        for col in time_cols:
            if col in laps.columns:
                laps[col] = laps[col].dt.total_seconds() * 1000
        
        # Chuẩn hóa tên cột (lowercase và snake_case)
        laps.columns = [col.lower() for col in laps.columns]
        
        # Thêm timestamp thu thập
        laps['collected_at'] = datetime.now().isoformat()
        
        logger.info(f"Processed {len(laps)} laps from {session.event.EventName}")
        return laps

    
    def process_driver_data(self, session: fastf1.core.Session) -> pd.DataFrame:
        """
        Xử lý dữ liệu driver từ session
        
        Args:
            session: Session object từ FastF1
            
        Returns:
            DataFrame chứa thông tin driver
        """
        if not hasattr(session, 'drivers') or not session.drivers:
            logger.warning("Session has no driver data")
            return pd.DataFrame()
            
        drivers_data = []
        
        for driver_number in session.drivers:
            try:
                driver_info = session.get_driver(driver_number)
                
                # Xử lý trường hợp thiếu dữ liệu
                driver_data = {
                    'driver_number': driver_number,
                    'driver_code': driver_info.get('Abbreviation', ''),
                    'first_name': driver_info.get('FirstName', ''),
                    'last_name': driver_info.get('LastName', ''),
                    'full_name': f"{driver_info.get('FirstName', '')} {driver_info.get('LastName', '')}",
                    'team_name': driver_info.get('TeamName', ''),
                    'year': session.event.year,
                    'event_name': session.event.EventName,
                    'collected_at': datetime.now().isoformat()
                }
                
                # Thêm quốc tịch nếu có
                if 'Nationality' in driver_info:
                    driver_data['nationality'] = driver_info['Nationality']
                elif 'Country' in driver_info and isinstance(driver_info['Country'], dict):
                    driver_data['nationality'] = driver_info['Country'].get('Name', '')
                
                drivers_data.append(driver_data)
            except Exception as e:
                logger.warning(f"Error processing driver {driver_number}: {str(e)}")
                continue
                
        drivers_df = pd.DataFrame(drivers_data)
        logger.info(f"Processed {len(drivers_df)} drivers from {session.event.EventName}")
        return drivers_df
    
    def process_results_data(self, session: fastf1.core.Session) -> pd.DataFrame:
        """
        Xử lý dữ liệu kết quả từ session
        
        Args:
            session: Session object từ FastF1
            
        Returns:
            DataFrame chứa kết quả
        """
        if not hasattr(session, 'results') or session.results.empty:
            logger.warning("Session has no results data")
            return pd.DataFrame()
            
        # Lấy dữ liệu kết quả
        results = session.results.copy()
        
        # Thêm metadata
        results['year'] = session.event.year
        results['event_name'] = session.event.EventName
        results['track_name'] = session.event.Location
        results['session_type'] = session.name
        results['session_date'] = session.date
        results['collected_at'] = datetime.now().isoformat()
        
        # Chuẩn hóa tên cột
        results.columns = [col.lower() for col in results.columns]
        
        logger.info(f"Processed results for {session.event.EventName}")
        return results
    
    def save_data(self, df: pd.DataFrame, year: int, gp_name: str, data_type: str) -> Path:
        """
        Lưu DataFrame vào file Parquet
        
        Args:
            df: DataFrame cần lưu
            year: Năm của dữ liệu
            gp_name: Tên Grand Prix
            data_type: Loại dữ liệu (laps, drivers, results)
            
        Returns:
            Path đến file đã lưu
        """
        if df.empty:
            logger.warning(f"No {data_type} data to save for {gp_name} {year}")
            return None
            
        # Tạo thư mục cho năm
        year_dir = config.get_path_for_season(year)
        
        # Chuẩn hóa tên GP
        gp_name_safe = gp_name.lower().replace(' ', '_').replace('-', '_')
        
        # Tạo đường dẫn file
        file_path = year_dir / f"{gp_name_safe}_{data_type}.parquet"
        
        # Lưu file
        df.to_parquet(file_path, index=False)
        logger.info(f"Saved {len(df)} records to {file_path}")
        
        return file_path
    
    def collect_event_data(self, year: int, gp_name: str, session_types: List[str] = None) -> Dict[str, Path]:
        """
        Thu thập toàn bộ dữ liệu cho một sự kiện
        
        Args:
            year: Năm của sự kiện
            gp_name: Tên Grand Prix
            session_types: Danh sách loại session cần thu thập (mặc định: Race, Qualifying)
            
        Returns:
            Dict chứa đường dẫn đến các file đã lưu
        """
        if session_types is None:
            session_types = config.SESSION_TYPES
            
        result_paths = {}
        
        for session_type in session_types:
            try:
                # Lấy session
                session = self.get_session(year, gp_name, session_type)
                if not session:
                    continue
                    
                # Xử lý dữ liệu lap
                laps_df = self.process_lap_data(session)
                if not laps_df.empty:
                    lap_path = self.save_data(laps_df, year, gp_name, f"{session_type.lower()}_laps")
                    result_paths[f"{session_type.lower()}_laps"] = lap_path
                
                # Xử lý dữ liệu driver
                drivers_df = self.process_driver_data(session)
                if not drivers_df.empty:
                    driver_path = self.save_data(drivers_df, year, gp_name, f"{session_type.lower()}_drivers")
                    result_paths[f"{session_type.lower()}_drivers"] = driver_path
                
                # Xử lý dữ liệu kết quả
                results_df = self.process_results_data(session)
                if not results_df.empty:
                    results_path = self.save_data(results_df, year, gp_name, f"{session_type.lower()}_results")
                    result_paths[f"{session_type.lower()}_results"] = results_path
                    
            except Exception as e:
                logger.error(f"Error collecting {session_type} data for {gp_name} {year}: {str(e)}")
                continue
                
        return result_paths
    
    def collect_season_data(self, year: int) -> Dict[str, Dict[str, Path]]:
        """
        Thu thập dữ liệu cho toàn bộ mùa giải
        
        Args:
            year: Năm cần thu thập
            
        Returns:
            Dict chứa kết quả thu thập cho từng sự kiện
        """
        logger.info(f"Starting data collection for {year} season")
        
        # Lấy lịch sự kiện
        schedule = self.get_event_schedule(year)
        if schedule.empty:
            logger.warning(f"No events found for {year}")
            return {}
            
        results = {}
        
        # Thu thập dữ liệu cho từng sự kiện
        for _, event in schedule.iterrows():
            gp_name = event['EventName']
            logger.info(f"Collecting data for {gp_name} {year}")
            
            event_results = self.collect_event_data(year, gp_name)
            results[gp_name] = event_results
            
        logger.info(f"Completed data collection for {year} season")
        return results


# Hàm tiện ích để sử dụng từ CLI
def collect_data_for_year(year: int) -> Dict[str, Dict[str, Path]]:
    """
    Hàm tiện ích để thu thập dữ liệu cho một năm
    
    Args:
        year: Năm cần thu thập
        
    Returns:
        Dict chứa kết quả thu thập
    """
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
