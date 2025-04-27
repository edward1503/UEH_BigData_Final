"""
F1 Data Processor Module

Module này chịu trách nhiệm:
1. Đọc dữ liệu thô từ thư mục raw
2. Làm sạch và chuyển đổi dữ liệu
3. Tích hợp dữ liệu từ nhiều nguồn
4. Lưu dữ liệu đã xử lý vào thư mục processed
"""

import pandas as pd
import numpy as np
import logging
import glob
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Import cấu hình từ module config
import config

# Thiết lập logger
logger = logging.getLogger(__name__)

class F1Processor:
    """
    Class xử lý dữ liệu F1 từ raw đến processed
    """
    
    def __init__(self, use_spark: bool = False):
        """
        Khởi tạo processor
        
        Args:
            use_spark: Sử dụng Spark để xử lý dữ liệu lớn
        """
        self.use_spark = use_spark
        self.spark = None
        
        if use_spark:
            self._init_spark()
            
        logger.info(f"F1Processor initialized with Spark: {use_spark}")
    
    def _init_spark(self):
        """Khởi tạo Spark session"""
        try:
            self.spark = (
                SparkSession.builder
                .appName("F1DataProcessor")
                .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .config("spark.sql.session.timeZone", "UTC")
            )
            
            # Thêm các cấu hình từ config
            for key, value in config.SPARK_CONFIG.items():
                self.spark = self.spark.config(key, value)
                
            self.spark = self.spark.getOrCreate()
            logger.info("Spark session initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Spark: {str(e)}")
            self.use_spark = False
    
    def _get_raw_files(self, year: int, data_type: str = None) -> List[Path]:
        """
        Lấy danh sách file raw cho một năm và loại dữ liệu
        
        Args:
            year: Năm cần lấy dữ liệu
            data_type: Loại dữ liệu (laps, drivers, results), None để lấy tất cả
            
        Returns:
            Danh sách đường dẫn file
        """
        year_dir = config.get_path_for_season(year, create=False)
        
        if not year_dir.exists():
            logger.warning(f"No data directory found for {year}")
            return []
            
        pattern = f"*_{data_type}.parquet" if data_type else "*.parquet"
        files = list(year_dir.glob(pattern))
        
        logger.info(f"Found {len(files)} {data_type or 'all'} files for {year}")
        return files
    
    def _read_parquet_pandas(self, file_path: Path) -> Optional[pd.DataFrame]:
        """
        Đọc file Parquet bằng Pandas
        
        Args:
            file_path: Đường dẫn file
            
        Returns:
            DataFrame hoặc None nếu có lỗi
        """
        try:
            df = pd.read_parquet(file_path)
            logger.debug(f"Read {len(df)} rows from {file_path}")
            return df
        except Exception as e:
            logger.error(f"Error reading {file_path}: {str(e)}")
            return None
    
    def _read_parquet_spark(self, file_path: Path) -> Optional[SparkDataFrame]:
        """
        Đọc file Parquet bằng Spark
        
        Args:
            file_path: Đường dẫn file
            
        Returns:
            Spark DataFrame hoặc None nếu có lỗi
        """
        if not self.spark:
            logger.error("Spark session not initialized")
            return None
            
        try:
            df = self.spark.read.parquet(str(file_path))
            logger.debug(f"Read Spark DataFrame from {file_path}")
            return df
        except Exception as e:
            logger.error(f"Error reading {file_path} with Spark: {str(e)}")
            return None
    
    def clean_lap_data(self, df: Union[pd.DataFrame, SparkDataFrame]) -> Union[pd.DataFrame, SparkDataFrame]:
        """
        Làm sạch dữ liệu lap
        
        Args:
            df: DataFrame chứa dữ liệu lap
            
        Returns:
            DataFrame đã làm sạch
        """
        if self.use_spark and isinstance(df, SparkDataFrame):
            # Xử lý với Spark
            # Loại bỏ các lap không hợp lệ
            df = df.filter(F.col("laptime").isNotNull())
            
            # Chuyển đổi kiểu dữ liệu
            df = df.withColumn("laptime", F.col("laptime").cast("double"))
            df = df.withColumn("sector1time", F.col("sector1time").cast("double"))
            df = df.withColumn("sector2time", F.col("sector2time").cast("double"))
            df = df.withColumn("sector3time", F.col("sector3time").cast("double"))
            
            # Tính toán thêm các trường
            df = df.withColumn("is_valid_lap", 
                              (F.col("laptime") > 0) & 
                              (F.col("laptime") < 300))  # Lap hợp lệ < 5 phút
            
            return df
        else:
            # Xử lý với Pandas
            # Tạo bản sao để tránh warning
            df = df.copy()
            
            # Loại bỏ các lap không hợp lệ
            df = df.dropna(subset=['laptime'])
            
            # Chuyển đổi kiểu dữ liệu
            numeric_cols = ['laptime', 'sector1time', 'sector2time', 'sector3time']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Tính toán thêm các trường
            df['is_valid_lap'] = (df['laptime'] > 0) & (df['laptime'] < 300)  # Lap hợp lệ < 5 phút
            
            return df
    
    def clean_driver_data(self, df: Union[pd.DataFrame, SparkDataFrame]) -> Union[pd.DataFrame, SparkDataFrame]:
        """
        Làm sạch dữ liệu driver
        
        Args:
            df: DataFrame chứa dữ liệu driver
            
        Returns:
            DataFrame đã làm sạch
        """
        if self.use_spark and isinstance(df, SparkDataFrame):
            # Xử lý với Spark
            # Loại bỏ các bản ghi trùng lặp
            df = df.dropDuplicates(['driver_number', 'year', 'event_name'])
            
            # Chuẩn hóa tên team
            df = df.withColumn("team_name", F.trim(F.col("team_name")))
            
            return df
        else:
            # Xử lý với Pandas
            df = df.copy()
            
            # Loại bỏ các bản ghi trùng lặp
            df = df.drop_duplicates(subset=['driver_number', 'year', 'event_name'])
            
            # Chuẩn hóa tên team
            if 'team_name' in df.columns:
                df['team_name'] = df['team_name'].str.strip()
            
            return df
    
    def merge_lap_with_driver(self, lap_df: Union[pd.DataFrame, SparkDataFrame], 
                             driver_df: Union[pd.DataFrame, SparkDataFrame]) -> Union[pd.DataFrame, SparkDataFrame]:
        """
        Kết hợp dữ liệu lap với dữ liệu driver
        
        Args:
            lap_df: DataFrame chứa dữ liệu lap
            driver_df: DataFrame chứa dữ liệu driver
            
        Returns:
            DataFrame đã kết hợp
        """
        if self.use_spark and isinstance(lap_df, SparkDataFrame) and isinstance(driver_df, SparkDataFrame):
            # Xử lý với Spark
            # Join dữ liệu
            merged_df = lap_df.join(
                driver_df,
                on=['driver_number', 'year', 'event_name'],
                how='left'
            )
            
            return merged_df
        else:
            # Xử lý với Pandas
            # Join dữ liệu
            merged_df = pd.merge(
                lap_df,
                driver_df,
                on=['driver_number', 'year', 'event_name'],
                how='left'
            )
            
            return merged_df
    
    def save_processed_data(self, df: Union[pd.DataFrame, SparkDataFrame], 
                           year: int, data_type: str) -> Path:
        """
        Lưu dữ liệu đã xử lý
        
        Args:
            df: DataFrame cần lưu
            year: Năm của dữ liệu
            data_type: Loại dữ liệu
            
        Returns:
            Đường dẫn đến file đã lưu
        """
        # Tạo thư mục cho năm
        year_dir = config.PROCESSED_DATA_DIR / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        
        # Tạo đường dẫn file
        file_path = year_dir / f"{data_type}.parquet"
        
        if self.use_spark and isinstance(df, SparkDataFrame):
            # Lưu với Spark
            df.write.mode("overwrite").parquet(str(file_path))
        else:
            # Lưu với Pandas
            df.to_parquet(file_path, index=False)
            
        logger.info(f"Saved processed {data_type} data to {file_path}")
        return file_path
    
    def process_season_laps(self, year: int) -> Optional[Path]:
        """
        Xử lý dữ liệu lap cho một mùa giải
        
        Args:
            year: Năm cần xử lý
            
        Returns:
            Đường dẫn đến file đã lưu hoặc None nếu có lỗi
        """
        logger.info(f"Processing lap data for {year}")
        
        # Lấy danh sách file lap
        lap_files = self._get_raw_files(year, "laps")
        if not lap_files:
            logger.warning(f"No lap data found for {year}")
            return None
            
        # Lấy danh sách file driver
        driver_files = self._get_raw_files(year, "drivers")
        if not driver_files:
            logger.warning(f"No driver data found for {year}")
        
        # Đọc và kết hợp dữ liệu
        if self.use_spark:
            # Xử lý với Spark
            lap_dfs = [self._read_parquet_spark(file) for file in lap_files]
            lap_dfs = [df for df in lap_dfs if df is not None]
            
            if not lap_dfs:
                logger.warning(f"Failed to read any lap data for {year}")
                return None
                
            # Union tất cả DataFrame
            lap_data = lap_dfs[0]
            for df in lap_dfs[1:]:
                lap_data = lap_data.union(df)
                
            # Làm sạch dữ liệu
            lap_data = self.clean_lap_data(lap_data)
            
            # Xử lý dữ liệu driver nếu có
            if driver_files:
                driver_dfs = [self._read_parquet_spark(file) for file in driver_files]
                driver_dfs = [df for df in driver_dfs if df is not None]
                
                if driver_dfs:
                    driver_data = driver_dfs[0]
                    for df in driver_dfs[1:]:
                        driver_data = driver_data.union(df)
                        
                    driver_data = self.clean_driver_data(driver_data)
                    
                    # Kết hợp dữ liệu
                    lap_data = self.merge_lap_with_driver(lap_data, driver_data)
            
            # Lưu dữ liệu đã xử lý
            return self.save_processed_data(lap_data, year, "laps")
            
        else:
            # Xử lý với Pandas
            lap_dfs = [self._read_parquet_pandas(file) for file in lap_files]
            lap_dfs = [df for df in lap_dfs if df is not None]
            
            if not lap_dfs:
                logger.warning(f"Failed to read any lap data for {year}")
                return None
                
            # Concat tất cả DataFrame
            lap_data = pd.concat(lap_dfs, ignore_index=True)
            
            # Làm sạch dữ liệu
            lap_data = self.clean_lap_data(lap_data)
            
            # Xử lý dữ liệu driver nếu có
            if driver_files:
                driver_dfs = [self._read_parquet_pandas(file) for file in driver_files]
                driver_dfs = [df for df in driver_dfs if df is not None]
                
                if driver_dfs:
                    driver_data = pd.concat(driver_dfs, ignore_index=True)
                    driver_data = self.clean_driver_data(driver_data)
                    
                    # Kết hợp dữ liệu
                    lap_data = self.merge_lap_with_driver(lap_data, driver_data)
            
            # Lưu dữ liệu đã xử lý
            return self.save_processed_data(lap_data, year, "laps")
    
    def process_all_seasons(self, years: List[int] = None) -> Dict[int, Dict[str, Path]]:
        """
        Xử lý dữ liệu cho nhiều mùa giải
        
        Args:
            years: Danh sách năm cần xử lý, None để xử lý tất cả các năm có dữ liệu
            
        Returns:
            Dict chứa kết quả xử lý cho từng năm
        """
        if years is None:
            # Lấy danh sách năm từ thư mục raw
            years = [int(d.name) for d in config.RAW_DATA_DIR.iterdir() if d.is_dir() and d.name.isdigit()]
            
        logger.info(f"Processing data for years: {years}")
        
        results = {}
        
        for year in years:
            year_results = {}
            
            # Xử lý dữ liệu lap
            lap_path = self.process_season_laps(year)
            if lap_path:
                year_results['laps'] = lap_path
                
            # TODO: Xử lý các loại dữ liệu khác (results, telemetry, ...)
            
            results[year] = year_results
            
        return results
    
    def close(self):
        """Đóng Spark session nếu có"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


# Hàm tiện ích để sử dụng từ CLI
def process_data_for_years(years: List[int] = None, use_spark: bool = False) -> Dict[int, Dict[str, Path]]:
    """
    Hàm tiện ích để xử lý dữ liệu cho nhiều năm
    
    Args:
        years: Danh sách năm cần xử lý, None để xử lý tất cả các năm có dữ liệu
        use_spark: Sử dụng Spark để xử lý dữ liệu lớn
        
    Returns:
        Dict chứa kết quả xử lý
    """
    processor = F1Processor(use_spark=use_spark)
    try:
        return processor.process_all_seasons(years)
    finally:
        processor.close()


if __name__ == "__main__":
    # Ví dụ sử dụng từ command line
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(description='Process F1 data')
    parser.add_argument('--years', type=int, nargs='+', help='Years to process')
    parser.add_argument('--spark', action='store_true', help='Use Spark for processing')
    
    args = parser.parse_args()
    
    process_data_for_years(args.years, args.spark)
