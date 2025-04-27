"""
F1 Data Processor Module - Feature Selection & Engineering

Module này chịu trách nhiệm:
1. Đọc dữ liệu thô từ thư mục RAW
2. Lựa chọn và xây dựng các đặc trưng quan trọng
3. Lưu dữ liệu đã xử lý vào thư mục PROCESSED
"""

import pandas as pd
import numpy as np
import logging
import glob
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, BooleanType

# Import cấu hình từ module config
import config

# Thiết lập logger
logger = logging.getLogger(__name__)

class F1Processor:
    """
    Class xử lý dữ liệu F1 từ raw đến processed
    """
    
    def __init__(self, use_spark: bool = True):
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
                .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                .config("spark.sql.session.timeZone", "UTC")
                .config("spark.sql.legacy.parquet.nanosAsLong", "true")  # Thêm cấu hình này để xử lý timestamp
            )
            
            # Thêm các cấu hình từ config
            for key, value in config.SPARK_CONFIG.items():
                self.spark = self.spark.config(key, value)
                
            self.spark = self.spark.getOrCreate()
            logger.info("Spark session initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Spark: {str(e)}")
            self.use_spark = False
    
    def _get_raw_files(self, year: int, data_type: str = "r_laps") -> List[Path]:
        """
        Lấy danh sách file raw cho một năm và loại dữ liệu
        
        Args:
            year: Năm cần lấy dữ liệu
            data_type: Loại dữ liệu (r_laps, r_drivers, r_results)
            
        Returns:
            Danh sách đường dẫn file
        """
        year_dir = config.RAW_DATA_DIR / str(year)
        
        if not year_dir.exists():
            logger.warning(f"No data directory found for {year}")
            return []
            
        pattern = f"*_{data_type}.parquet"
        files = list(year_dir.glob(pattern))
        
        logger.info(f"Found {len(files)} {data_type} files for {year}")
        return files
    
    def _read_parquet_spark(self, file_paths: List[Path]) -> Optional[SparkDataFrame]:
        """
        Đọc nhiều file Parquet bằng Spark
        
        Args:
            file_paths: Danh sách đường dẫn file
            
        Returns:
            Spark DataFrame hoặc None nếu có lỗi
        """
        if not self.spark:
            logger.error("Spark session not initialized")
            return None
            
        if not file_paths:
            logger.warning("No files to read")
            return None
            
        try:
            # Chuyển đổi Path thành string
            str_paths = [str(path) for path in file_paths]
            
            # Đọc tất cả các file
            df = self.spark.read.parquet(*str_paths)
            logger.info(f"Read {df.count()} rows from {len(file_paths)} files")
            
            # In schema để debug
            logger.debug("DataFrame schema:")
            df.printSchema()
            
            return df
        except Exception as e:
            logger.error(f"Error reading Parquet files: {str(e)}")
            return None
    
    def select_features(self, df: SparkDataFrame) -> SparkDataFrame:
        """
        Chọn và xây dựng các đặc trưng quan trọng
        
        Args:
            df: DataFrame chứa dữ liệu lap
            
        Returns:
            DataFrame với các đặc trưng đã chọn
        """
        # In schema để debug
        logger.info("Original DataFrame columns:")
        logger.info(str(df.columns))
        
        # Chuẩn hóa tên cột thành lowercase
        renamed_df = df
        for col in df.columns:
            renamed_df = renamed_df.withColumnRenamed(col, col.lower())
        
        # In schema sau khi chuẩn hóa
        logger.info("Renamed DataFrame columns:")
        logger.info(str(renamed_df.columns))
        
        # Chọn các cột cơ bản
        selected_cols = [
            # Thông tin cơ bản về tay đua và vòng đua
            "driver", "drivernumber", "team", "lapnumber", "position",
            
            # Thông tin thời gian (milliseconds)
            "laptime", "sector1time", "sector2time", "sector3time", 
            "pitintime", "pitouttime",
            
            # Thông tin về lốp xe
            "compound", "tyrelife", "stint", "freshtyre",
            
            # Thông tin về tốc độ
            "speedi1", "speedi2", "speedfl", "speedst",
            
            # Thông tin bổ sung
            "trackstatus", "ispersonalbest", "year", "event_name", "track_name"
        ]
        
        # Lọc các cột tồn tại trong DataFrame
        existing_cols = [col for col in selected_cols if col in renamed_df.columns]
        
        # Chọn các cột cơ bản
        result_df = renamed_df.select(existing_cols)
        
        # Tính các đặc trưng bổ sung
        
        # 1. Chuyển đổi thời gian từ seconds sang milliseconds (nếu cần)
        time_cols = ["laptime", "sector1time", "sector2time", "sector3time", 
                    "pitintime", "pitouttime"]
        for col in time_cols:
            if col in existing_cols:
                result_df = result_df.withColumn(col, F.col(col) * 1000)
        
        # 2. Tính tỷ lệ sector
        sector_cols = ["sector1time", "sector2time", "sector3time"]
        for sector_col in sector_cols:
            if sector_col in existing_cols and "laptime" in existing_cols:
                ratio_col = f"{sector_col}_ratio"
                result_df = result_df.withColumn(
                    ratio_col, 
                    F.when(F.col("laptime") > 0, F.col(sector_col) / F.col("laptime"))
                    .otherwise(None)
                )
        
        # 3. Tính số lần pit stop
        if "pitintime" in existing_cols:
            result_df = result_df.withColumn(
                "pit_stop_count", 
                F.when(F.col("pitintime").isNotNull(), 1)
                .otherwise(0)
            )
        
        # 4. Tính delta với vòng trước và vòng tốt nhất
        if "drivernumber" in existing_cols and "lapnumber" in existing_cols and "laptime" in existing_cols:
            window_spec = Window.partitionBy("drivernumber").orderBy("lapnumber")
            
            result_df = result_df.withColumn(
                "prev_laptime", 
                F.lag("laptime", 1).over(window_spec)
            )
            
            result_df = result_df.withColumn(
                "delta_previous", 
                F.col("laptime") - F.col("prev_laptime")
            )
            
            # 5. Tính delta với vòng tốt nhất
            window_spec_min = Window.partitionBy("drivernumber")
            
            result_df = result_df.withColumn(
                "best_laptime", 
                F.min("laptime").over(window_spec_min)
            )
            
            result_df = result_df.withColumn(
                "delta_optimal", 
                F.col("laptime") - F.col("best_laptime")
            )
        
        # 6. Tính tyre degradation (đơn giản)
        if "tyrelife" in existing_cols and "laptime" in existing_cols:
            result_df = result_df.withColumn(
                "tyre_degradation", 
                F.when(F.col("tyrelife") > 0, 
                      (F.col("laptime") - F.col("best_laptime")) / F.col("tyrelife"))
                .otherwise(0)
            )
        
        # 7. Tính khoảng cách với xe trước/sau
        if "lapnumber" in existing_cols and "position" in existing_cols and "laptime" in existing_cols:
            window_spec_pos = Window.partitionBy("lapnumber").orderBy("position")
            
            result_df = result_df.withColumn(
                "gap_ahead", 
                F.lead("laptime", 1).over(window_spec_pos) - F.col("laptime")
            )
            
            result_df = result_df.withColumn(
                "gap_behind", 
                F.col("laptime") - F.lag("laptime", 1).over(window_spec_pos)
            )
        
        # Loại bỏ các cột tạm thời
        if "prev_laptime" in result_df.columns and "best_laptime" in result_df.columns:
            result_df = result_df.drop("prev_laptime", "best_laptime")
        
        return result_df
    
    def process_race_data(self, year: int) -> Optional[Path]:
        """
        Xử lý dữ liệu đua cho một năm
        
        Args:
            year: Năm cần xử lý
            
        Returns:
            Đường dẫn đến file đã lưu hoặc None nếu có lỗi
        """
        logger.info(f"Processing race data for {year}")
        
        # Lấy danh sách file lap
        lap_files = self._get_raw_files(year, "r_laps")
        if not lap_files:
            logger.warning(f"No lap data found for {year}")
            return None
        
        # Đọc dữ liệu lap
        lap_df = self._read_parquet_spark(lap_files)
        if lap_df is None:
            logger.warning(f"Failed to read lap data for {year}")
            return None
        
        # Chọn và xây dựng đặc trưng
        processed_df = self.select_features(lap_df)
        
        # Tạo thư mục cho năm
        year_dir = config.PROCESSED_DATA_DIR / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        
        # Tạo đường dẫn file
        output_path = year_dir / "race_features.parquet"
        
        # Lưu file
        processed_df.write.mode("overwrite").parquet(str(output_path))
        
        logger.info(f"Processed race data saved to {output_path}")
        return output_path
    
    def process_all_years(self, years: List[int] = None) -> Dict[int, Path]:
        """
        Xử lý dữ liệu cho nhiều năm
        
        Args:
            years: Danh sách năm cần xử lý, None để xử lý tất cả các năm có dữ liệu
            
        Returns:
            Dict chứa kết quả xử lý cho từng năm
        """
        if years is None:
            # Lấy danh sách năm từ thư mục raw
            years = [int(d.name) for d in config.RAW_DATA_DIR.iterdir() 
                    if d.is_dir() and d.name.isdigit()]
            
        logger.info(f"Processing data for years: {years}")
        
        results = {}
        
        for year in years:
            output_path = self.process_race_data(year)
            if output_path:
                results[year] = output_path
                
        return results
    
    def close(self):
        """Đóng Spark session nếu có"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


# Hàm tiện ích để sử dụng từ CLI
def process_data(years: List[int] = None) -> Dict[int, Path]:
    """
    Hàm tiện ích để xử lý dữ liệu cho nhiều năm
    
    Args:
        years: Danh sách năm cần xử lý, None để xử lý tất cả các năm có dữ liệu
        
    Returns:
        Dict chứa kết quả xử lý
    """
    processor = F1Processor(use_spark=True)
    try:
        return processor.process_all_years(years)
    finally:
        processor.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Process F1 data")
    parser.add_argument("--years", type=int, nargs="+", help="Years to process")
    
    args = parser.parse_args()
    
    process_data(args.years)
