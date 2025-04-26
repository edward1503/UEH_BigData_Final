"""
Feature Engineering Pipeline for F1 Data - Optimized for Spark
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Optional
from src.config import settings
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FeatureEngineer:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.raw_dir = settings.RAW_DATA_DIR
        self.interim_dir = settings.INTERIM_DIR
    
    def load_season_data(self, year: int) -> Optional[DataFrame]:
        """Load raw data cho 1 mùa giải từ thư mục đã được partition"""
        try:
            path = f"{self.raw_dir}/year={year}/*.parquet"
            df = self.spark.read.parquet(path)
            
            # Validate schema
            required_columns = {'driver_id', 'lap_time', 'circuit_id', 'position', 'compound'}
            if not required_columns.issubset(set(df.columns)):
                raise ValueError(f"Missing columns in {year} data")
                
            logger.info(f"Successfully loaded {df.count()} records for {year}")
            return df
        except Exception as e:
            logger.error(f"Failed to load {year} data: {str(e)}")
            return None

    def calculate_driver_stats(self, df: DataFrame) -> DataFrame:
        """Tính toán metrics hiệu suất cho từng tay đua"""
        driver_window = Window.partitionBy('driver_id').orderBy('race_date')
        
        return df.groupBy('driver_id').agg(
            F.avg('lap_time').alias('avg_lap_time'),
            F.stddev('lap_time').alias('lap_time_std'),
            F.count(F.when(F.col('position') == 1, True)).alias('wins'),
            F.sum('points').alias('total_points'),
            F.first('team').alias('current_team'),
            # Tính rolling avg 5 race gần nhất
            F.avg('lap_time').over(driver_window.rowsBetween(-4, 0)).alias('rolling_5race_avg')
        )

    def calculate_circuit_stats(self, df: DataFrame) -> DataFrame:
        """Tính toán đặc điểm từng đường đua"""
        return df.groupBy('circuit_id').agg(
            F.avg('lap_time').alias('circuit_avg_lap'),
            F.countDistinct('driver_id').alias('total_drivers'),
            F.stddev('lap_time').alias('circuit_lap_std')
        )

    def merge_datasets(self, driver_df: DataFrame, circuit_df: DataFrame) -> DataFrame:
        """Kết hợp driver stats và circuit stats"""
        return driver_df.join(
            circuit_df,
            on='circuit_id',
            how='left'
        ).withColumnRenamed('circuit_avg_lap', 'track_base_time')

    def save_features(self, df: DataFrame, year: int) -> None:
        """Lưu features theo định dạng Spark partition"""
        output_path = f"{self.interim_dir}/year={year}"
        df.write.mode('overwrite').parquet(output_path)
        logger.info(f"Saved {df.count()} features to {output_path}")

    def build_features(self, years: List[int]) -> None:
        """End-to-end feature engineering pipeline"""
        for year in years:
            if raw_df := self.load_season_data(year):
                driver_stats = self.calculate_driver_stats(raw_df)
                circuit_stats = self.calculate_circuit_stats(raw_df)
                final_df = self.merge_datasets(driver_stats, circuit_stats)
                self.save_features(final_df, year)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("F1FeatureEngineering") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    # Khởi tạo và chạy pipeline
    fe = FeatureEngineer(spark)
    
    # Ví dụ: Xử lý dữ liệu từ 2021-2023
    fe.build_features([2021, 2022, 2023])
    
    spark.stop()
