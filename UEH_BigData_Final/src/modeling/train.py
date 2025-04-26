"""
Model Training Module - Spark ML Pipeline for F1 Prediction
"""

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession, DataFrame
from typing import List, Union
from functools import reduce
from pathlib import Path
from src.config import settings
import logging
import mlflow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class F1ModelTrainer:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.features_dir = settings.FEATURES_DIR
        self.model_dir = settings.MODEL_DIR

    def _load_training_data(self, years: List[int]) -> Union[DataFrame, None]:
        """Load và combine features từ nhiều mùa giải"""
        dfs = []
        for year in years:
            try:
                path = f"{self.features_dir}/year={year}"
                df = self.spark.read.parquet(path)
                dfs.append(df)
                logger.info(f"Loaded {year} features with {df.count()} records")
            except Exception as e:
                logger.warning(f"Skipping {year}: {str(e)}")
        
        if not dfs:
            return None
    
    # Xử lý union an toàn cho Spark
        return dfs[0] if len(dfs) == 1 else reduce(lambda a, b: a.unionByName(b), dfs)

    def _build_pipeline(self) -> Pipeline:
        """Xây dựng ML Pipeline với Spark ML"""
        # Feature Engineering
        indexer = StringIndexer(
            inputCol="driver_id", 
            outputCol="driver_index",
            handleInvalid="keep"
        )
        
        assembler = VectorAssembler(
            inputCols=[
                "avg_lap_time",
                "total_points",
                "avg_tyre_life",
                "performance_ratio"
            ],
            outputCol="features"
        )
        
        # Model
        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol="total_points",
            numTrees=100,
            maxDepth=5,
            seed=42
        )
        
        return Pipeline(stages=[indexer, assembler, rf])

    def _log_metrics(self, model, test_df):
        """Ghi nhận metrics và log lên MLflow"""
        predictions = model.transform(test_df)
        
        evaluator = RegressionEvaluator(
            labelCol="total_points",
            predictionCol="prediction",
            metricName="rmse"
        )
        
        rmse = evaluator.evaluate(predictions)
        mlflow.log_metric("rmse", rmse)
        
        logger.info(f"Model RMSE: {rmse}")
        return rmse

    def train(self, train_years: list, test_years: list):
        """End-to-end training workflow"""
        with mlflow.start_run():
            # Bước 1: Load data
            train_df = self._load_training_data(train_years)
            test_df = self._load_training_data(test_years)
            
            # Bước 2: Build pipeline
            pipeline = self._build_pipeline()
            
            # Bước 3: Train model
            model = pipeline.fit(train_df)
            
            # Bước 4: Evaluate
            rmse = self._log_metrics(model, test_df)
            
            # Bước 5: Log model và params
            mlflow.spark.log_model(model, "model")
            mlflow.log_params({
                "train_years": train_years,
                "test_years": test_years,
                "num_trees": 100,
                "max_depth": 5
            })
            
            # Bước 6: Save model
            model.write().overwrite().save(f"{self.model_dir}/f1_rf_model")
            logger.info(f"Model saved to {self.model_dir}")
            
            return rmse

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("F1ModelTraining") \
        .config("spark.jars.packages", "org.mlflow:mlflow-spark:1.30.0") \
        .getOrCreate()
    
    # Cấu hình MLflow
    mlflow.set_tracking_uri(settings.MLFLOW_URI)
    mlflow.set_experiment("F1-Prediction")
    
    trainer = F1ModelTrainer(spark)
    
    # Train trên 2021-2023, test trên 2024
    rmse = trainer.train(
        train_years=[2021, 2022, 2023],
        test_years=[2024]
    )
    
    spark.stop()
