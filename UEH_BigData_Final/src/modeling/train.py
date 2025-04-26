"""
Model Training Pipeline - Production Grade
"""
import sys
import os
from pyspark.sql import DataFrame
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from src.config import settings
from data.utils import DataUtils
import mlflow
import logging
from typing import Dict, Any

logging.basicConfig(level=settings.LOG_LEVEL)
logger = logging.getLogger(__name__)

class ModelTrainer:
    def __init__(self, spark):
        self.spark = spark
        self.experiment_name = "f1_prediction_v1"
        self.model_path = str(settings.MODEL_DIR / "rf_model")
        
    def _load_features(self, years: list[int]) -> DataFrame:
        """Tải features từ nhiều mùa giải"""
        dfs = []
        for year in years:
            path = settings.INTERIM_DIR / f"year={year}"
            if df := DataUtils.read_parquet(self.spark, str(path)):
                dfs.append(df)
        return dfs[0] if len(dfs) == 1 else dfs[0].unionByName(*dfs[1:])

    def _build_pipeline(self) -> Pipeline:
        """Tạo ML pipeline với cấu hình động"""
        indexer = StringIndexer(
            inputCol="driver_id",
            outputCol="driver_index",
            handleInvalid="keep"
        )
        
        assembler = VectorAssembler(
            inputCols=settings.MODEL_CONFIG["features"],
            outputCol="feature_vector"
        )
        
        rf = RandomForestRegressor(
            featuresCol="feature_vector",
            labelCol=settings.MODEL_CONFIG["target"],
            numTrees=settings.MODEL_CONFIG["num_trees"],
            maxDepth=settings.MODEL_CONFIG["max_depth"],
            seed=settings.SEED
        )
        
        return Pipeline(stages=[indexer, assembler, rf])

    def _log_mlflow_metrics(self, model, test_df) -> Dict[str, float]:
        """Ghi nhận metrics và artifacts lên MLflow"""
        predictions = model.transform(test_df)
        
        metrics = {}
        for metric in ["rmse", "mae", "r2"]:
            evaluator = RegressionEvaluator(
                labelCol=settings.MODEL_CONFIG["target"],
                predictionCol="prediction",
                metricName=metric
            )
            metrics[metric] = evaluator.evaluate(predictions)
            
        mlflow.log_metrics(metrics)
        mlflow.spark.log_model(model, "model")
        return metrics

    def train(self, train_years: list[int], test_years: list[int]) -> Dict[str, Any]:
        """Workflow huấn luyện end-to-end"""
        mlflow.set_tracking_uri(settings.MLFLOW_URI)
        mlflow.set_experiment(self.experiment_name)
        
        with mlflow.start_run():
            try:
                # Data preparation
                train_df = self._load_features(train_years)
                test_df = self._load_features(test_years)
                
                if train_df.isEmpty() or test_df.isEmpty():
                    raise ValueError("Empty training/test data")
                
                # Pipeline construction
                pipeline = self._build_pipeline()
                
                # Model training
                model = pipeline.fit(train_df)
                
                # Evaluation
                metrics = self._log_mlflow_metrics(model, test_df)
                
                # Model saving
                model.write().overwrite().save(self.model_path)
                logger.info(f"Model saved to {self.model_path}")
                
                return {
                    "status": "success",
                    "metrics": metrics,
                    "model_path": self.model_path
                }
                
            except Exception as e:
                logger.error(f"Training failed: {str(e)}", exc_info=True)
                mlflow.log_param("status", "failed")
                return {
                    "status": "error",
                    "error": str(e)
                }
