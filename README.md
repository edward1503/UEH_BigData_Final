# UEH Big Data Final Project - An Application of Big Data in Analyzing and Forecasting Formula 1 Results
<h1 align="center">
<img src="UEH_BigData_Final/picture/f1 rmmd.jpg" width="600">
</h1><br>

A comprehensive data pipeline for collecting, processing, and analyzing Formula 1 racing data to predict race outcomes using machine learning and big data technology. 

# Overview
This project implements an end-to-end data pipeline for Formula 1 racing analytics. It collects historical race data from the FastF1 API, processes and transforms the data using Apache Spark, and builds machine learning models to predict race outcomes. The pipeline is designed to be scalable, maintainable, and reproducible.

## Prerequisites
- Python 3.9+

- Apache Spark 3.3.4+

- FastF1 Python package

- AWS account (optional, for cloud deployment)

- Docker (optional, for containerized execution)

## Main Features
Data Collection
Comprehensive Race Data: Collects lap times, sector times, driver information, and race results

Weather Information: Captures weather conditions (temperature, humidity, rainfall) for each race

Telemetry Data: Gathers speed, RPM, and other telemetry metrics where available

Automatic Retry Logic: Implements robust error handling with configurable retry attempts

Efficient Caching: Uses FastF1's caching system to minimize API calls

Data Processing
Spark-Based Processing: Leverages Apache Spark for distributed data processing

Time Conversion: Automatically converts time data to milliseconds for consistent analysis

Standardized Schema: Normalizes column names and data types across different data sources

Missing Data Handling: Implements strategies for handling missing or inconsistent data

Feature Engineering
Pit Stop Analysis: Calculates pit stop counts, durations, and strategies

Overtaking Metrics: Tracks position changes to quantify driver overtaking abilities

Tire Performance: Analyzes tire degradation and compound performance

Weather Impact: Quantifies the effect of weather conditions on race performance

Driver Consistency: Measures lap time consistency and sector performance

## Pipeline
<h2 align="center">
<img src="UEH_BigData_Final/picture/Pipeline.png" width="800">
</h2><br>
- The pipeline follows a modern data engineering architecture:

- Data Collection: Raw data extraction from [FastF1 API](https://docs.fastf1.dev/).

- Data Processing: Transformation using Apache Spark.

- Feature Engineering: Creating predictive features from raw data.

- Model Training: Building and evaluating ML models.

- Prediction: Forecasting race outcomes.

