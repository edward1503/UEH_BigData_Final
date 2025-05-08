# Big Data Project - An Application of Big Data in Analyzing and Forecasting Formula 1 Results
<h1 align="center">
<img src="UEH_BigData_Final/picture/f1 rmmd.jpg" width="600">
</h1><br>

A comprehensive data pipeline for collecting, processing, and analyzing Formula 1 racing data to predict race outcomes using machine learning and big data technology. 

## Contributor
Intructor: Dr. Nguyễn Mạnh Tuấn.
1. Nguyễn Đôn Đức.
2. Nguyễn Thành Vinh.
3. Đỗ Nhật Phương.
4. Bùi Tiến Hiếu.



# Overview
This project implements an end-to-end data pipeline for Formula 1 racing analytics. It collects historical race data from the FastF1 API, processes and transforms the data using Apache Spark, and builds machine learning models to predict race outcomes. The pipeline is designed to be scalable, maintainable, and reproducible.

## Prerequisites
- Python 3.9+

- Apache Spark 3.3.4+

- FastF1 Python package

- AWS account (optional, for cloud deployment)

- Docker (optional, for containerized execution)

## Main Features
### 1. Data Collection
- Comprehensive Race Data: Collects lap times, sector times, driver information, and race results

- Weather Information: Captures weather conditions (temperature, humidity, rainfall) for each race

- Telemetry Data: Gathers speed, RPM, and other telemetry metrics where available

- Automatic Retry Logic: Implements robust error handling with configurable retry attempts

- Efficient Caching: Uses FastF1's caching system to minimize API calls

### 2. Data Processing
- Spark-Based Processing: Leverages Apache Spark for distributed data processing

- Time Conversion: Automatically converts time data to milliseconds for consistent analysis

- Standardized Schema: Normalizes column names and data types across different data sources

- Missing Data Handling: Implements strategies for handling missing or inconsistent data

### 3. Feature Engineering
- Pit Stop Analysis: Calculates pit stop counts, durations, and strategies

- Overtaking Metrics: Tracks position changes to quantify driver overtaking abilities

- Tire Performance: Analyzes tire degradation and compound performance

- Weather Impact: Quantifies the effect of weather conditions on race performance

- Driver Consistency: Measures lap time consistency and sector performance

### 4. Model Training
- Multiple Algorithm Support: Random Forest, XGBoost, and neural networks

- Cross-Validation: Implements time-based cross-validation for racing predictions

- Hyperparameter Tuning: Automated optimization of model parameters

- Feature Importance: Identifies the most influential factors in race outcomes

## Pipeline
<h2 align="center">
<img src="UEH_BigData_Final/picture/Pipeline.png" width="800">
</h2><br>
The pipeline follows a modern data engineering architecture:

- Data Collection: Raw data extraction from [FastF1 API](https://docs.fastf1.dev/).

- Data Processing: Transformation using Apache Spark.

- Feature Engineering: Creating predictive features from raw data.

- Model Training: Building and evaluating ML models.

- Prediction: Forecasting race outcomes.

## Installation
### 1. Setup Environment
```commandline
# Clone the repository
git clone https://github.com/edward1503/UEH_BigData_Final
cd UEH_BigData_Final

# Install dependencies
pip install -r requirements.txt

# Set up Hadoop for Windows (if needed)
# Download winutils.exe for Hadoop 3.3.4 and place in C:\hadoop\bin
# Set environment variable HADOOP_HOME=C:\hadoop
```
### 2. Data Collection
```commandline
# Collect data for a specific year
python scripts/collect_data.py --year 2023

# Collect data for multiple years
python scripts/collect_data.py --years 2019 2020 2021 2022 2023
```

### 3. Feature Engineering
```commandline
This features in under development...
```
### 4. Predict the race outcomes
```commandline
This features in under development...
```

## Future Developement
- Implement real-time data processing for live race predictions

- Add telemetry data analysis for deeper insights

- Develop a web dashboard for interactive visualization

- Expand the model to include qualifying and practice session predictions
