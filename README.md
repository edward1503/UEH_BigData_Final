# UEH Big Data Final Project - An Application of Big Data in Analyzing and Forecasting Formula 1 Results
![F1 Racing]([https://images.unsplash.com/photo-1541773367336-d3f7e6a32dad?ixlib=rb-1.2.1&auto=format&fit=](https://www.google.com/search?q=f1&sa=X&sca_esv=6f4cffb8fcc6cffb&sxsrf=AHTn8zq8Oc9BaOVo1I00jntE1311eKBywQ:1746542102558&udm=2&fbs=ABzOT_AfCikcO6SgGMxZXxAG9tmSag6i5UQdILGiivX0qZVqd_ZbB6r-iRVBllSRxsxwTrLqWtRgtrWpIjk-C3nT-vLbcL_PLgy05W_5pJNt2vZFIOe0XSH88l9SafhMoOXXCfjjEMXN5LN8P_cfRpGIN6TD7fECf19YkrozjGjoHM-KS_QSUvf05LbRxIbjsoNfRoz1fQ9IkTal_HPcc6oy-OWiyZ8UIwqQhfOfNkE8m_7oEHgSQXI&ved=2ahUKEwjArO6siI-NAxWahlYBHRJQOf4QtKgLegQIBxAH&biw=1450&bih=689&dpr=1.33#vhid=4mCq_kK9fbJ2kM&vssid=mosaic))
A comprehensive data pipeline for collecting, processing, and analyzing Formula 1 racing data to predict race outcomes using machine learning. 

# Overview
This project implements an end-to-end data pipeline for Formula 1 racing analytics. It collects historical race data from the FastF1 API, processes and transforms the data using Apache Spark, and builds machine learning models to predict race outcomes. The pipeline is designed to be scalable, maintainable, and reproducible.

## Main Features
- Access to F1 Telementry data via FastF1 API.
- Perform Data Cleaning and validation.
- Predict the race results.

## Pipeline
- The pipeline follows a modern data engineering architecture:

- Data Collection: Raw data extraction from [FastF1 API](https://docs.fastf1.dev/).

- Data Processing: Transformation using Apache Spark.

- Feature Engineering: Creating predictive features from raw data.

- Model Training: Building and evaluating ML models.

- Prediction: Forecasting race outcomes.
