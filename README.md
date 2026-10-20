# Large-Scale Agricultural Market Data Processing and Real-Time Market Analysis

This repository contains the complete implementation of a big data analytics and real-time anomaly detection system built entirely from scratch. The system ingests 132.9M real agricultural records, cleans them, engineers features, performs large-scale price analysis, predicts anomalies using machine learning, and visualizes results in a real-time streaming dashboard.

## Complete Architecture

```mermaid
graph TD
    A1[Daily Market Prices] --> B(Spark Processing)
    A2[India Mandi CSVs] --> B
    B --> C[Canonical Dataset in HDFS]
    C --> D(Spark Feature Engineering)
    D --> E[Engineered Features in HDFS]
    
    E --> F(Spark MLlib Training)
    F --> G[Random Forest Model in HDFS]
    F --> H[Price Analytics in HDFS]
    
    I[Kafka Topic: agri-market-transactions] --> J(Spark Structured Streaming)
    G --> J
    J --> K[Streaming Predictions in HDFS]
    J -.-> L(Checkpointing)
    K --> M[Streamlit Dashboard]
```

## Scale and Data Volume

The project strictly processes real agricultural data:
- **Real source records:** 132.9M
- **Canonical dataset:** 93.41M records
- **Real raw data volume:** ≈ 10.9 GB
- **Synthetic Scalability Workload:** ≈ 7.1 GB (Used exclusively for isolated scalability testing)
- **Total Experimental Workload:** ≈ 18 GB

##  Price Analytics & Forecasting
The system provides comprehensive historical price analysis utilizing Spark DataFrames and Window functions. 
*(Note: Forecasting is strictly out-of-scope for the live dashboard unless actively validated models are deployed).*

## Final ML Results

Model Selection was performed using a deterministic 1% entity-safe representative sample with price deviation features, evaluated on an untouched 2025+ temporal test set (48,354 records).

**Model Details:**
- **Model:** Spark MLlib Random Forest Classifier
- **Number of Trees:** 100
- **Max Depth:** 14
- **Max Bins:** 128
- **Decision Threshold:** 0.35 (Calibrated for optimal F1 & high precision)
- **Seed:** 42L

**Test Results (Untouched 2025+ Test Set):**
- **Accuracy:** **95.66%**
- **Precision:** **86.55%**
- **Recall:** **85.47%**
- **F1 Score:** **86.01%**
- **False Positive Rate (FPR):** **2.46%** (Ultra-low false alarms)
- **ROC-AUC:** **94.82%**
- **PR-AUC:** **86.15%**

**Confusion Matrix (48,354 Unseen Test Records):**
- **True Negatives (TN):** 39,809
- **False Positives (FP):** 1,002
- **False Negatives (FN):** 1,096
- **True Positives (TP):** 6,447


## Distributed Storage (HDFS)

All data is reliably persisted in HDFS (1 NameNode + 3 DataNodes) with Replication Factor = 3:
- **Canonical Dataset:** `/agri/processed/canonical`
- **Engineered Features:** `/agri/features/engineered`
- **Trained Model:** `/agri/models/anomaly_prediction_rf`
- **Batch Analytics:** `/agri-market/analytics/`
- **Streaming Predictions Sink:** `/agri/streaming/real_anomaly_predictions`
- **Streaming Checkpoints:** `/agri/checkpoints/real_market_stream`

## Real-Time Streaming Infrastructure

Kafka acts as the event transport mechanism (not a database).
- **Topic:** `agri-market-real-replay` (for real data replay)
- **Host Producer:** `localhost:9092`
- **Spark Structured Streaming Consumer:** `host.docker.internal:29092`

##  Streamlit Dashboard

A lightweight, real-time PySpark + Streamlit dashboard reads directly from the streaming sink and batch analytics paths.
- **Title:** Large-Scale Agricultural Market Data & Real-Time Analysis
- **Startup Command (Using Docker):**
  ```bash
  docker build -f Dockerfile.dashboard -t agri-dashboard .
  docker run -d --name agri-dashboard --network docker_default -p 8501:8501 agri-dashboard
  ```
