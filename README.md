# CAN 2025: Incremental Streaming Analytics Pipeline

### **Predicting High-Risk Matches for Crowd Safety and Security**
**Author:** EL RHERBI MOHAMED AMINE  
**Event:** Student Challenge 2025 - Special Edition CAN (Morocco)

---

## 📋 Project Overview
This project implements an **Incremental Streaming Analytics Pipeline** designed for the **Africa Cup of Nations (CAN) 2025** in Morocco. The primary goal is to predict match risk levels in real-time by analyzing crowd density, ticketing velocity, and environmental factors.

By leveraging predictive management, organizers can proactively deploy security and medical resources, helping to prevent historical stadium incidents and ensuring a safe experience for fans.

---

![Project Architecture](architecture.png)

## 🏗️ Architecture: Medallion Design
The pipeline follows the **Medallion Architecture** to ensure data reliability and scalability using **Databricks**, **Spark Structured Streaming**, and **Delta Lake**.

### **1. Bronze Layer (Raw Data)**
Ingests raw data from both batch and simulated streaming sources.
* **Batch Data:** Static tournament info (Matches, Stadiums, and Historical Rivalry Scores).
* **Streaming Data:** Real-time simulations of ticketing percentages, crowd pressure sensors, and weather risks.

### **2. Silver Layer (Enriched Context)**
Unifies and cleans the data. It joins streaming events with batch metadata (stadium capacity, match phase) and calculates an **Importance Score** based on the tournament stage (e.g., Finals vs. Group Stage).

### **3. Gold Layer (Business & Prediction)**
* **Business Risk Score:** Computes a weighted risk index based on business logic.
* **ML Inference:** Uses a **Random Forest** model (managed via **MLflow**) to predict "High," "Medium," or "Low" risk levels for incoming real-time data.

---

## 🛠️ Technical Stack
* **Platform:** Databricks (Lakehouse Architecture)
* **Engine:** PySpark & Spark Structured Streaming
* **Storage:** Delta Lake & Unity Catalog
* **ML Lifecycle:** MLflow (Experiment tracking, Model Registry)
* **Orchestration:** Databricks Workflows

---

##  Project Structure
```text
CAN_RISK_PIPELINE/
├── 01_bronze/
│   ├── 01_generate_batch_data.py   # Populates Matches, Stadiums, and Rivalries (Batch)
│   └── generate_stream_data.py      # Simulates real-time crowd/weather sensors (Stream)
├── 02_silver/
│   └── build_silver_context.py     # Joins batch/stream and calculates importance scores
├── 03_gold/
│   ├── compute_gold_risk_score.py  # Manual business logic risk calculation
│   └── gold_realTime_inference.py  # Real-time ML model prediction and label assignment
├── create medallion.py             # Database and Schema initialization
└── model_training.py               # MLflow training pipeline for Random Forest Classifier
```

## Key Insights & Business Impact
Optimized Resource Management: Efficient deployment of security and medical teams based on predicted risk levels.

Enhanced Fan Experience: Reduction in crowd bottlenecks through data-driven flow management.

Cost Efficiency: The pipeline uses Incremental/Triggered Streaming, ensuring the cluster processes data only when needed, minimizing cloud costs.

Proactive Safety: Identifying high-risk scenarios before they escalate into incidents.