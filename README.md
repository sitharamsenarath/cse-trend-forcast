# Quantitative Equity Intelligence Pipeline (Tiered ML)

An end-to-end machine learning system designed to predict stock price movements in the **Sri Lankan (LKR) market**. The project moves beyond simple price charts by integrating macro-economic indicators into a tiered **Gradient Boosted Trees (GBT)** architecture.

## 🎯 The Goal
The primary objective is to generate actionable daily trade signals by forecasting next-day returns. The system is designed to:
* **Filter Technical Signals:** Validate price-action triggers (SMA, Volume) against the macro-economic environment (USD/LKR, Interest Rates).
* **Handle Production Latency:** Generate "Day-Zero" predictions for the current trading session even when the target label is not yet known.
* **Scalable Intelligence:** Utilize a distributed computing framework (Spark) to ensure the pipeline can scale as the data volume grows.

## 🛠 The Tech Stack
* **Data Processing:** `PySpark` (MLlib, Spark SQL)
* **Data Modeling:** `dbt` (Data Build Tool)
* **Database:** `PostgreSQL`
* **Inference/API:** `Python`
* **Infrastructure:** `Docker`, `JDBC`
* **Environment:** `Dotenv` for secure configuration management

## 🏗 The Architecture
The pipeline follows a modern **ELT + ML** workflow:

1.  **Ingestion Layer:** Raw financial data is ingested into **PostgreSQL** via JDBC.
2.  **Transformation Layer (dbt):** dbt models clean raw data into `analytics` schemas, handling currency normalization and time-series joins.
3.  **Feature Engineering (Spark):** PySpark computes technical indicators (Rolling Volatility, SMA) and handles **StandardScaling**.
4.  **Tiered Modeling:**
    * **Tier 1:** GBT Regressor identifies patterns in price action.
    * **Tier 2:** Macro-economic features (Interest Rates, USD/LKR) are joined to filter technical signals.
6.  **Inference:** A dedicated prediction script loads the persisted **Spark ML Pipeline** to generate signals for live records.
