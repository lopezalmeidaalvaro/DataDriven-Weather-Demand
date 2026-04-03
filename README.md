# 🏝️ Data-Driven Revenue Management: Meteorological Impact on Hotel Demand
### *An End-to-End ETL & Statistical Analysis Pipeline for the Hospitality Sector (Gran Canaria)*

![Python](https://img.shields.io/badge/Python-3.10+-blue?style=for-the-badge&logo=python&logoColor=white) ![Pandas](https://img.shields.io/badge/Pandas-Data_Wrangling-150458?style=for-the-badge&logo=pandas&logoColor=white) ![API](https://img.shields.io/badge/AEMET-REST_API-green?style=for-the-badge) ![Data](https://img.shields.io/badge/ISTAC-Microdata-yellow?style=for-the-badge) ![Status](https://img.shields.io/badge/Status-Production_Ready-success?style=for-the-badge)

> **TL;DR:** This project engineers a robust data pipeline to test a widespread hospitality industry assumption: *Does Saharan Dust (Calima) negatively impact last-minute hotel bookings?* By engineering dynamic weather anomaly thresholds and calculating Pearson's correlation ($r = 0.268$), the analysis proves that last-minute demand is **inelastic** to Calima, protecting hotel ADR (Average Daily Rate) from unwarranted reactive price drops.

---

## 🎯 1. Business Context & Problem Statement

In the hospitality industry, pricing elasticity models are frequently skewed by heuristic biases or "gut feelings" rather than empirical data. A widespread assumption among Hotel General Managers in the Canary Islands is that **Calima events significantly reduce last-minute booking demand**. 

This assumption historically leads to:
1. **Reactive Price Dumping:** Unnecessary reduction of the ADR to stimulate perceived low demand.
2. **Inefficient CAC (Customer Acquisition Cost):** Reallocating marketing budgets to "panic" campaigns.
3. **Revenue Instability:** Margin erosion based on meteorological alerts rather than actual booking pace.

**The Objective:** Architect a reproducible data pipeline to programmatically extract, clean, and correlate external climatological data with internal booking metrics to validate or refute this hypothesis.

---

## 🏗️ 2. Data Pipeline Architecture (ETL)
The project relies on a programmatic **Extract-Transform-Load (ETL)** pipeline designed for idempotency and resilience against dirty data.

### A. Data Extraction
* **Meteorological Data (AEMET OpenData API):** Automated extraction of daily time-series weather data from station `C629X` (Puerto de Mogán). Implemented batch-request handling to bypass strict API rate limits.
* **Tourism Microdata (ISTAC):** Batch ingestion of large-scale transactional surveys (>3,500 individual records). Filtered specifically for the 4-star hotel segment in Gran Canaria to ensure parity with the target business model.

### B. Transformation & Schema Drift Mitigation
* **Data Harmonization:** Type-casting European numerical formats to standard floating-point variables.
* **Resilient Aggregation:** Implemented robust counting strategies (`.size()` vs `.count()`) to mitigate **Schema Drift**. This prevented critical pipeline failures caused by inconsistently reported questionnaire IDs (Nulls) across different quarters.
* **Relational Merging:** Conducted a Left Join unifying disparate datasets via a shared temporal dimension (`Month/OLA`).

---

## 🧠 3. Advanced Feature Engineering: Dynamic Anomaly Detection
A static temperature threshold (e.g., $T > 27.5^\circ C$) is statistically unreliable in subtropical climates due to heavy **seasonality bias** (yielding 100% false positives during August).

To isolate *true* Saharan Dust intrusions, I engineered a **Dynamic Thresholding Algorithm**. A day $i$ in month $m$ is flagged as a Calima Anomaly ($C_i$) if and only if it exceeds the historical rolling average of its specific month, combined with a severe drop in humidity:

$$
C_i = (T_{max, i} \ge \bar{T}_{max, m} + 4.5^\circ C) \land (RH_{min, i} \le 55\text{\%})
$$

*Where:*
* $\bar{T}_{max, m}$ = Monthly rolling average maximum temperature.
* This dynamic heuristic successfully filtered out summer heat waves, accurately isolating genuine dust events and improving the signal-to-noise ratio of the dataset.

---

## 🧮 4. Statistical Modeling & Results
We evaluated the relationship between Calima Days ($X$) and the Last-Minute Booking Ratio ($Y$) using the **Pearson Correlation Coefficient**:

$$
r = \frac{\sum (X_i - \bar{X})(Y_i - \bar{Y})}{\sqrt{\sum (X_i - \bar{X})^2 \sum (Y_i - \bar{Y})^2}}
$$

### 📊 Key Findings

| Metric | Result | Statistical Interpretation | Business Translation |
| :--- | :---: | :--- | :--- |
| **Pearson ($r$)** | `0.268` | Weak positive correlation. | Weather does not deter impulsive buyers. |
| **Null Hypothesis** | Retained | Variables are largely independent. | Dust events do not drive cancellations. |
| **Revenue Risk** | Minimal | Demand is inelastic to this factor. | Price drops during Calima are unjustified. |

> **Executive Conclusion:** Short-term booking demand in Gran Canaria is statistically insensitive to Saharan Dust events. Hotels reacting with price reductions during Calima alerts are sacrificing revenue margin unnecessarily.

---

## 📂 5. Repository Structure 

```text
DataDriven-Weather-Demand/
├── data/
│   └── raw/                              # Place your ISTAC & AEMET CSV files here
├── docs/
│   ├── executive_summary.md              # Business-facing insights
│   ├── personal_study_notes.md           # Personal study guide
│   └── technical_annex.md                # Mathematical proofs & methodology
├── scripts/
│   ├── etl_pipeline_analytics.py         # Main ETL logic & correlation engine
│   └── extract_aemet_api.py              # API connection & data fetching batch script
├── .env.example                          # Environment variables template
├── .gitignore                            # Python caches & sensitive keys
├── requirements.txt                      # Pinned dependency tree
└── README.md                             # Project documentation
```

---

## 🚀 6. Reproducibility & Installation
The pipeline is fully reproducible. To run the analysis on your local machine:
1. **Clone the repository:**
 ```Bash
git clone [https://github.com/lopezalmeidaalvaro/DataDriven-Weather-Demand.git](https://github.com/lopezalmeidaalvaro/DataDriven-Weather-Demand.git)
cd DataDriven-Weather-Demand
```
2.   **Install dependencies:**
```Bash
pip install -r requirements.txt
```
3. **Execute the ETL Pipeline:**
```Bash
python scripts/etl_pipeline_analytics.py
```
* **Note:** Ensure your AEMET API Key is safely stored as an environment variable (or within a .env file) before executing to allow the script to authenticate correctly.

---

## 🔮 7. Future Scalability (Next Steps)
To scale this proof-of-concept into an enterprise-grade product:
* **Cloud Orchestration:** Migrate the Python scripts to Apache Airflow (or AWS Step Functions) for automated daily runs and monitoring.
* **Machine Learning:** Integrate flight pricing data (AENA) to train a Random Forest regressor, predicting last-minute demand volume accurately by combining weather anomalies and connectivity factors.

---

👨‍💻 Architected & Developed by Álvaro López Almeida
Capstone Project — Data Engineering & Revenue Analytics
