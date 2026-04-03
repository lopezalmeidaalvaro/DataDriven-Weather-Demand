# 🏝️ Data-Driven Analysis: Saharan Dust (Calima) Impact on Last-Minute Hotel Bookings

### *A Quantitative Study for Revenue Management Optimization in Gran Canaria (2025)*

![Python](https://img.shields.io/badge/python-3.10+-blue.svg) ![Pandas](https://img.shields.io/badge/library-pandas-orange.svg) ![AEMET](https://img.shields.io/badge/Data%20Source-AEMET%20API-green.svg) ![ISTAC](https://img.shields.io/badge/Data%20Source-ISTAC%20Microdata-yellow.svg)

---

## 🎯 1. Problem Statement & Stakeholders

**The Problem:** In the hotel industry, decision-making is often driven by "gut feelings" or historical heuristics rather than empirical evidence. A common belief among hotel directors in the Canary Islands is that Saharan Dust episodes (Calima) significantly deter "Last-Minute" bookings (reservations made < 15 days before arrival), leading to panic-driven price drops and inefficient marketing spend.

**The Stakeholders:**

* **Hotel General Managers:** Seeking to validate if weather alerts should trigger pricing changes.
* **Revenue Managers:** Requiring data-backed evidence to protect the Average Daily Rate (ADR).
* **Marketing Teams:** Needing to optimize ad spend during meteorological alerts.

---

## 💡 2. Initial Hypotheses

* **$H_0$ (Null Hypothesis):** There is no significant correlation between Calima intensity and the ratio of Last-Minute bookings.
* **$H_1$ (Alternative Hypothesis):** There is a strong negative correlation ($r \approx -1$), where an increase in Calima days leads to a sharp decline in short-term booking intent.

---

## 🛠️ 3. Data Engineering Procedure & Sources

This project implements a full **ETL (Extract, Transform, Load) Pipeline** to consolidate heterogeneous data sources:

### A. Data Extraction

* **AEMET OpenData (REST API):** Programmatic extraction of daily meteorological series from station `C629X` (Puerto de Mogán). Implemented **batch processing** to handle API rate limits and data volume constraints.
* **ISTAC Microdata (Batch CSV):** Processing of anonymized transactional surveys from the Canary Islands Institute of Statistics. This involved handling over **3,500 individual records** to isolate the 4-star hotel segment in Gran Canaria.

### B. Data Transformation & Cleaning

* **Type Casting:** Conversion of European decimal formats to standard floats for mathematical processing.
* **Schema Drift Mitigation:** Implementation of robust counting methods (`.size()` vs `.count()`) to handle null values in longitudinal datasets where questionnaire IDs were inconsistently reported across quarters.
* **Relational Merging:** Performing a *Left Join* using the temporal dimension (`OLA`/Month) as the primary key.

---

## 🧪 4. Advanced Feature Engineering: Dynamic Anomaly Detection

A static threshold for "heat" (e.g., $> 27.5$°C) fails in subtropical climates due to **Seasonality Bias**. To isolate *true* Calima events from standard summer heat, I developed a **Dynamic Thresholding Algorithm**:

A day $i$ in month $m$ is classified as a "Calima Event" ($C_i$) only if:

$$
C_i = (T_{max, i} \ge \bar{T}_{max, m} + 4.5^\circ C) \land (RH_{min, i} \le 55\text{\%})
$$

Where:

* $\bar{T}_{max, m}$ is the rolling mean temperature for that specific month.
* This approach successfully eliminated **False Positives** during August, providing a high-fidelity proxy for Saharan Dust intrusions.

---

## 🧮 5. Mathematical Implications & Results

To measure the relationship between the independent variable (Calima Days) and the dependent variable (Last-Minute Ratio), I applied the **Pearson Correlation Coefficient ($r$):**

$$
r = \frac{\sum (X_i - \bar{X})(Y_i - \bar{Y})}{\sqrt{\sum (X_i - \bar{X})^2 \sum (Y_i - \bar{Y})^2}}
$$

### **The Outcome:**

* **Calculated $r = 0.268$**
* **Interpretation:** The result indicates a **weak positive correlation**.

**Conclusion:** We failed to reject the Null Hypothesis ($H_0$). The data proves that Last-Minute booking intent is **inelastic** to Calima events. Tourists booking on short notice are likely driven by price, flight availability, or structural seasonality rather than immediate atmospheric conditions.

---

## 📈 6. Business Impact & ROI

* **Price Integrity:** Hotels should **not** decrease rates reactively during Calima alerts.
* **Marketing Efficiency:** Significant cost savings can be achieved by reallocating "emergency" ad budgets to high-intent periods.
* **Operational Readiness:** The automated pipeline allows for real-time monitoring of external factors against internal PMS data.

---

## 💻 7. Installation & Usage

1. **Clone the repository:**

   ```bash
   git clone [https://github.com/YourUser/Calima-Hotel-Impact.git](https://github.com/YourUser/Calima-Hotel-Impact.git)
2. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
3. **Run the Pipeline:**

   ```bash
   python scripts/pipeline_final.py

4. **Clone the repository:**

   ```bash
   git clone [https://github.com/YourUser/Calima-Hotel-Impact.git](https://github.com/YourUser/Calima-Hotel-Impact.git)
