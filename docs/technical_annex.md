# TECHNICAL & METHODOLOGICAL ANNEX: ETL PIPELINE AND STATISTICAL MODELING

**Project:** Hotel Cost Optimization via Meteorological Impact Analysis  
**Author:** Álvaro  
**Date:** April 2026  

---

## 1. System Architecture and Methodology (ETL)

[cite_start]This project is built upon an ETL (Extract, Transform, Load) architecture developed entirely in Python 3.10+[cite: 41]. [cite_start]The core objective is the asynchronous consolidation of heterogeneous public data to analyze tourist consumer behavior[cite: 42].

### 1.1. Data Sources

1. [cite_start]**AEMET OpenData API (REST):** Extraction of climatological time series [Daily granularity](cite: 44).
2. [cite_start]**ISTAC (Canary Institute of Statistics):** Batch processing of microdata from the Tourist Expenditure Survey [Transactional/Survey granularity](cite: 45).

### 1.2. Design Principles

* [cite_start]**Idempotency:** The pipeline can be executed multiple times while guaranteeing the exact same final state in the resulting DataFrame.
* [cite_start]**Modularity:** Strict separation of concerns between extraction logic, Feature Engineering, and statistical analysis.
* [cite_start]**Resilience (Schema Handling):** Implemented robust counting strategies (`.size()` vs `.count()`) to mitigate the Schema Drift detected in the ISTAC datasets during H2-2025.

---

## 2. Mathematical Justification: Feature Engineering (Anomaly Detection)

[cite_start]The independent variable "Calima" is not natively provided by the C629X weather station[cite: 51]. [cite_start]Therefore, developing a Proxy variable based on multivariate anomaly detection was required[cite: 52, 53].

[cite_start]The use of static thresholds (e.g., $T_{max} > 27.5^\circ C$) was discarded due to severe seasonality bias, which yielded a high rate of false positives during the summer months[cite: 54].

[cite_start]Instead, a **Seasonal Mean-Based Dynamic Threshold** was implemented[cite: 55]. [cite_start]A specific day $i$ in month $m$ is classified as a Calima episode ($C_i = 1$) if and only if both thermal and humidity conditions are met simultaneously[cite: 56]:

$$
C_{i} = \begin{cases} 1 & \text{if } (T_{max,i} \ge \bar{T}_{max,m} + \delta) \land (HR_{min,i} \le \gamma) \\ 0 & \text{otherwise} \end{cases}
$$

**Where:**

* [cite_start]$\bar{T}_{max,m}$: Historical average maximum temperature for month $m$[cite: 63].
* [cite_start]$\delta$: Thermal deviation considered anomalous (Empirically set to $+4.5^\circ C$)[cite: 64].
* [cite_start]$\gamma$: Maximum relative humidity threshold [Set to 55%](cite: 65).

[cite_start]This approach drastically reduces Type I errors (false positives) during the summer period, aligning the model accurately with historical records of real Calima events[cite: 66].

---

## 3. Statistical Justification: Pearson Correlation

[cite_start]To measure the strength and direction of the linear relationship between severe climatic episodes and booking lead time, the Pearson Correlation Coefficient ($r$) was applied[cite: 68]:

$$
r = \frac{\sum_{i=1}^{n}(X_{i}-\overline{X})(Y_{i}-\overline{Y})}{\sqrt{\sum_{i=1}^{n}(X_{i}-\overline{X})^{2}\sum_{i=1}^{n}(Y_{i}-\overline{Y})^{2}}}
$$

**Where:**

* [cite_start]**$X$:** Independent variable [Number of anomalous Calima days per month](cite: 71).
* [cite_start]**$Y$:** Dependent variable [Proportion of Last-Minute bookings out of the total monthly sample](cite: 72).
* [cite_start]**$n$:** Number of observations ($n = 12$ months)[cite: 73].

[cite_start]*(Note: The full refactored Python source code executing these methodologies can be found in the `scripts/` directory of this repository, following PEP-8 standards for maintainability [cite: 76, 77]).*
