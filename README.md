# Big_Data_Essentials_Group_11


# Rwanda Road Safety Intelligence System (RRSIS)

**HDFS + Apache Spark (PySpark) Analytics Engine**
Mid-Term Group Project — AUCA Masters, Big Data Essentials — Group 11

---

## 1. Overview

RRSIS is a prototype road-safety analytics pipeline built to demonstrate how HDFS and Apache Spark can process large-scale accident records and surface actionable road-safety intelligence — determining **where, when, and under what circumstances** accident risk is highest.

Rwanda National Police recorded **9,995 road accidents in 2023**, including **761 fatal accidents** (NISR Statistical Yearbook 2024, Table 14.2.6). This project prototypes the analytics engine that could process such records at scale.

> **Important:** This project uses the Kaggle *Road Accident Dataset* (UK-sourced, ~308K records) as a **surrogate dataset** to build and validate the pipeline, per the assignment brief. These are **not actual Rwanda accident records**. Rwanda-specific context is drawn separately from NISR and Rwanda National Police statistics.

---

## 2. Project Structure

```
Big_Data_Essentials_Group_11/
├── Data/
│   └── Road Accident Data.csv          # Kaggle surrogate dataset (place here)
├── notebooks/
│   ├── 01_ingestion.ipynb              # Task 1 — HDFS + Spark ingestion
│   ├── 02_data_quality.ipynb           # Task 2 — cleaning & data quality
│   └── 03_temporal_severity_risk.ipynb # Tasks 3–8 — full analytics pipeline
├── screenshots/                        # HDFS listings, Spark output, explain() plans
├── report/
│   └── RRSIS_Technical_Report.pdf     # Full technical report (pdf)
├── RRSIS_Presentation.pptx             # Viva presentation slides
└── README.md                           # This file
```

---

## 3. Environment Setup

### Prerequisites
| Component | Version Used |
|---|---|
| OS | Windows |
| Java (JDK) | OpenJDK 11 |
| Hadoop | 3.3.6 (local single-node, with Windows `winutils.exe`/`hadoop.dll`) |
| Apache Spark | 3.5.1 (pre-built for Hadoop 3.3) |
| Python | 3.x (Anaconda `ml_env` environment) |
| PySpark | 3.5.1 |

### Environment Variables (Windows)
```
JAVA_HOME   = C:\Java\jdk-11
HADOOP_HOME = C:\hadoop-3.3.6
SPARK_HOME  = C:\spark-3.5.1
```
Add `%JAVA_HOME%\bin`, `%HADOOP_HOME%\bin`, and `%SPARK_HOME%\bin` to your `Path`.

### Install PySpark
```bash
pip install pyspark==3.5.1 findspark jupyter pandas
```

---

## 4. Running the Project

### Step 1 — Start HDFS
```bash
start-dfs.cmd
```
Verify at **http://localhost:9870** (Namenode UI) or:
```bash
hdfs dfs -ls /
```

### Step 2 — Upload the dataset to HDFS
```bash
hdfs dfs -mkdir -p /rrsis/data
hdfs dfs -put "Data/Road Accident Data.csv" /rrsis/data/
hdfs dfs -ls /rrsis/data
```

### Step 3 — Launch Jupyter and run the notebooks in order
```bash
cd notebooks
jupyter notebook
```
Run **Cell 1 (setup + cleaning) first in every notebook** — each notebook is self-contained and re-loads/cleans the data from HDFS so it can run independently of other notebooks or prior kernel state.

### Step 4 — View the Spark Web UI (optional, while a session is active)
```
http://localhost:4040
```
Useful for visually inspecting job stages, shuffle sizes, and SQL execution DAGs (complements `df.explain(True)` output used in Task 8). Only available while the Spark session is running — confirm the exact port with:
```python
print(spark.sparkContext.uiWebUrl)
```

---

## 5. Dataset Summary

| Property | Value |
|---|---|
| Source | Kaggle: Road Accident Dataset (xavierberge) |
| Raw records | 307,973 |
| Raw columns | 23 |
| Granularity | 1 row per **vehicle involved** (not per accident) |
| Unique accidents | 197,644 (after deduplication on `Accident_Index`) |

---

## 6. Task Summary & Key Findings

| Task | Focus | Key Result |
|---|---|---|
| 1 | HDFS + Spark ingestion | 307,973 records, 23 columns loaded directly from HDFS |
| 2 | Data quality engineering | 7 issues identified (vehicle-vs-accident granularity, mis-inferred `Time`, string dates, "None" as valid category, true nulls, 1 duplicate, rare speed values) |
| 3 | Temporal intelligence | Top risk period: **Weekday Afternoon** (49,971 accidents); peak hour 17:00 |
| 4 | Severity Index | **Birmingham** highest total severity (7,805); **South Northamptonshire** highest avg. severity among high-volume districts (1.65/accident) |
| 5 | Dangerous-factor combinations | Vans (≤3.5t) on 70mph dual carriageways at Late Night: avg. severity 2.03 — highest of any combination |
| 6 | Window function ranking | Top 3 districts ranked per Urban/Rural area and per Police Force using `dense_rank()` |
| 7 | Road Safety Risk Score | Composite score (50% severity, 35% frequency, 15% danger rate) — **Birmingham scores 0.904**, ~2× the runner-up |
| 8 | Spark execution analysis | Shuffle boundaries identified via `explain(True)`; caching found to *not* help at low reuse counts (3.76s vs 5.07s) — a genuine, explainable result |
| 9 | Management recommendations | 5 evidence-based road-safety priorities (see technical report, Section 12) |

Full evidence, methodology, and justification for every task are documented in `report/RRSIS_Technical_Report.docx`.

---

## 7. Mandatory Spark Requirements — Compliance

- All major processing uses **PySpark DataFrame operations only** (no Pandas for core analysis).
- Demonstrated: `select()`, `filter()`/`where()`, `withColumn()`, `when()`, `groupBy()`, `agg()`, `orderBy()`.
- **Window functions**: `dense_rank()` over `Window.partitionBy(...).orderBy(...)` (Task 6).
- **Advanced operations**: `join()` (Task 7 component merging), `dropDuplicates()` with subset keys (Task 2), `explain(True)` plan inspection (Task 8).
- Dataset is read directly from HDFS (`hdfs://localhost:9000/...`) in every notebook.

---

## 8. Deliverables Checklist

- [x] Technical report (methodology, results, interpretation, recommendations)
- [x] PySpark source code / Jupyter notebooks
- [x] Evidence screenshots (HDFS storage, Spark output, `explain(True)` plans)
- [x] Final presentation (PPTX)
- [ ] Live viva presentation (Task 10)

---

## 9. Notes & Caveats

- The dataset is **UK-sourced** and used strictly as a surrogate for prototyping; findings should not be reported as Rwanda-specific accident patterns.
- Districts with very small accident counts (e.g. under ~30) were treated cautiously in average-severity rankings, since small-sample averages are statistically noisy.
- The project working directory sits inside OneDrive — pause OneDrive sync while running notebooks if file-lock errors occur during heavy read/write operations.

---

## 10. References

- National Institute of Statistics of Rwanda (NISR). *Statistical Yearbook 2024*, Table 14.2.6 — Road Accidents. Source: Rwanda National Police.
- Kaggle. *Road Accident Dataset* (xavierberge). https://www.kaggle.com/datasets/xavierberge/road-accident-dataset/data
- Apache Spark Documentation. https://spark.apache.org/docs/latest/
- Apache Hadoop Documentation. https://hadoop.apache.org/docs/stable/