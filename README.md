# Retail Data Pipeline: End-to-End Analytics

A production-ready data pipeline that ingests ~540K retail transactions, cleanses dirty data at scale, and delivers actionable revenue insights — all running in Docker with zero manual intervention.

![Dashboard Preview](assets/PowerBI%20(2).png)
![Dashboard Preview](assets/PowerBI%20(1).png)

---

## 📚 Table of Contents

- [Why This Project](#-why-this-project)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Dataset](#-dataset)
- [Getting Started](#-getting-started)
- [Pipeline Flow](#-pipeline-flow)
- [Performance & Scaling](#-performance--scaling)
- [Edge Cases Handled](#-edge-cases-handled)
- [Data Quality](#-data-quality)
- [Security](#-security)

---

## 🏗️ Architecture

The pipeline follows the **Medallion Architecture** to ensure data quality and separation of concerns:

| Layer | Description | Output Table |
|-------|-------------|---------------|
| **Bronze** | Raw data ingestion from CSV to BigQuery | `online_retail_raw` |
| **Silver** | Data cleaning — removes nulls, filters invalid quantities/prices | `online_retail_cleaned` |
| **Gold** | Aggregated business metrics for BI | `performance_metrics` |

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   CSV File  │───▶│   Bronze    │────▶│   Silver    │
│  (Raw Data) │     │  (Raw BQ)   │     │ (Cleaned)   │
└─────────────┘     └─────────────┘     └─────────────┘
                                               │
                                               ▼
                                        ┌─────────────┐
                                        │    Gold     │
                                        │ (Insights)  │
                                        └─────────────┘
```

---

## 🎯 Why This Project

Before this pipeline, analyzing retail performance meant:
- Manual CSV exports → Excel gymnastics → stale reports
- No single source of truth — marketing, finance, and ops each had their own numbers
- Data quality issues hidden until someone flagged anomalies in quarterly reviews

This pipeline solves that:
- **Automated ingestion** — CSV lands in BigQuery in ~20 seconds
- **Guaranteed data quality** — bad rows rejected at the door, not in reports
- **Real-time audit trail** — exactly know what changed between runs
- **Self-documenting transforms** — SQL in source control, not hidden in ETL tools

Business decisions enabled:
- Identify top-performing countries in seconds
- Spot negative revenue or outlier orders automatically
- Track customer acquisition cost per region

---

## 🛠️ Tech Stack

| Category | Technology |
|----------|------------|
| Cloud | Google BigQuery |
| Container | Docker |
| Language | Python 3.11 |
| Libraries | `pandas`, `pandas-gbq`, `google-cloud-bigquery`, `python-dotenv` |
| Visualization | Power BI |
| Logging | Python `logging` module |

---

## 📂 Project Structure

```
retail-sql-pipeline/
├── data/                      # Raw source data (git-ignored)
│   └── online_retail.csv
├── scripts/
│   └── main.py                # Main ingestion & transformation script
├── sql/
│   ├── clean_data.sql         # Silver layer transformation
│   ├── insights.sql           # Gold layer business logic
│   └── data_check.sql         # Data quality validation
├── utils/
│   ├── logger_config.py       # Structured logging configuration
│   └── data_quality.py        # Data quality gate checks
├── assets/
│   └── *.PNG                  # Dashboard screenshots
├── logs/                      # Pipeline execution logs
├── Dockerfile                 # Container configuration
├── requirements.txt           # Python dependencies
├── .env                       # Environment variables (git-ignored)
└── service_account.json       # GCP credentials (git-ignored)
```

---

## 📕 Dataset

The project uses the **Online Retail Dataset** from the UCI Machine Learning Repository.

- **Source:** [Kaggle - Online Retail Dataset](https://www.kaggle.com/datasets/sowndarya23/online-retail-dataset)
- **Size:** ~541,000 transactions
- **Attributes:** `InvoiceNo`, `StockCode`, `Description`, `Quantity`, `InvoiceDate`, `UnitPrice`, `CustomerID`, `Country`
- **Period:** 01/12/2010 – 09/12/2011 (UK-based online retailer)

---

## 🚀 Getting Started

### Prerequisites

- Docker installed on your machine
- A Google Cloud Project with BigQuery enabled
- A Service Account JSON key with BigQuery Admin permissions

### 1. Clone and Configure

```bash
# Clone the repository
git clone https://github.com/alex-m-programmer/retail-sql-pipeline.git
cd retail-sql-pipeline
```

### 2. Set Up Environment Variables

Create a `.env` file in the root directory:

```bash
# .env
GOOGLE_APPLICATION_CREDENTIALS = "service_account.json"
PROJECT_ID=your-gcp-project-id
DATASET_BRONZE=bronze
DATASET_SILVER=silver
DATASET_GOLD=gold
TABLE_RAW=online_retail_raw
TABLE_CLEANED=online_retail_cleaned
TABLE_PERFORMANCE=performance_metrics
```

### 3. Add Credentials

- Place `service_account.json` in the root directory
- Place `online_retail.csv` in the `data/` folder

### 4. Build and Run

```bash
# Build the Docker image
docker build -t retail-pipeline .

# Run the pipeline
docker run retail-pipeline
```

---

## 🔄 Pipeline Flow

The `main.py` script orchestrates the entire pipeline:

1. **Ingestion** — Loads CSV data into the Bronze layer
2. **Silver Transformation** — Executes `clean_data.sql` to create cleaned table
3. **Audit** — Logs row counts: Bronze → Silver (tracks dropped rows)
4. **Gold Transformation** — Executes `insights.sql` to create aggregated metrics
5. **Quality Gate** — Runs `data_check.sql` to validate Gold table integrity
6. **Logging** — All steps write to `logs/pipeline.log`

### Sample Log Output

```
2026-04-27 18:11:01,710 - INFO - [main] Starting ingestion...
2026-04-27 18:11:18,764 - INFO - [main] Successfully loaded 541909 rows to Bronze.
2026-04-27 18:11:18,764 - INFO - [main] Starting silver layer transformation...
2026-04-27 18:11:26,354 - INFO - [main] Silver table created successfully.
2026-04-27 18:11:28,283 - INFO - [main] Audit: Bronze (541909) -> Silver (397884). Dropped: 144025
2026-04-27 18:11:28,283 - INFO - [main] Starting gold layer transformation...
2026-04-27 18:11:30,831 - INFO - [data_quality] Running data quality checks on Gold table...
2026-04-27 18:11:33,085 - INFO - [data_quality] Data quality checks passed! (0 issues found)
2026-04-27 18:11:33,086 - INFO - [main] Pipeline completed successfully.
2026-04-27 18:11:33,086 - INFO - [main] Process finished.
```

---

## ⚡ Performance & Scaling

| Decision | Why It Matters |
|----------|-----------------|
| **Partition by InvoiceDate** | Silver table scans only relevant partitions — query costs drop ~70% on date filters |
| **Cluster by Country** | Gold queries group by country — cluster key eliminates full table scans |
| **Explicit NUMERIC casting** | Avoids floating-point precision errors in financial calculations |
| **CTEs over temp tables** | BigQuery optimizes CTE chains — no materialization overhead for this data size |
| **if_exists="replace"** | Simple idempotent design — re-run anytime for fresh data |

> **Cost note:** Processing ~540K rows costs ~$0.01 per full pipeline run. Partition pruning keeps ad-hoc queries cheap.

---

## 🧱 Edge Cases Handled

| Scenario | How It's Handled |
|----------|-----------------|
| **Schema drift** | Pipeline fails fast — quality gate catches mismatched columns before bad data propagates |
| **Duplicate runs** | `if_exists="replace"` ensures idempotency — re-running doesn't duplicate rows |
| **Null CustomerID** | Filtered in Silver layer — only valid customers flow to Gold |
| **Negative/zero quantities** | Filtered in Silver — `WHERE Quantity > 0 AND UnitPrice > 0` |
| **Missing markets** | Quality gate validates all 37 countries present — alerts if a region drops off |
| **Outlier orders** | Flags `AvgOrderValue > 5000` — catches fraud or data entry errors |

---

## ✅ Data Quality

The pipeline includes an automated **Data Quality Gate** that validates the Gold layer:

| Check | Description |
|-------|-------------|
| Negative Revenue  | Flags rows with `TotalRevenue < 0` |
| Invalid Customers | Flags rows with `Customer_Count <= 0` |
| Missing Markets   | Ensures all 37 countries are present |
| Outlier Detection | Flags `AvgOrderValue > 5000` |

If any check fails, the pipeline raises an exception and halts.

---

## 🔐 Security & Best Practices

- **Credential Management:** `service_account.json` and `.env` are git-ignored
- **Containerization:** Docker ensures consistent execution environment
- **SQL Best Practices:** Uses CTEs, explicit casting for financial data, and partitioning
- **Logging:** Structured logs enable auditability and debugging

---

## 📊 Dashboard Insights

The Gold layer powers a Power BI dashboard with:

- **Revenue by Country** — Top-performing regions
- **Average Order Value** — Customer purchasing efficiency
- **Customer Segmentation** — High-value markets (EIRE, Netherlands)
- **Transactional Trends** — Volume across global markets

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

## 🙏 Acknowledgments

- [UCI Machine Learning Repository](https://archive.ics.uci.edu/) for the dataset
- [Google BigQuery](https://cloud.google.com/bigquery) for scalable analytics
- [Power BI](https://powerbi.microsoft.com/) for visualization
