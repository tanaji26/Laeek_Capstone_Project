
# SmartRetail Analytics — Agentic Data Engineering Pipeline

> An end-to-end, production-grade data engineering pipeline built with Apache Airflow, Apache Iceberg, Trino, MinIO, and OpenAI. Implements a multi-agent Medallion Architecture (Bronze → Silver → Gold) with AI-generated business insights.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Apache Airflow (Orchestrator)                 │
│                         LocalExecutor | DAG: smartretail_pipeline    │
└────────────┬────────────────────────────────────────────────────────┘
             │
             ▼
┌────────────────────────────────────────────────────────────────────────────┐
│                          AGENT PIPELINE                                     │
│                                                                             │
│  [1] generate_data  →  [2] ingest_to_minio  →  [3] governance_agent        │
│                                                         │                   │
│                                               [4] quality_agent             │
│                                                         │                   │
│                                               [5] iceberg_loader            │
│                                                         │                   │
│                                               [6] insight_agent             │
└────────────────────────────────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA LAKEHOUSE                                │
│                                                                      │
│  MinIO (S3-compatible)                                               │
│  ├── s3://data/bronze/        ← raw Parquet (ingested data)          │
│  ├── s3://data/silver/        ← enriched/joined Parquet              │
│  ├── s3://data/gold/          ← aggregated KPI tables                │
│  └── s3://data/warehouse/     ← Iceberg table files                  │
│                                                                      │
│  Apache Iceberg (REST Catalog)  ←→  Trino (SQL query engine)        │
└─────────────────────────────────────────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     OUTPUTS                                          │
│  /data/logs/insight_summary.md   ← Markdown business report         │
│  /data/logs/insight_report.json  ← Full JSON report                 │
│  /data/logs/quality_report.json  ← Data quality scores              │
│  /data/logs/lineage_log.json     ← Data lineage audit trail         │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Orchestration | Apache Airflow 2.9 | DAG scheduling, task dependency management |
| Object Storage | MinIO | S3-compatible data lake storage |
| Table Format | Apache Iceberg | ACID transactions, schema evolution, time travel |
| Query Engine | Trino 438 | Distributed SQL over Iceberg tables |
| Catalog | Iceberg REST Catalog | Table metadata management |
| Data Processing | Python / Pandas | ETL transformations |
| AI Insights | OpenAI GPT-4o | Natural language business intelligence |
| Containerization | Docker Compose | Local multi-service orchestration |
| Source DBs | PostgreSQL 16, MySQL 8 | Simulated OLTP source systems |

---

## Agent Descriptions

### 1. `generate_data` — Data Generator
Synthesizes realistic Indian e-commerce data: 10,000 orders, 200 customers, 50 products, and 5,625 feedback records. Seeds PostgreSQL and MySQL source tables.

### 2. `ingest_to_minio` — Ingestion Agent
Reads from PostgreSQL and MySQL source systems, converts to Parquet format, and lands raw files in the **Bronze zone** (`s3://data/bronze/`).

### 3. `governance_agent` — Governance Agent
Applies business rules and data enrichment:
- Joins orders with customer and product dimensions
- Computes derived fields (`discount_pct`, `final_amount`, `is_prime_active`)
- Writes enriched data to the **Silver zone** (`s3://data/silver/`)
- Appends a lineage entry to `lineage_log.json`

### 4. `quality_agent` — Data Quality Agent
Runs 15+ data quality checks across all Silver tables:
- Null checks, range validations, referential integrity
- Duplicate detection, business rule assertions
- Produces a scored quality report (`overall_score/100`)
- Flags rows to quarantine zone on failure

### 5. `iceberg_loader` — Iceberg Loader
Creates Iceberg schemas and tables in the `iceberg.landing` namespace via Trino DDL, then batch-INSERTs Bronze Parquet data (500 rows/batch) into Iceberg tables. Supports idempotent re-runs.

### 6. `insight_agent` — Insight Agent
Builds **Gold layer** aggregations (revenue summary, category performance, customer segments, regional breakdown) and calls OpenAI GPT-4o to generate:
- Executive summary
- Product & category insights
- Customer & regional analysis
- Operational risk flags

Outputs a formatted Markdown report and JSON artifact.

---

## Data Flow (Medallion Architecture)

```
Source Systems                Bronze              Silver              Gold
(PostgreSQL/MySQL)            (Raw Parquet)       (Enriched Parquet)  (Aggregated Parquet)
       │                           │                    │                    │
  orders_data     ──ingest──►  orders.parquet  ──gov──► orders_enriched ──► revenue_summary
  customers_data  ──ingest──►  customers.parquet        customers_clean     category_perf
  products_data   ──ingest──►  products.parquet         products_clean      product_perf
  feedback_data   ──ingest──►  feedback.parquet         feedback_enriched   regional_perf
                                                                             customer_segments
```

---

## Project Structure

```
smartretail_pipeline/
├── dags/
│   ├── orchestration_agent.py       # Main Airflow DAG definition
│   └── ingest/
│       ├── Dockerfile.airflow       # Custom Airflow image
│       ├── generate_data.py         # Synthetic data generator
│       ├── ingest_agent.py          # Bronze ingestion
│       ├── governance_agent.py      # Silver enrichment
│       ├── quality_agent.py         # Data quality checks
│       ├── create_iceberg_tables.py # Iceberg DDL + loader
│       ├── insight_agent.py         # Gold layer + OpenAI insights
│       ├── pipeline_monitor.py      # Shared logging utilities
│       ├── schema_loader.py         # YAML schema parser
│       └── schemas/                 # Table schema definitions (YAML)
├── docker/
│   ├── trino/
│   │   ├── etc-coordinator/         # Trino coordinator config
│   │   └── etc-worker/              # Trino worker config
│   └── init/
│       ├── pg_source.sql            # PostgreSQL source schema
│       ├── mysql_source.sql         # MySQL source schema
│       └── pg_catalog.sql           # Iceberg catalog schema
├── data/
│   └── logs/                        # Pipeline output artifacts
├── docker-compose.yml
├── .env                             # Environment variables (not committed)
└── README.md
```

---

## Quick Start

### Prerequisites
- Docker Desktop (8GB RAM recommended)
- Python 3.10+
- OpenAI API key

### 1. configure environment

cp .env.example .env
# Edit .env and add your OpenAI API key
```

### 2. Configure `.env`

```env
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minio
MINIO_SECRET_KEY=minio123
OPENAI_API_KEY=sk-your-key-here
OPENAI_MODEL=gpt-4o
```

### 3. Start all services

```bash
docker compose up -d --build
```

### 4. Wait for services to be healthy (~2 minutes)

```bash
docker compose ps   # all should show "healthy" or "running"
```

### 5. Trigger the pipeline

```bash
docker exec airflow-scheduler airflow dags unpause smartretail_pipeline
docker exec airflow-scheduler airflow dags trigger smartretail_pipeline
```

### 6. Monitor

- **Airflow UI**: http://localhost:8085 (admin / admin)
- **MinIO Console**: http://localhost:9001 (minio / minio123)
- **Trino UI**: http://localhost:13579

### 7. View the insight report

```bash
cat data/logs/insight_summary.md
```

---

## Key Metrics (Sample Run)

| Metric | Value |
|---|---|
| Total Orders | 10,000 |
| Delivered | 8,036 |
| Cancellation Rate | 11.89% |
| Return Rate | 7.75% |
| Total GMV | ₹200,308,701 |
| Net Revenue | ₹157,562,821 |
| Avg Order Value | ₹19,607 |
| Data Quality Score | 100 / 100 |
| Pipeline Duration | ~12 minutes |

---

## Data Lineage

Every agent writes a lineage entry to `/data/logs/lineage_log.json`:

```json
[
  {
    "agent": "governance_agent",
    "timestamp": "2026-06-09T13:16:45Z",
    "inputs": ["s3://data/bronze/orders_data.parquet", "s3://data/bronze/customers_data.parquet"],
    "outputs": ["s3://data/silver/orders_enriched.parquet"],
    "rows_in": 10000,
    "rows_out": 10000,
    "status": "success"
  }
]
```

---

## Future Improvements

- Scale Trino to multi-worker for parallel query execution
- Add Slack/email alerts on pipeline failure via Airflow callbacks
- Replace synthetic data with real e-commerce dataset

---

## Author

**Laeek Tanaji** — TCS iON Applied Agentic AI for Modern Data Engineering  
*SmartRetail Analytics Capstone Project, 2026*