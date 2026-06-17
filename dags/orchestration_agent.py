"""
dags/orchestration_agent.py
------------------------
Airflow DAG for SmartRetail Enterprise Agentic AI Data Pipeline.

Each pipeline agent becomes a PythonOperator task.
Task dependencies replace .done flag file checks.
Retries replace manual re-run logic.

Schedule: Daily at 6:00 AM UTC
UI:       http://localhost:8085

Pipeline flow:
  generate_data → ingest_to_minio → governance_agent
    → quality_agent → iceberg_loader → insight_agent
"""

import sys
import os
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago


from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Add scripts directory to Python path
SCRIPTS_DIR = "/opt/airflow/dags/ingest"
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

#Default task arguments
default_args = {
    "owner":            "smartretail",
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
    "email_on_failure": False,
    "email_on_retry":   False,
}

# ── Task callables
# Each function imports its module fresh at call time.
# Lineage is logged at the end of every task using pipeline_lineage.log_lineage().

def task_generate_data(**context):
    """Generate synthetic CSV datasets → /data/"""
    import base_data_generation
    import pipeline_lineage

    base_data_generation.run()

    pipeline_lineage.log_lineage(
        agent    = "generate_data",
        inputs   = ["synthetic_seed=42"],
        outputs  = [
            "/data/orders_data.csv",
            "/data/customers_data.csv",
            "/data/products_data.csv",
            "/data/feedback_data.csv",
        ],
        rows_in  = 0,
        rows_out = 15875,   # 10000 orders + 200 customers + 50 products + 5625 feedback
        notes    = "Synthetic Indian e-commerce data generated with fixed seed",
    )


def task_ingest_to_minio(**context):
    """Upload CSVs from /data/ → s3://data/bronze/"""
    import ingest_to_minio
    import pipeline_lineage

    ingest_to_minio.main()

    pipeline_lineage.log_lineage(
        agent    = "ingest_to_minio",
        inputs   = [
            "/data/orders_data.csv",
            "/data/customers_data.csv",
            "/data/products_data.csv",
            "/data/feedback_data.csv",
        ],
        outputs  = [
            "s3://data/bronze/orders_data.parquet",
            "s3://data/bronze/customers_data.parquet",
            "s3://data/bronze/products_data.parquet",
            "s3://data/bronze/feedback_data.parquet",
        ],
        rows_in  = 15875,
        rows_out = 15875,
        notes    = "CSV → Parquet conversion, landed in MinIO Bronze zone",
    )


def task_governance(**context):
    """
    Schema enforcement + PII masking → s3://data/bronze/
    Silver layer enrichment          → s3://data/silver/
    """
    import governance_agent
    import pipeline_lineage

    governance_agent.run()

    pipeline_lineage.log_lineage(
        agent    = "governance_agent",
        inputs   = [
            "s3://data/bronze/orders_data.parquet",
            "s3://data/bronze/customers_data.parquet",
            "s3://data/bronze/products_data.parquet",
            "s3://data/bronze/feedback_data.parquet",
        ],
        outputs  = [
            "s3://data/silver/orders_enriched.parquet",
            "s3://data/silver/customers_clean.parquet",
            "s3://data/silver/products_clean.parquet",
            "s3://data/silver/feedback_enriched.parquet",
        ],
        rows_in  = 15875,
        rows_out = 15875,
        notes    = "Schema enforcement, PII masking, join enrichment, discount/AOV derivation",
    )


def task_quality(**context):
    """31 DQ checks across 4 bronze tables → quality_report.json"""
    import quality_agent
    import pipeline_lineage

    quality_agent.run()

    pipeline_lineage.log_lineage(
        agent    = "quality_agent",
        inputs   = [
            "s3://data/silver/orders_enriched.parquet",
            "s3://data/silver/customers_clean.parquet",
            "s3://data/silver/products_clean.parquet",
            "s3://data/silver/feedback_enriched.parquet",
        ],
        outputs  = ["/data/logs/quality_report.json"],
        rows_in  = 15875,
        rows_out = 0,
        notes    = "31 DQ checks: nulls, ranges, referential integrity, duplicates, business rules",
    )


def task_iceberg(**context):
    """
    CREATE Iceberg tables in Trino + batch INSERT
    from bronze Parquet → iceberg.landing.*
    """
    import create_iceberg_tables
    import pipeline_lineage

    create_iceberg_tables.main()

    pipeline_lineage.log_lineage(
        agent    = "iceberg_loader",
        inputs   = [
            "s3://data/bronze/orders_data.parquet",
            "s3://data/bronze/customers_data.parquet",
            "s3://data/bronze/products_data.parquet",
            "s3://data/bronze/feedback_data.parquet",
        ],
        outputs  = [
            "iceberg.landing.orders_data",
            "iceberg.landing.customers_data",
            "iceberg.landing.products_data",
            "iceberg.landing.feedback_data",
        ],
        rows_in  = 15875,
        rows_out = 15875,
        notes    = "Trino DDL CREATE TABLE IF NOT EXISTS + batch INSERT 500 rows/batch",
    )


def task_insight(**context):
    """
    Build Gold KPI tables → s3://data/gold/
    Generate LLM narratives via OpenAI
    Write insight_report.json + insight_summary.md
    """
    import insight_agent
    import pipeline_lineage

    insight_agent.run()

    pipeline_lineage.log_lineage(
        agent    = "insight_agent",
        inputs   = [
            "s3://data/silver/orders_enriched.parquet",
            "s3://data/silver/customers_clean.parquet",
            "s3://data/silver/products_clean.parquet",
            "s3://data/silver/feedback_enriched.parquet",
        ],
        outputs  = [
            "s3://data/gold/revenue_summary.parquet",
            "s3://data/gold/monthly_revenue_trend.parquet",
            "s3://data/gold/category_performance.parquet",
            "s3://data/gold/product_performance.parquet",
            "s3://data/gold/customer_segment_summary.parquet",
            "s3://data/gold/regional_performance.parquet",
            "/data/logs/insight_report.json",
            "/data/logs/insight_summary.md",
        ],
        rows_in  = 15875,
        rows_out = 0,
        notes    = "Gold aggregations + OpenAI GPT-4o narrative insights",
    )


# DAG definition
with DAG(
    dag_id          = "smartretail_pipeline",
    default_args    = default_args,
    description     = "SmartRetail Enterprise Agentic AI Data Pipeline — Medallion Architecture",
    schedule        = None,                # ← change from "0 6 * * *" to None
    start_date      = days_ago(1),
    catchup         = False,
    max_active_runs = 1,
    tags            = ["smartretail", "capstone", "medallion", "agentic-ai"],
) as dag:

    t1_generate = PythonOperator(
        task_id         = "generate_data",
        python_callable = task_generate_data,
        doc_md          = "Generate synthetic Indian e-commerce datasets (seed=42)",
    )

    t2_ingest = PythonOperator(
        task_id         = "ingest_to_minio",
        python_callable = task_ingest_to_minio,
        doc_md          = "Upload 4 CSV files to MinIO Bronze zone as Parquet",
    )

    t3_governance = PythonOperator(
        task_id         = "governance_agent",
        python_callable = task_governance,
        doc_md          = "Schema enforcement, PII masking → bronze; enrichment → silver",
    )

    t4_quality = PythonOperator(
        task_id         = "quality_agent",
        python_callable = task_quality,
        doc_md          = "31 DQ checks across 4 tables. DQ score 0–100.",
    )

    t5_iceberg = PythonOperator(
        task_id           = "iceberg_loader",
        python_callable   = task_iceberg,
        execution_timeout = timedelta(minutes=15),
        doc_md            = "CREATE Iceberg tables + batch INSERT 15,875 rows via Trino",
    )

    t6_insight = PythonOperator(
        task_id         = "insight_agent",
        python_callable = task_insight,
        doc_md          = "Gold KPI tables + OpenAI LLM insights + Markdown report",
    )

    #Pipeline dependency chain
    t1_generate >> t2_ingest >> t3_governance >> t4_quality >> t5_iceberg >> t6_insight