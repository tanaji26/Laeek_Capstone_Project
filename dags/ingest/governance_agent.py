"""
governance_agent.py
--------------------
Enterprise Governance Agent

Responsibilities:
  1. Load each CSV from MinIO landing zone
  2. Enforce schema contract (column presence, dtypes) from schema YAMLs
  3. Mask PII columns (name → initials, email → hash, phone → redacted)
  4. Write governed (masked) Parquet files back to MinIO → s3://data/bronze/
  5. Build Silver layer (cleaned, enriched, joined) → s3://data/silver/
  6. Emit a compliance report to /data/logs/governance_report.json

Input  : s3://data/landing/<table>.csv      (raw, may contain PII)
Output : s3://data/bronze/<table>.parquet   (governed, PII-masked)
         s3://data/silver/<table>.parquet   (enriched, analytics-ready)
"""

import hashlib
import io
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
import numpy as np

from schema_loader import load_all_schemas, TableSchema
from pipeline_monitor import get_logger, StepTracker, log_metric, LOG_DIR

AGENT = "governance_agent"
logger = get_logger(AGENT)

# MinIO / S3 config
S3 = boto3.client(
    "s3",
    endpoint_url          = os.environ["MINIO_ENDPOINT"],
    aws_access_key_id     = os.environ["MINIO_ACCESS_KEY"],
    aws_secret_access_key = os.environ["MINIO_SECRET_KEY"],
    region_name           = "us-east-1",
)
BUCKET      = "data"
LANDING_PFX = "landing"
BRONZE_PFX  = "bronze"
SILVER_PFX  = "silver"
SCHEMAS_DIR = os.getenv("SCHEMAS_DIR", "schemas")


# PII masking functions

def _mask_name(value: str) -> str:
    """'Rahul Sharma' → 'R***S***'"""
    if pd.isna(value):
        return value
    parts = str(value).split()
    return " ".join(p[0] + "***" for p in parts)


def _mask_email(value: str) -> str:
    """SHA-256 hash of local part, domain retained for analytics."""
    if pd.isna(value):
        return value
    local, _, domain = str(value).partition("@")
    hashed = hashlib.sha256(local.encode()).hexdigest()[:12]
    return f"{hashed}@{domain}"


def _mask_phone(value: str) -> str:
    """'+91 98765 43210' → '+91 XXXXX XXXXX'"""
    if pd.isna(value):
        return value
    parts = str(value).split()
    masked = [parts[0]] + ["XXXXX"] * (len(parts) - 1)
    return " ".join(masked)


PII_MASKERS = {
    "name":  _mask_name,
    "email": _mask_email,
    "phone": _mask_phone,
}


# Schema enforcement

def enforce_schema(df: pd.DataFrame, schema: TableSchema, table_key: str) -> tuple[pd.DataFrame, list[str]]:
    """
    Coerce dtypes and drop/add columns to match schema contract.
    Returns (coerced_df, list_of_warnings).
    """
    warnings = []
    schema_cols = schema.column_names

    # Columns present in schema but missing from CSV
    missing = [c for c in schema_cols if c not in df.columns]
    if missing:
        warnings.append(f"Missing columns added as null: {missing}")
        for col in missing:
            df[col] = pd.NA

    # Columns in CSV but not in schema (drop them)
    extra = [c for c in df.columns if c not in schema_cols]
    if extra:
        warnings.append(f"Extra columns dropped: {extra}")
        df = df.drop(columns=extra)

    # Reorder to schema order
    df = df[schema_cols]

    # Dtype coercion
    dtype_map = schema.pandas_dtype_map
    date_cols = [c.name for c in schema.date_columns]

    for col, dtype in dtype_map.items():
        try:
            df[col] = df[col].astype(dtype)
        except Exception as e:
            warnings.append(f"dtype coercion failed for '{col}' → {dtype}: {e}")

    for col in date_cols:
        try:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        except Exception as e:
            warnings.append(f"date parse failed for '{col}': {e}")

    return df, warnings


def _read_bronze(table_name: str) -> pd.DataFrame:
    """Read a Parquet file from the bronze zone."""
    key = f"{BRONZE_PFX}/{table_name}.parquet"
    obj = S3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


def _write_silver(df: pd.DataFrame, table_name: str) -> str:
    """Upload a DataFrame as Parquet to the silver zone."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    key = f"{SILVER_PFX}/{table_name}.parquet"
    S3.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())
    return f"s3://{BUCKET}/{key}"


def build_silver() -> bool:
    """
    Build Silver layer from Bronze Parquet files.

    Silver tables produced:
      orders_enriched    — orders joined with customer + product dims;
                           adds delivery_days, order_year/month/quarter,
                           is_prime_active (at order time), price_tier
      customers_clean    — customers with prime_tenure_days,
                           is_prime_active_now, age_group
      products_clean     — products with margin_pct, price_tier
      feedback_enriched  — feedback joined with order + product info;
                           adds days_to_review, sentiment label
    """
    logger.info("Building Silver layer...", extra={"agent": AGENT, "step": "silver_init"})

    try:
        orders    = _read_bronze("orders_data")
        customers = _read_bronze("customers_data")
        products  = _read_bronze("products_data")
        feedback  = _read_bronze("feedback_data")
    except Exception as e:
        logger.error(f"Silver: failed to read bronze: {e}",
                     extra={"agent": AGENT, "step": "silver_init"})
        return False

    # ── Parse dates ───────────────────────────────────────────────────────────
    orders["order_date"]    = pd.to_datetime(orders["order_date"],    errors="coerce")
    orders["delivery_date"] = pd.to_datetime(orders["delivery_date"], errors="coerce")
    customers["prime_start_date"] = pd.to_datetime(customers["prime_start_date"], errors="coerce")
    customers["prime_end_date"]   = pd.to_datetime(customers["prime_end_date"],   errors="coerce")
    feedback["review_date"] = pd.to_datetime(feedback["review_date"], errors="coerce")

    # ── silver/orders_enriched ────────────────────────────────────────────────
    with StepTracker(logger, "silver_orders_enriched", AGENT):
        cust_dim = customers[[
            "customer_id", "customer_age", "customer_gender",
            "is_prime_customer", "prime_start_date", "prime_end_date"
        ]].copy()

        prod_dim = products[["product_id", "cost_price_per_unit", "retail_price_per_unit"]].copy()
        prod_dim["margin_pct"] = (
            (prod_dim["retail_price_per_unit"] - prod_dim["cost_price_per_unit"])
            / prod_dim["retail_price_per_unit"] * 100
        ).round(2)

        oe = orders.merge(cust_dim, on="customer_id", how="left")
        oe = oe.merge(prod_dim, on="product_id", how="left")

        oe["delivery_days"]   = (oe["delivery_date"] - oe["order_date"]).dt.days
        oe["order_year"]      = oe["order_date"].dt.year
        oe["order_month"]     = oe["order_date"].dt.month
        oe["order_quarter"]   = oe["order_date"].dt.quarter

        def _is_prime_active(row):
            if not row["is_prime_customer"]:
                return False
            if pd.isna(row["prime_start_date"]) or pd.isna(row["prime_end_date"]):
                return False
            return row["prime_start_date"] <= row["order_date"] <= row["prime_end_date"]

        oe["is_prime_active"] = oe.apply(_is_prime_active, axis=1)

        oe["price_tier"] = pd.cut(
            oe["final_amount"],
            bins=[0, 500, 2000, 10000, float("inf")],
            labels=["Budget", "Mid-Range", "Premium", "Luxury"]
        ).astype(str)

        path = _write_silver(oe, "orders_enriched")
        logger.info(f"Silver orders_enriched → {path} ({len(oe):,} rows)",
                    extra={"agent": AGENT, "step": "silver_orders_enriched"})

    # ── silver/customers_clean ────────────────────────────────────────────────
    with StepTracker(logger, "silver_customers_clean", AGENT):
        cc = customers.copy()
        today = pd.Timestamp.now().normalize()

        cc["prime_tenure_days"] = (
            (cc["prime_end_date"] - cc["prime_start_date"])
            .dt.days.fillna(0).astype(int)
        )
        cc["is_prime_active_now"] = (
            cc["is_prime_customer"] &
            cc["prime_start_date"].notna() &
            cc["prime_end_date"].notna() &
            (cc["prime_start_date"] <= today) &
            (cc["prime_end_date"]   >= today)
        )
        cc["age_group"] = pd.cut(
            cc["customer_age"],
            bins=[0, 25, 35, 45, 65],
            labels=["18-25", "26-35", "36-45", "46-65"]
        ).astype(str)

        path = _write_silver(cc, "customers_clean")
        logger.info(f"Silver customers_clean → {path} ({len(cc):,} rows)",
                    extra={"agent": AGENT, "step": "silver_customers_clean"})

    # ── silver/products_clean ─────────────────────────────────────────────────
    with StepTracker(logger, "silver_products_clean", AGENT):
        pc = products.copy()
        pc["margin_pct"] = (
            (pc["retail_price_per_unit"] - pc["cost_price_per_unit"])
            / pc["retail_price_per_unit"] * 100
        ).round(2)
        pc["price_tier"] = pd.cut(
            pc["retail_price_per_unit"],
            bins=[0, 500, 2000, 10000, float("inf")],
            labels=["Budget", "Mid-Range", "Premium", "Luxury"]
        ).astype(str)

        path = _write_silver(pc, "products_clean")
        logger.info(f"Silver products_clean → {path} ({len(pc):,} rows)",
                    extra={"agent": AGENT, "step": "silver_products_clean"})

    # ── silver/feedback_enriched ──────────────────────────────────────────────
    with StepTracker(logger, "silver_feedback_enriched", AGENT):
        fe = feedback.merge(
            orders[["order_id", "order_date", "product_id",
                    "product_name", "product_category", "customer_id"]],
            on="order_id", how="left"
        )
        fe["days_to_review"] = (fe["review_date"] - fe["order_date"]).dt.days
        fe["sentiment"] = pd.cut(
            fe["review_score"],
            bins=[0, 2, 3, 5],
            labels=["Negative", "Neutral", "Positive"]
        ).astype(str)

        path = _write_silver(fe, "feedback_enriched")
        logger.info(f"Silver feedback_enriched → {path} ({len(fe):,} rows)",
                    extra={"agent": AGENT, "step": "silver_feedback_enriched"})

    logger.info("Silver layer complete (4 tables)",
                extra={"agent": AGENT, "step": "silver_done"})
    return True


# Core agent logic

def read_csv_from_minio(source_file: str) -> pd.DataFrame:
    """Download a CSV from the landing zone and return as DataFrame."""
    key = f"{LANDING_PFX}/{source_file}"
    obj = S3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def write_parquet_to_minio(df: pd.DataFrame, table_name: str) -> str:
    """Upload a DataFrame as Parquet to the bronze zone."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    key = f"{BRONZE_PFX}/{table_name}.parquet"
    S3.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())
    return f"s3://{BUCKET}/{key}"


def process_table(table_key: str, schema: TableSchema) -> dict:
    """Run governance pipeline for one table. Returns a compliance record."""
    report = {
        "table":         schema.table_name,
        "source_file":   schema.source_file,
        "status":        "unknown",
        "rows_in":       0,
        "rows_out":      0,
        "pii_cols_masked": [],
        "schema_warnings": [],
        "output_path":   "",
        "ts":            datetime.now(timezone.utc).isoformat(),
    }

    with StepTracker(logger, f"read_{table_key}", AGENT):
        df = read_csv_from_minio(schema.source_file)
        report["rows_in"] = len(df)
        logger.info(f"Read {len(df)} rows from {schema.source_file}",
                    extra={"agent": AGENT, "step": f"read_{table_key}"})

    with StepTracker(logger, f"schema_enforce_{table_key}", AGENT):
        df, warnings = enforce_schema(df, schema, table_key)
        report["schema_warnings"] = warnings
        for w in warnings:
            logger.warning(w, extra={"agent": AGENT, "step": f"schema_enforce_{table_key}"})

    with StepTracker(logger, f"pii_mask_{table_key}", AGENT):
        masked_cols = []
        for col in schema.pii_columns:
            masker = PII_MASKERS.get(col.pii_type)
            if masker and col.name in df.columns:
                df[col.name] = df[col.name].apply(masker)
                masked_cols.append(f"{col.name}({col.pii_type})")
        report["pii_cols_masked"] = masked_cols
        logger.info(f"PII masked: {masked_cols}",
                    extra={"agent": AGENT, "step": f"pii_mask_{table_key}"})

    with StepTracker(logger, f"write_bronze_{table_key}", AGENT):
        output_path = write_parquet_to_minio(df, schema.table_name)
        report["rows_out"]     = len(df)
        report["output_path"]  = output_path
        report["status"]       = "compliant"
        logger.info(f"Written to {output_path}",
                    extra={"agent": AGENT, "step": f"write_bronze_{table_key}"})

    log_metric(AGENT, table_key, rows_in=report["rows_in"],
               rows_out=report["rows_out"], pii_masked=len(masked_cols))
    return report


def run() -> bool:
    logger.info("Governance Agent starting",
                extra={"agent": AGENT, "step": "init"})

    schemas      = load_all_schemas(schemas_dir=SCHEMAS_DIR)
    compliance   = []
    all_ok       = True

    # Process in dependency order
    order = ["products_data", "customers_data", "orders_data", "feedback_data"]

    for key in order:
        if key not in schemas:
            logger.warning(f"Schema key '{key}' not found, skipping.",
                           extra={"agent": AGENT, "step": key})
            continue
        try:
            record = process_table(key, schemas[key])
            compliance.append(record)
        except Exception as e:
            logger.error(f"Governance failed for '{key}': {e}",
                         extra={"agent": AGENT, "step": key})
            compliance.append({"table": key, "status": "failed", "error": str(e)})
            all_ok = False

    # Build Silver layer from the freshly written bronze tables
    silver_ok = build_silver()
    if not silver_ok:
        logger.warning("Silver layer build failed — continuing to compliance report",
                       extra={"agent": AGENT, "step": "silver_done"})
        all_ok = False

    # Write compliance report
    report_path = LOG_DIR / "governance_report.json"
    with open(report_path, "w") as f:
        json.dump({
            "agent":         AGENT,
            "ts":            datetime.now(timezone.utc).isoformat(),
            "tables":        compliance,
            "overall":       "compliant" if all_ok else "non_compliant",
            "silver_status": "complete" if silver_ok else "failed",
        }, f, indent=2, default=str)

    logger.info(f"Compliance report → {report_path}",
                extra={"agent": AGENT, "step": "report"})

    if not all_ok:
        raise Exception("Governance agent finished with errors — check compliance report.")
    return all_ok


if __name__ == "__main__":
    run()