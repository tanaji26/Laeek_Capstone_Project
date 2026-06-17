"""
quality_agent.py
-----------------
Enterprise Quality & Validation Agent

Responsibilities:
  1. Read governed Parquet files from bronze zone
  2. Run a suite of DQ checks per table:
       - Null checks on NOT NULL columns
       - Primary key uniqueness
       - Referential integrity (FK checks)
       - Value range validation
       - Enum / allowed-value checks
       - Date logic checks (e.g. delivery_date >= order_date)
  3. Score each table (0–100) and flag rows that fail
  4. Write DQ report to /data/logs/quality_report.json
  5. Write bad-row CSVs to s3://data/bronze/quarantine/

Input  : s3://data/bronze/<table>.parquet
Output : DQ report JSON + quarantine CSVs
"""

import io
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import boto3
import pandas as pd

from schema_loader import load_all_schemas, TableSchema
from pipeline_monitor import get_logger, StepTracker, log_metric, LOG_DIR

AGENT  = "quality_agent"
logger = get_logger(AGENT)

S3 = boto3.client(
    "s3",
    endpoint_url          = os.environ["MINIO_ENDPOINT"],
    aws_access_key_id     = os.environ["MINIO_ACCESS_KEY"],
    aws_secret_access_key = os.environ["MINIO_SECRET_KEY"],
    region_name           = "us-east-1",
)
BUCKET       = "data"
BRONZE_PFX   = "bronze"
QUARANTINE   = "bronze/quarantine"
SCHEMAS_DIR  = os.getenv("SCHEMAS_DIR", "schemas")

# Allowed value sets
ALLOWED = {
    "order_status":    {"Delivered", "Cancelled", "Returned"},
    "payment_method":  {"UPI", "Cash on Delivery", "Credit Card",
                        "Debit Card", "Net Banking", "EMI"},
    "payment_status":  {"Paid", "Refunded"},
    "customer_region": {"North", "South", "East", "West", "Central"},
    "customer_gender": {"Male", "Female", "Other"},
    "product_category":{"Electronics", "Apparel", "Grocery", "Furniture", "Sports"},
    "currency":        {"INR"},
}

#Check result builder
def _result(check: str, passed: bool, rows_failed: int, total: int, detail: str = "") -> dict:
    return {
        "check":       check,
        "passed":      passed,
        "rows_failed": rows_failed,
        "total_rows":  total,
        "fail_rate":   round(rows_failed / total, 4) if total else 0,
        "detail":      detail,
    }

#Individual check functions

def check_nulls(df: pd.DataFrame, schema: TableSchema) -> tuple[list[dict], pd.Index]:
    results    = []
    bad_idx    = pd.Index([], dtype=int)
    for col in schema.non_nullable_columns:
        if col.name not in df.columns:
            continue
        mask = df[col.name].isna()
        bad  = df.index[mask]
        bad_idx = bad_idx.union(bad)
        results.append(_result(
            f"null_check::{col.name}", not mask.any(),
            int(mask.sum()), len(df),
            f"Column '{col.name}' must not be null"
        ))
    return results, bad_idx


def check_primary_key(df: pd.DataFrame, schema: TableSchema) -> tuple[list[dict], pd.Index]:
    pk  = schema.primary_key
    if pk not in df.columns:
        return [], pd.Index([])
    dupes = df[df.duplicated(subset=[pk], keep=False)].index
    return [_result(
        f"pk_unique::{pk}", len(dupes) == 0,
        len(dupes), len(df),
        f"Primary key '{pk}' must be unique"
    )], dupes

def check_enums(df: pd.DataFrame) -> tuple[list[dict], pd.Index]:
    results = []
    bad_idx = pd.Index([], dtype=int)
    for col, allowed in ALLOWED.items():
        if col not in df.columns:
            continue
        mask = ~df[col].isin(allowed) & df[col].notna()
        bad  = df.index[mask]
        bad_idx = bad_idx.union(bad)
        results.append(_result(
            f"enum_check::{col}", not mask.any(),
            int(mask.sum()), len(df),
            f"Column '{col}' must be one of {sorted(allowed)}"
        ))
    return results, bad_idx

def check_ranges(df: pd.DataFrame) -> tuple[list[dict], pd.Index]:
    """Numeric range checks specific to retail domain."""
    checks_cfg = [
        ("customer_age",    lambda s: (s >= 18) & (s <= 65)),
        ("review_score",    lambda s: s.between(1, 5)),
        ("order_qty",       lambda s: s >= 1),
        ("unit_price",      lambda s: s > 0),
        ("final_amount",    lambda s: s >= 0),
        ("discount_pct",    lambda s: s.between(0, 1)),
        ("cost_price_per_unit",   lambda s: s > 0),
        ("retail_price_per_unit", lambda s: s > 0),
    ]
    results = []
    bad_idx = pd.Index([], dtype=int)
    for col, rule in checks_cfg:
        if col not in df.columns:
            continue
        numeric = pd.to_numeric(df[col], errors="coerce")
        mask    = ~rule(numeric) & numeric.notna()
        bad     = df.index[mask]
        bad_idx = bad_idx.union(bad)
        results.append(_result(
            f"range_check::{col}", not mask.any(),
            int(mask.sum()), len(df),
            f"Column '{col}' failed range constraint"
        ))
    return results, bad_idx

def check_date_logic(df: pd.DataFrame) -> list[dict]:
    """Cross-column date checks"""
    results = []
    if "order_date" in df.columns and "delivery_date" in df.columns:
        od = pd.to_datetime(df["order_date"], errors="coerce")
        dd = pd.to_datetime(df["delivery_date"], errors="coerce")
        mask = (dd < od) & od.notna() & dd.notna()
        results.append(_result(
            "date_logic::delivery>=order", not mask.any(),
            int(mask.sum()), len(df),
            "delivery_date must be >= order_date"
        ))
    if "prime_start_date" in df.columns and "prime_end_date" in df.columns:
        ps = pd.to_datetime(df["prime_start_date"], errors="coerce")
        pe = pd.to_datetime(df["prime_end_date"], errors="coerce")
        mask = (pe <= ps) & ps.notna() & pe.notna()
        results.append(_result(
            "date_logic::prime_end>prime_start", not mask.any(),
            int(mask.sum()), len(df),
            "prime_end_date must be > prime_start_date"
        ))
    return results

def check_referential_integrity(
    child_df: pd.DataFrame, child_col: str,
    parent_df: pd.DataFrame, parent_col: str,
    label: str
) -> tuple[list[dict], pd.Index]:
    if child_col not in child_df.columns or parent_col not in parent_df.columns:
        return [], pd.Index([])
    valid   = set(parent_df[parent_col].dropna())
    mask    = ~child_df[child_col].isin(valid) & child_df[child_col].notna()
    bad_idx = child_df.index[mask]
    return [_result(
        f"fk_check::{label}", not mask.any(),
        int(mask.sum()), len(child_df),
        f"'{child_col}' references missing values in parent '{parent_col}'"
    )], bad_idx

# DQ score

def compute_dq_score(check_results: list[dict]) -> float:
    """
    Weighted DQ score 0–100.
    Failed checks reduce score proportionally to their fail_rate.
    """
    if not check_results:
        return 100.0
    penalty = sum(r["fail_rate"] for r in check_results if not r["passed"])
    score   = max(0.0, 100.0 - (penalty / len(check_results)) * 100)
    return round(score, 2)

# MinIO helpers

def read_parquet(table_name: str) -> pd.DataFrame:
    key = f"{BRONZE_PFX}/{table_name}.parquet"
    obj = S3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))

def write_quarantine(df: pd.DataFrame, table_name: str) -> str:
    if df.empty:
        return ""
    buffer = io.BytesIO()
    df.to_csv(buffer, index=False)
    buffer.seek(0)
    key = f"{QUARANTINE}/{table_name}_bad_rows.csv"
    S3.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())
    return f"s3://{BUCKET}/{key}"

# Per-table orchestration

def validate_table(
    table_key: str,
    schema: TableSchema,
    ref_frames: dict[str, pd.DataFrame]
) -> dict:
    with StepTracker(logger, f"read_{table_key}", AGENT):
        df = read_parquet(schema.table_name)
        logger.info(f"Loaded {len(df)} rows for {schema.table_name}",
                    extra={"agent": AGENT, "step": f"read_{table_key}"})

    all_checks  = []
    all_bad_idx = pd.Index([], dtype=int)

    with StepTracker(logger, f"checks_{table_key}", AGENT):
        r, b = check_nulls(df, schema);         all_checks += r; all_bad_idx = all_bad_idx.union(b)
        r, b = check_primary_key(df, schema);   all_checks += r; all_bad_idx = all_bad_idx.union(b)
        r, b = check_enums(df);                 all_checks += r; all_bad_idx = all_bad_idx.union(b)
        r, b = check_ranges(df);                all_checks += r; all_bad_idx = all_bad_idx.union(b)
        date_r = check_date_logic(df);          all_checks += date_r

        # FK checks
        if table_key == "orders_data":
            if "products_data" in ref_frames:
                r, b = check_referential_integrity(df, "product_id", ref_frames["products_data"], "product_id", "orders→products")
                all_checks += r; all_bad_idx = all_bad_idx.union(b)
            if "customers_data" in ref_frames:
                r, b = check_referential_integrity(df, "customer_id", ref_frames["customers_data"], "customer_id", "orders→customers")
                all_checks += r; all_bad_idx = all_bad_idx.union(b)
        if table_key == "feedback_data" and "orders_data" in ref_frames:
            r, b = check_referential_integrity(df, "order_id", ref_frames["orders_data"], "order_id", "feedback→orders")
            all_checks += r; all_bad_idx = all_bad_idx.union(b)

    score = compute_dq_score(all_checks)
    bad_rows = df.loc[df.index.isin(all_bad_idx)]
    quarantine_path = write_quarantine(bad_rows, schema.table_name)

    failed_checks = [c for c in all_checks if not c["passed"]]
    log_metric(AGENT, table_key, dq_score=score,
               total_checks=len(all_checks), failed_checks=len(failed_checks),
               bad_rows=len(bad_rows))

    logger.info(
        f"DQ score {schema.table_name}: {score}/100 "
        f"({len(failed_checks)}/{len(all_checks)} checks failed, {len(bad_rows)} bad rows)",
        extra={"agent": AGENT, "step": f"score_{table_key}"}
    )

    return {
        "table":           schema.table_name,
        "rows_validated":  len(df),
        "dq_score":        score,
        "total_checks":    len(all_checks),
        "failed_checks":   len(failed_checks),
        "bad_rows":        len(bad_rows),
        "quarantine_path": quarantine_path,
        "checks":          all_checks,
        "ts":              datetime.now(timezone.utc).isoformat(),
    }

# Entry point

def run() -> bool:
    logger.info("Quality Agent starting", extra={"agent": AGENT, "step": "init"})

    schemas  = load_all_schemas(schemas_dir=SCHEMAS_DIR)
    order    = ["products_data", "customers_data", "orders_data", "feedback_data"]
    ref_frames: dict[str, pd.DataFrame] = {}
    results  = []
    all_ok   = True

    for key in order:
        if key not in schemas:
            continue
        try:
            table_result  = validate_table(key, schemas[key], ref_frames)
            results.append(table_result)
            # Cache for FK checks downstream
            ref_frames[key] = read_parquet(schemas[key].table_name)
        except Exception as e:
            logger.error(f"Quality check failed for '{key}': {e}",
                         extra={"agent": AGENT, "step": key})
            results.append({"table": key, "status": "error", "error": str(e)})
            all_ok = False

    overall_score = round(
        sum(r.get("dq_score", 0) for r in results if "dq_score" in r) /
        max(len([r for r in results if "dq_score" in r]), 1), 2
    )

    report = {
        "agent":         AGENT,
        "ts":            datetime.now(timezone.utc).isoformat(),
        "overall_score": overall_score,
        "tables":        results,
    }
    report_path = LOG_DIR / "quality_report.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    logger.info(f"Quality report → {report_path} | Overall DQ score: {overall_score}/100",
                extra={"agent": AGENT, "step": "report", "overall_score": overall_score})

    if not all_ok:
        raise Exception(f"Quality agent finished with errors. Overall DQ score: {overall_score}/100")
    return all_ok

if __name__ == "__main__":
    run()