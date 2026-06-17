"""
create_iceberg_tables.py
------------------------
Single-script pipeline:
  1. Load YAML schemas via schema_loader
  2. Wait for Trino to be ready + ACTIVE
  3. CREATE SCHEMA IF NOT EXISTS in Iceberg (file catalog)
  4. CREATE TABLE IF NOT EXISTS for all four tables
  5. Read governed Parquet files from MinIO bronze zone
  6. Batch-INSERT rows into each Iceberg table via Trino
  7. Verify final row counts

Flow:
  schemas/*.yaml
      → CREATE TABLE IF NOT EXISTS in iceberg.landing.*
  s3://data/bronze/<table>.parquet
      → pandas DataFrame
      → batch INSERT INTO iceberg.landing.<table>

Catalog type: file  (no external metastore required)
"""

import io
import math
import os
import sys
import time
import urllib.request

import boto3
import pandas as pd
import trino
from schema_loader import load_all_schemas, TableSchema
import numpy as np
from datetime import date, datetime
import pandas as pd

# Trino
TRINO_HOST    = os.getenv("TRINO_HOST", "trino-coordinator")
TRINO_PORT    = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER    = os.getenv("TRINO_USER", "trino")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA  = os.getenv("TRINO_SCHEMA", "landing")
SCHEMAS_DIR   = os.getenv("SCHEMAS_DIR", "schemas")

# ── MinIO / S3 ────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS   = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET   = os.environ["MINIO_SECRET_KEY"]
BUCKET         = "data"
BRONZE_PFX     = "bronze"

# ── Loader tuning ─────────────────────────────────────────────────────────────
BATCH_SIZE = 500   # rows per INSERT statement

# FK dependency order: parents first
TABLE_ORDER = ["products_data", "customers_data", "orders_data", "feedback_data"]

# ── S3 client ─────────────────────────────────────────────────────────────────
S3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS,
    aws_secret_access_key=MINIO_SECRET,
    region_name="us-east-1",
)

# Connection helpers

def get_connection(with_catalog: bool = False):
    """
    Return a fresh Trino connection.

    with_catalog=False  →  bare connection (used for DDL / schema creation
                            to avoid pre-flight catalog checks)
    with_catalog=True   →  connection pre-scoped to TRINO_CATALOG.TRINO_SCHEMA
                            (used for INSERT / SELECT queries)
    """
    kwargs = dict(host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER)
    if with_catalog:
        kwargs["catalog"] = TRINO_CATALOG
        kwargs["schema"]  = TRINO_SCHEMA
    return trino.dbapi.connect(**kwargs)

# Phase 1 — Wait for Trino

def wait_for_trino(retries: int = 30, delay: int = 10):
    """Poll until Trino accepts queries."""
    print(f"  Waiting for Trino at {TRINO_HOST}:{TRINO_PORT}...")
    for attempt in range(1, retries + 1):
        try:
            conn = get_connection()
            cur  = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
            print(f"  ✓ Trino ready (attempt {attempt})")
            return
        except Exception as e:
            print(f"  Attempt {attempt}/{retries}: {e}")
            if attempt < retries:
                time.sleep(delay)
    print("[ERROR] Trino did not become ready. Aborting.")
    raise Exception(f"Trino at {TRINO_HOST}:{TRINO_PORT} did not become ready after {retries} attempts.")


def wait_for_trino_active(retries: int = 20, delay: int = 10):
    """
    Wait until Trino reports ACTIVE state.
    The /v1/info/state endpoint returns: STARTING | ACTIVE
    """
    url = f"http://{TRINO_HOST}:{TRINO_PORT}/v1/info/state"
    print("  Waiting for Trino to reach ACTIVE state...")
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=5) as r:
                state = r.read().decode().strip().strip('"')
                print(f"  Attempt {attempt}: Trino state = {state}")
                if state == "ACTIVE":
                    print("  ✓ Trino is ACTIVE")
                    return
        except Exception as e:
            print(f"  Attempt {attempt}/{retries}: {e}")
        time.sleep(delay)
    print("[WARN] Trino did not reach ACTIVE state — proceeding anyway")


# Phase 2 — Schema + Table DDL

def ensure_schema(cursor):
    """Create the Iceberg schema with S3 location for file catalog."""
    schema_location = f"s3://data/warehouse/{TRINO_SCHEMA}/"
    ddl = (
        f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.{TRINO_SCHEMA} "
        f"WITH (location = '{schema_location}')"
    )
    print(f"  Creating schema: {TRINO_CATALOG}.{TRINO_SCHEMA}")
    print(f"  Location: {schema_location}")
    cursor.execute(ddl)
    try:
        cursor.fetchall()
    except Exception:
        pass
    print(f"  ✓ Schema ready")


def build_ddl(schema: TableSchema) -> str:
    col_defs  = schema.iceberg_ddl_columns()
    col_block = ",\n    ".join(col_defs)
    s3_loc    = f"s3://data/warehouse/{TRINO_SCHEMA}/{schema.table_name}/"
    return (
        f"CREATE TABLE IF NOT EXISTS "
        f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{schema.table_name} (\n"
        f"    {col_block}\n"
        f")\n"
        f"WITH (\n"
        f"    format   = 'PARQUET',\n"
        f"    location = '{s3_loc}'\n"
        f")"
    )


def drop_warehouse_data():
    """
    Wipe all existing warehouse Parquet/metadata files from MinIO
    so that Iceberg CREATE TABLE always starts from a clean S3 location.
    Called once at the start of main() before any DDL.
    """
    try:
        paginator = S3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=BUCKET, Prefix="warehouse/")
        deleted = 0
        for page in pages:
            for obj in page.get("Contents", []):
                S3.delete_object(Bucket=BUCKET, Key=obj["Key"])
                deleted += 1
        print(f"  ✓ Warehouse data cleared ({deleted} objects removed)")
    except Exception as e:
        print(f"  Warehouse clear skipped: {e}")


def create_table(schema: TableSchema) -> bool:
    """
    DROP the table if it exists (clears stale catalog entry),
    then CREATE it fresh. This guarantees catalog and S3 are in sync
    on every pipeline run, regardless of prior state.
    """
    ddl_drop   = (
        f"DROP TABLE IF EXISTS "
        f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{schema.table_name}"
    )
    ddl_create = build_ddl(schema)
    print(f"\n  Table   : {TRINO_CATALOG}.{TRINO_SCHEMA}.{schema.table_name}")
    print(f"  Columns : {len(schema.columns)}")
    try:
        conn   = get_connection()          # bare connection for DDL
        cursor = conn.cursor()

        # Step 1: Drop stale catalog entry (no-op if table doesn't exist)
        cursor.execute(ddl_drop)
        try:
            cursor.fetchall()
        except Exception:
            pass
        print(f"  Dropped (if existed)")

        # Step 2: Create fresh on clean S3 location
        cursor.execute(ddl_create)
        try:
            cursor.fetchall()
        except Exception:
            pass
        print(f"  ✓ Created fresh")
        return True
    except Exception as e:
        print(f"  [ERROR] {schema.table_name}: {e}")
        return False


# Phase 3 — Load data  (Bronze Parquet → Iceberg via Trino INSERT)

def read_parquet(table_name: str) -> pd.DataFrame:
    key = f"{BRONZE_PFX}/{table_name}.parquet"
    print(f"  Reading s3://{BUCKET}/{key} ...")
    obj = S3.get_object(Bucket=BUCKET, Key=key)
    df  = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    print(f"  Loaded {len(df):,} rows, {len(df.columns)} columns")
    return df


def format_value(val) -> str:
    """Format a Python/numpy/pandas value as a Trino SQL literal."""
    # NULL
    if val is None:
        return "NULL"
    if val is pd.NaT:
        return "NULL"
    if isinstance(val, float) and math.isnan(val):
        return "NULL"

    # Boolean — before int check (numpy.bool_ is not subclass of bool)
    if isinstance(val, (bool, np.bool_)):
        return "TRUE" if val else "FALSE"

    # Integer
    if isinstance(val, (int, np.integer)):
        return str(int(val))

    # Float
    if isinstance(val, (float, np.floating)):
        return str(float(val))

    # Date / Timestamp → Trino DATE literal
    if isinstance(val, (datetime, pd.Timestamp)):
        return f"DATE '{pd.Timestamp(val).strftime('%Y-%m-%d')}'"
    if isinstance(val, date):
        return f"DATE '{val.isoformat()}'"

    # String fallback
    return "'" + str(val).replace("'", "''") + "'"


def build_insert_batch(table: str, columns: list[str], rows: list) -> str:
    col_list   = ", ".join(f'"{c}"' for c in columns)
    value_rows = []
    for row in rows:
        vals = ", ".join(format_value(v) for v in row)
        value_rows.append(f"({vals})")
    values_block = ",\n  ".join(value_rows)
    return (
        f"INSERT INTO {TRINO_CATALOG}.{TRINO_SCHEMA}.{table} ({col_list})\n"
        f"VALUES\n  {values_block}"
    )


def truncate_table(conn, table: str):
    """Delete all rows before reload (idempotent re-run support)."""
    cur = conn.cursor()
    try:
        cur.execute(f"DELETE FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{table}")
        cur.fetchall()
        print(f"  Truncated existing rows")
    except Exception as e:
        print(f"  Truncate skipped (table empty or error): {e}")


def load_table(table_name: str) -> bool:
    print(f"\n{'='*55}")
    print(f"  Loading: {TRINO_CATALOG}.{TRINO_SCHEMA}.{table_name}")
    print(f"{'='*55}")

    try:
        df = read_parquet(table_name)
    except Exception as e:
        print(f"  [ERROR] Cannot read parquet: {e}")
        return False

    if df.empty:
        print(f"  [SKIP] No data to load")
        return True

    # Replace pd.NA / NaT with None for SQL
    df = df.where(pd.notnull(df), None)

    columns    = list(df.columns)
    total_rows = len(df)
    n_batches  = math.ceil(total_rows / BATCH_SIZE)

    conn = get_connection(with_catalog=True)
    truncate_table(conn, table_name)

    inserted = 0
    for batch_num in range(n_batches):
        start = batch_num * BATCH_SIZE
        end   = min(start + BATCH_SIZE, total_rows)
        rows  = [
            tuple(row)
            for row in df.iloc[start:end].itertuples(index=False, name=None)
        ]
        try:
            sql = build_insert_batch(table_name, columns, rows)
            cur = conn.cursor()
            cur.execute(sql)
            cur.fetchall()
            inserted += len(rows)
            print(
                f"  Batch {batch_num+1}/{n_batches}: "
                f"inserted rows {start+1}–{end} ({inserted:,}/{total_rows:,})"
            )
        except Exception as e:
            print(f"  [ERROR] Batch {batch_num+1} failed: {e}")
            return False

    print(f"  ✓ {inserted:,} rows loaded into {table_name}")
    return True

# Phase 4 — Verification

def verify_counts(tables: list[str]):
    print(f"\n{'='*55}")
    print("  Row count verification")
    print(f"{'='*55}")
    conn = get_connection(with_catalog=True)
    for table in tables:
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT COUNT(*) FROM {TRINO_CATALOG}.{TRINO_SCHEMA}.{table}"
            )
            count = cur.fetchone()[0]
            print(f"  {table:<25} {count:>8,} rows")
        except Exception as e:
            print(f"  {table:<25} ERROR: {e}")


# Entry point

def main():
    """
    Entry point for Airflow PythonOperator.
    Raises Exception on failure so Airflow marks the task as failed.
    """
    print("=" * 60)
    print("  Iceberg Table Creator + Loader  (catalog type: file)")
    print("=" * 60)

    # Load schemas
    print("\n>>> [1/4] Loading schemas...")
    try:
        all_schemas = load_all_schemas(schemas_dir=SCHEMAS_DIR)
    except Exception as e:
        raise Exception(f"Schema load failed: {e}")
    print(f"  Loaded: {list(all_schemas.keys())}")

    # Wait for Trino
    print("\n>>> [2/4] Waiting for Trino...")
    wait_for_trino()
    wait_for_trino_active()

    # Wipe stale warehouse data so CREATE TABLE always gets a clean S3 location
    print("\n>>> [2b/4] Clearing stale warehouse data from MinIO...")
    drop_warehouse_data()

    # Create schema + tables
    print("\n>>> [3/4] Creating Iceberg schema and tables...")
    try:
        conn   = get_connection()
        cursor = conn.cursor()
        ensure_schema(cursor)
    except Exception as e:
        raise Exception(f"Schema creation failed: {e}")

    ddl_errors = []
    for key in TABLE_ORDER:
        if key not in all_schemas:
            print(f"  [SKIP] '{key}' not found in schemas")
            continue
        if not create_table(all_schemas[key]):
            ddl_errors.append(key)

    if ddl_errors:
        raise Exception(f"Table creation failed for: {ddl_errors}")
    print(f"\n  ✓ All tables created in {TRINO_CATALOG}.{TRINO_SCHEMA}")

    # Load data
    print("\n>>> [4/4] Loading data (Bronze → Iceberg)...")
    load_errors = []
    for table in TABLE_ORDER:
        if not load_table(table):
            load_errors.append(table)

    verify_counts(TABLE_ORDER)

    if load_errors:
        raise Exception(f"Data load failed for tables: {load_errors}")

    print("[DONE] All tables created and loaded successfully.")


if __name__ == "__main__":
    main()