import boto3
import os
import sys
from pathlib import Path

# Config from environment
MINIO_ENDPOINT  = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS    = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET    = os.environ["MINIO_SECRET_KEY"]
BUCKET          = "data"
PREFIX          = "landing"          # s3://data/landing/<file>
DATA_DIR        = Path(os.getenv("DATA_DIR", "/data"))

CSV_FILES = [
    "orders_data.csv",
    "customer_data.csv",
    "products_data.csv",
    "feedback_data.csv",
]

# S3 client
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS,
    aws_secret_access_key=MINIO_SECRET,
    region_name="us-east-1",
)

def upload_csv(file_name: str) -> None:
    local_path = DATA_DIR / file_name
    s3_key     = f"{PREFIX}/{file_name}"

    if not local_path.exists():
        print(f"  [SKIP] {local_path} not found — was base_data_generation.py run?")
        return

    file_size = local_path.stat().st_size
    print(f"  Uploading {file_name} ({file_size:,} bytes) → s3://{BUCKET}/{s3_key}")
    s3.upload_file(str(local_path), BUCKET, s3_key)
    print(f"  ✓ {file_name} uploaded successfully")

def main():
    """
    Entry point for Airflow PythonOperator.
    Raises Exception on failure so Airflow marks the task as failed.
    """
    print("=" * 55)
    print("MinIO CSV Uploader — Landing Zone")
    print("=" * 55)
    print(f"Endpoint : {MINIO_ENDPOINT}")
    print(f"Bucket   : s3://{BUCKET}/{PREFIX}/")
    print(f"Source   : {DATA_DIR}")
    print()

    errors = []
    for csv_file in CSV_FILES:
        try:
            upload_csv(csv_file)
        except Exception as e:
            print(f"  [ERROR] Failed to upload {csv_file}: {e}")
            errors.append(csv_file)

    print()
    if errors:
        raise Exception(f"Upload failed for: {errors}")
    else:
        print(f"All {len(CSV_FILES)} files uploaded to s3://{BUCKET}/{PREFIX}/")

if __name__ == "__main__":
    main()