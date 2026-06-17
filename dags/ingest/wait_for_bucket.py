import boto3, os, sys

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["MINIO_ENDPOINT"],
    aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
    aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    region_name="us-east-1",
)
buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
sys.exit(0 if "warehouse" in buckets else 1)
