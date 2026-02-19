import os
import json
import boto3
from botocore.config import Config

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "datalake")

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"}
        )
    )

def write_bronze(endpoint: str, records: list, run_date: str):
    if not records:
        return

    key = f"bronze/{endpoint}/ingestion_date={run_date}/data.jsonl"
    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in records) + "\n"

    s3 = s3_client()
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode("utf-8"))
