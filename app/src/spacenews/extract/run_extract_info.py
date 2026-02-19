import os
import json
from datetime import datetime, timezone

import requests
import boto3

SPACEFLIGHT_BASE_URL = os.getenv("SPACEFLIGHT_BASE_URL", "https://api.spaceflightnewsapi.net/v4")
RUN_DATE = os.getenv("RUN_DATE")  # YYYY-MM-DD opcional

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin123")
S3_BUCKET = os.getenv("S3_BUCKET", "datalake")

def s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )

def run_date():
    if RUN_DATE:
        return RUN_DATE
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def main():
    rd = run_date()
    url = f"{SPACEFLIGHT_BASE_URL}/info"

    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()

    key = f"bronze/info/ingestion_date={rd}/info.json"
    body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")

    s3().put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/json")

    print(f"[INFO] wrote s3://{S3_BUCKET}/{key}")

if __name__ == "__main__":
    main()
