import os
import glob
import boto3
from urllib.parse import urlparse

from spacenews.config import S3_BUCKET

import json
import tempfile
from pathlib import Path

import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit, row_number, year, month
from pyspark.sql.window import Window

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin123")
S3_BUCKET = os.getenv("S3_BUCKET", "datalake")
import os

from spacenews.config import S3_BUCKET

def s3_client():
    endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    # boto/botocore a veces valida hostnames; evita underscores en el hostname
    parsed = urlparse(endpoint)
    if "_" in parsed.hostname:
        raise ValueError(
            f"S3_ENDPOINT tiene hostname inválido para boto3 (underscore): {endpoint}. "
            f"Usa un alias tipo http://minio:9000"
        )

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )


def upload_silver(endpoint: str, run_date: str, local_silver_dir: str):
    """
    Sube el folder parquet generado por Spark a:
    silver/{endpoint}/ingestion_date={run_date}/...
    """
    prefix = f"silver/{endpoint}/ingestion_date={run_date}/"
    client = s3_client()

    for fp in glob.glob(os.path.join(local_silver_dir, "**"), recursive=True):
        if os.path.isdir(fp):
            continue
        rel = os.path.relpath(fp, local_silver_dir)
        key = prefix + rel
        with open(fp, "rb") as f:
            client.put_object(Bucket=S3_BUCKET, Key=key, Body=f.read())


def s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )

from botocore.exceptions import ClientError

def download_bronze(endpoint, run_date, bronze_file):
    key = f"bronze/{endpoint}/ingestion_date={run_date}/data.jsonl"

    try:
        obj = s3_client().get_object(Bucket=S3_BUCKET, Key=key)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            print(f"[WARN] Bronze not found, skipping endpoint={endpoint} date={run_date}")
            return False
        raise

    with open(bronze_file, "wb") as f:
        f.write(obj["Body"].read())

    return True


def upload_dir_to_minio(local_dir: str, remote_prefix: str) -> None:
    client = s3()
    local_dir_path = Path(local_dir)
    for p in local_dir_path.rglob("*"):
        if p.is_file():
            rel = p.relative_to(local_dir_path).as_posix()
            key = f"{remote_prefix}/{rel}"
            client.upload_file(str(p), S3_BUCKET, key)

def get_spark():
    return (
        SparkSession.builder
        .appName("spacenews_silver_clean")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

import os

def process_endpoint(spark, endpoint: str, run_date: str):
    # 1) Define rutas primero
    tmp_dir = f"/tmp/spacenews_silver_{run_date}"
    os.makedirs(tmp_dir, exist_ok=True)

    bronze_file = f"{tmp_dir}/{endpoint}.jsonl"   # <-- DEFINIR ANTES
    local_silver = f"{tmp_dir}/{endpoint}.parquet"

    # 2) Descargar bronze (y si no existe, skip)
    ok = download_bronze(endpoint, run_date, bronze_file)
    if not ok:
        print(f"[WARN] Skipping endpoint={endpoint} run_date={run_date} (no bronze file)")
        return

    # 3) Si existe, ya sigues con tu lógica normal (leer, limpiar, escribir silver)
    df = spark.read.json(bronze_file)

    # ... tu limpieza actual ...
    # df_clean = ...

    df.write.mode("overwrite").parquet(local_silver)
    upload_silver(endpoint, run_date, local_silver)


def main():
    run_date = os.getenv("RUN_DATE")
    if not run_date:
        raise RuntimeError("Missing RUN_DATE (YYYY-MM-DD)")

    spark = get_spark()
    for ep in ["articles", "blogs", "reports"]:
        process_endpoint(spark, ep, run_date)
    spark.stop()

if __name__ == "__main__":
    main()
