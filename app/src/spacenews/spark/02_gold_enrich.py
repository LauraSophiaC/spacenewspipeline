import os
import re
import tempfile
from pathlib import Path
from typing import List

import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, regexp_replace, split, explode, length, trim,
    to_date, date_format, count as f_count, to_timestamp, lit, row_number,
    collect_list, to_json
)
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf

# ----------------------------
# MinIO / S3 config
# ----------------------------
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin123")
S3_BUCKET = os.getenv("S3_BUCKET", "datalake")

# ----------------------------
# Simple NLP config
# ----------------------------
STOPWORDS = set("""
a an the and or for to of in on at with by from is are was were be been being
this that these those as it its into over under about after before
""".split())

TOPIC_RULES = [
    ("launch",   ["launch", "rocket", "liftoff", "falcon", "starship", "booster"]),
    ("science",  ["telescope", "nasa", "esa", "james webb", "jwst", "mars", "moon"]),
    ("business", ["funding", "acquisition", "ipo", "market", "revenue", "contract"]),
    ("policy",   ["regulation", "policy", "government", "congress", "law", "defense"]),
]

# ----------------------------
# S3 helpers
# ----------------------------
def s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )

def list_keys(prefix: str) -> List[str]:
    client = s3()
    keys = []
    token = None
    while True:
        kwargs = dict(Bucket=S3_BUCKET, Prefix=prefix, MaxKeys=1000)
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        for o in resp.get("Contents", []):
            keys.append(o["Key"])
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys

def download_prefix(prefix: str, local_dir: str) -> str:
    client = s3()
    keys = list_keys(prefix)
    if not keys:
        raise RuntimeError(f"No objects found for prefix: {prefix}")

    for key in keys:
        rel = key[len(prefix):].lstrip("/")
        target = Path(local_dir) / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        client.download_file(S3_BUCKET, key, str(target))
    return local_dir

def upload_dir(local_dir: str, remote_prefix: str) -> None:
    client = s3()
    base = Path(local_dir)
    for p in base.rglob("*"):
        if p.is_file():
            rel = p.relative_to(base).as_posix()
            key = f"{remote_prefix}/{rel}"
            client.upload_file(str(p), S3_BUCKET, key)

# ----------------------------
# Spark helpers
# ----------------------------
def get_spark():
    return (
        SparkSession.builder
        .appName("spacenews_gold_enrich")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

def topic_expr(text_col):
    # encadena when(...) por reglas, si no coincide => other
    from pyspark.sql.functions import when, lit
    expr = None
    for topic, keywords in TOPIC_RULES:
        cond = None
        for kw in keywords:
            c = lower(text_col).contains(kw)
            cond = c if cond is None else (cond | c)
        if expr is None:
            expr = when(cond, lit(topic))
        else:
            expr = expr.when(cond, lit(topic))
    return expr.otherwise(lit("other"))

def normalize_for_union(df):
    """
    Normaliza columnas conflictivas entre endpoints.
    Algunos endpoints traen authors/events/launches como array<string>
    y otros como array<struct>. Para poder unir, los convertimos a JSON string.
    """
    for c in ["authors", "events", "launches"]:
        if c in df.columns:
            df = df.withColumn(c, to_json(col(c)))
    return df

def extract_entities(title: str):
    if not title:
        return []
    words = re.findall(r"\b[A-Z][A-Za-z]{2,}\b|\b[A-Z]{2,}\b", title)
    seen = set()
    out = []
    for w in words:
        if w not in seen:
            seen.add(w)
            out.append(w)
    return out[:10]

# ----------------------------
# Main
# ----------------------------
def main():
    spark = get_spark()

    tmp = tempfile.mkdtemp(prefix="spacenews_gold_")
    local_silver_root = os.path.join(tmp, "silver")

    # Descarga TODO silver/ desde MinIO a local tmp
    download_prefix("silver/", local_silver_root)

    # Lee por endpoint separado para evitar conflicto de estructuras
    paths = [
        os.path.join(local_silver_root, "articles"),
        os.path.join(local_silver_root, "blogs"),
        os.path.join(local_silver_root, "reports"),
    ]

    dfs = []
    for p in paths:
        if os.path.exists(p):
            d = spark.read.parquet(p)
            d = normalize_for_union(d)
            dfs.append(d)

    if not dfs:
        raise RuntimeError("No silver data found under downloaded silver/")

    # Union robusto (columnas faltantes => null)
    df = dfs[0]
    for other in dfs[1:]:
        df = df.unionByName(other, allowMissingColumns=True)

    # --- Enrichment ---
    # texto base
    title_clean = trim(regexp_replace(lower(col("title")), r"[^a-z0-9\s]", " "))
    summary_clean = trim(regexp_replace(lower(col("summary")), r"[^a-z0-9\s]", " "))
    full_text = trim(title_clean + lit(" ") + summary_clean)

    # tokens
    tokens = split(full_text, r"\s+")
    df_kw = df.withColumn("token", explode(tokens)).withColumn("token", trim(col("token")))
    df_kw = df_kw.filter((length(col("token")) >= 3))
    df_kw = df_kw.filter(~col("token").isin(list(STOPWORDS)))

    # top keywords por id
    kw_counts = df_kw.groupBy("id", "token").agg(f_count("*").alias("cnt"))
    w = Window.partitionBy("id").orderBy(col("cnt").desc(), col("token"))
    kw_top = kw_counts.withColumn("rn", row_number().over(w)).filter(col("rn") <= 10)
    kw_list = kw_top.groupBy("id").agg(collect_list(col("token")).alias("keyword_list"))

    # entities UDF
    entities_udf = udf(extract_entities, ArrayType(StringType()))

    # columnas de tiempo (si no existen ya como ts)
    # (en tu silver deberían existir published_at_ts / updated_at_ts)
    df_enriched = (
        df.join(kw_list, on="id", how="left")
          .withColumn("topic", topic_expr(col("title")))
          .withColumn("published_at_ts",
                      col("published_at_ts").cast("timestamp")
                      if "published_at_ts" in df.columns else to_timestamp(col("published_at")))
          .withColumn("published_date", to_date(col("published_at_ts")))
          .withColumn("published_month_str", date_format(col("published_at_ts"), "yyyy-MM"))
          .withColumn("entities", entities_udf(col("title")))
    )

    # --- Trends ---
    trends_daily = df_enriched.groupBy("published_date", "topic").agg(f_count("*").alias("items"))
    trends_monthly = df_enriched.groupBy("published_month_str", "topic").agg(f_count("*").alias("items"))

    # --- Sources activity (daily/monthly) ---
    sources_daily = (
        df_enriched.groupBy("published_date", "news_site", "content_type")
        .agg(f_count("*").alias("items"))
    )

    sources_monthly = (
        df_enriched.groupBy("published_month_str", "news_site", "content_type")
        .agg(f_count("*").alias("items"))
    )
    # --- Write local then upload to MinIO ---
    out_enriched = os.path.join(tmp, "out_enriched")
    out_daily = os.path.join(tmp, "out_trends_daily")
    out_monthly = os.path.join(tmp, "out_trends_monthly")
    out_sources_daily = os.path.join(tmp, "out_sources_daily")
    out_sources_monthly = os.path.join(tmp, "out_sources_monthly")


    df_enriched.write.mode("overwrite").parquet(out_enriched)
    df_enriched = df_enriched.cache()
    df_enriched.count()  # materializa cache (dataset pequeño, ok para prueba)

    trends_daily.write.mode("overwrite").parquet(out_daily)
    trends_monthly.write.mode("overwrite").parquet(out_monthly)
    sources_daily.write.mode("overwrite").parquet(out_sources_daily)
    sources_monthly.write.mode("overwrite").parquet(out_sources_monthly)

        

    upload_dir(out_enriched, "gold/content_enriched")
    upload_dir(out_daily, "gold/trends_daily")
    upload_dir(out_monthly, "gold/trends_monthly")
    upload_dir(out_sources_daily, "gold/sources_activity_daily")
    upload_dir(out_sources_monthly, "gold/sources_activity_monthly")


    spark.stop()

if __name__ == "__main__":
    main()
