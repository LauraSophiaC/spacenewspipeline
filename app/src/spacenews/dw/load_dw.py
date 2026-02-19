import os
import json
import tempfile
from pathlib import Path
from datetime import datetime

import boto3
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


# -------------------------
# Config
# -------------------------
MINIO_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS = os.getenv("S3_ACCESS_KEY", "minioadmin")
MINIO_SECRET = os.getenv("S3_SECRET_KEY", "minioadmin123")
BUCKET = os.getenv("S3_BUCKET", "datalake")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5433"))
PG_DB = os.getenv("PG_DB", "spacenews")
PG_USER = os.getenv("PG_USER", "spacenews")
PG_PASS = os.getenv("PG_PASS", "spacenews123")


def s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
    )


def list_keys(prefix: str):
    client = s3()
    keys = []
    token = None
    while True:
        kwargs = dict(Bucket=BUCKET, Prefix=prefix, MaxKeys=1000)
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


def download_prefix(prefix: str, local_dir: str):
    client = s3()
    keys = list_keys(prefix)
    if not keys:
        raise RuntimeError(f"No objects found for prefix={prefix}")
    for key in keys:
        rel = key[len(prefix):].lstrip("/")
        target = Path(local_dir) / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        client.download_file(BUCKET, key, str(target))


def connect_pg():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )


def date_key(d: pd.Timestamp) -> int:
    return int(d.strftime("%Y%m%d"))


def ensure_tables(cur):
    # Por si no las creaste (idempotente)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_date (
      date_key INTEGER PRIMARY KEY,
      date DATE NOT NULL,
      year INTEGER NOT NULL,
      month INTEGER NOT NULL,
      day INTEGER NOT NULL
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_source (
      source_key SERIAL PRIMARY KEY,
      news_site TEXT NOT NULL,
      content_type TEXT NOT NULL,
      UNIQUE(news_site, content_type)
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_topic (
      topic_key SERIAL PRIMARY KEY,
      topic_name TEXT NOT NULL UNIQUE
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_content (
      content_key SERIAL PRIMARY KEY,
      id_content BIGINT NOT NULL UNIQUE,
      date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
      source_key INTEGER NOT NULL REFERENCES dim_source(source_key),
      topic_key INTEGER NOT NULL REFERENCES dim_topic(topic_key),
      title TEXT,
      summary TEXT,
      keyword_list TEXT,
      entities TEXT
    );
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_trends_daily (
      date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
      topic_key INTEGER NOT NULL REFERENCES dim_topic(topic_key),
      items_count INTEGER NOT NULL,
      PRIMARY KEY (date_key, topic_key)
    );
    """)


def upsert_dim_date(cur, dates: pd.Series):
    dates = pd.to_datetime(dates).dropna().dt.normalize().drop_duplicates()
    rows = []
    for d in dates:
        rows.append((date_key(d), d.date(), d.year, d.month, d.day))

    if not rows:
        return

    execute_values(cur, """
        INSERT INTO dim_date (date_key, date, year, month, day)
        VALUES %s
        ON CONFLICT (date_key) DO NOTHING
    """, rows)


def upsert_dim_topic(cur, topics: pd.Series):
    topics = topics.dropna().astype(str).drop_duplicates()
    rows = [(t,) for t in topics if t.strip()]
    if not rows:
        return

    execute_values(cur, """
        INSERT INTO dim_topic (topic_name)
        VALUES %s
        ON CONFLICT (topic_name) DO NOTHING
    """, rows)


def upsert_dim_source(cur, df_content: pd.DataFrame):
    if "news_site" not in df_content.columns or "content_type" not in df_content.columns:
        raise RuntimeError("Gold content_enriched must include news_site and content_type")

    sources = df_content[["news_site", "content_type"]].dropna().drop_duplicates()
    rows = [(r.news_site, r.content_type) for r in sources.itertuples(index=False)]
    if not rows:
        return

    execute_values(cur, """
        INSERT INTO dim_source (news_site, content_type)
        VALUES %s
        ON CONFLICT (news_site, content_type) DO NOTHING
    """, rows)


def fetch_dim_maps(cur):
    cur.execute("SELECT topic_key, topic_name FROM dim_topic;")
    topic_map = {name: key for key, name in cur.fetchall()}

    cur.execute("SELECT source_key, news_site, content_type FROM dim_source;")
    source_map = {(news_site, content_type): source_key for source_key, news_site, content_type in cur.fetchall()}

    return topic_map, source_map


def upsert_fact_content(cur, df: pd.DataFrame, topic_map, source_map):
    # required
    req = ["id", "published_date", "topic", "news_site", "content_type", "title", "summary", "keyword_list", "entities"]
    for c in req:
        if c not in df.columns:
            raise RuntimeError(f"Missing column in gold content_enriched: {c}")

    rows = []
    for r in df.itertuples(index=False):
        if pd.isna(r.id) or pd.isna(r.published_date) or pd.isna(r.topic) or pd.isna(r.news_site) or pd.isna(r.content_type):
            continue

        dk = date_key(pd.to_datetime(r.published_date))
        tk = topic_map.get(str(r.topic), None)
        sk = source_map.get((str(r.news_site), str(r.content_type)), None)
        if tk is None or sk is None:
            continue

        # keyword_list/entities pueden ser listas -> las guardamos como JSON string
        kw = r.keyword_list
        ent = r.entities
        kw_s = json.dumps(kw) if isinstance(kw, (list, tuple)) else (str(kw) if kw is not None else None)
        ent_s = json.dumps(ent) if isinstance(ent, (list, tuple)) else (str(ent) if ent is not None else None)

        rows.append((
            int(r.id),
            dk,
            sk,
            tk,
            getattr(r, "title", None),
            getattr(r, "summary", None),
            kw_s,
            ent_s
        ))

    if not rows:
        return

    execute_values(cur, """
        INSERT INTO fact_content
          (id_content, date_key, source_key, topic_key, title, summary, keyword_list, entities)
        VALUES %s
        ON CONFLICT (id_content)
        DO UPDATE SET
          date_key = EXCLUDED.date_key,
          source_key = EXCLUDED.source_key,
          topic_key = EXCLUDED.topic_key,
          title = EXCLUDED.title,
          summary = EXCLUDED.summary,
          keyword_list = EXCLUDED.keyword_list,
          entities = EXCLUDED.entities
    """, rows, page_size=1000)


def upsert_fact_trends_daily(cur, df_trends: pd.DataFrame, topic_map):
    req = ["published_date", "topic", "items"]
    for c in req:
        if c not in df_trends.columns:
            raise RuntimeError(f"Missing column in gold trends_daily: {c}")

    rows = []
    for r in df_trends.itertuples(index=False):
        if pd.isna(r.published_date) or pd.isna(r.topic) or pd.isna(r.items):
            continue
        dk = date_key(pd.to_datetime(r.published_date))
        tk = topic_map.get(str(r.topic), None)
        if tk is None:
            continue
        rows.append((dk, tk, int(r.items)))

    if not rows:
        return

    execute_values(cur, """
        INSERT INTO fact_trends_daily (date_key, topic_key, items_count)
        VALUES %s
        ON CONFLICT (date_key, topic_key)
        DO UPDATE SET items_count = EXCLUDED.items_count
    """, rows, page_size=1000)


def read_parquet_dir(path: str) -> pd.DataFrame:
    # pandas+pyarrow soporta leer un directorio con part-files
    return pd.read_parquet(path, engine="pyarrow")


def main():
    tmp = tempfile.mkdtemp(prefix="spacenews_dw_")
    gold_dir = os.path.join(tmp, "gold")

    print(f"[DW] Downloading gold/ from MinIO -> {gold_dir}")
    download_prefix("gold/", gold_dir)

    content_path = os.path.join(gold_dir, "content_enriched")
    trends_daily_path = os.path.join(gold_dir, "trends_daily")

    print("[DW] Reading gold/content_enriched parquet...")
    df_content = read_parquet_dir(content_path)

    print("[DW] Reading gold/trends_daily parquet...")
    df_trends = read_parquet_dir(trends_daily_path)

    # Normalizaciones esperadas
    # published_date puede venir como datetime64 o date; lo dejamos en datetime normalizado
    if "published_date" in df_content.columns:
        df_content["published_date"] = pd.to_datetime(df_content["published_date"]).dt.normalize()

    if "published_date" in df_trends.columns:
        df_trends["published_date"] = pd.to_datetime(df_trends["published_date"]).dt.normalize()

    conn = connect_pg()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            ensure_tables(cur)

            # dims
            print("[DW] Upserting dim_date...")
            upsert_dim_date(cur, pd.concat([df_content["published_date"], df_trends["published_date"]], ignore_index=True))

            print("[DW] Upserting dim_topic...")
            upsert_dim_topic(cur, pd.concat([df_content["topic"], df_trends["topic"]], ignore_index=True))

            print("[DW] Upserting dim_source...")
            upsert_dim_source(cur, df_content)

            topic_map, source_map = fetch_dim_maps(cur)

            # facts
            print("[DW] Upserting fact_content...")
            upsert_fact_content(cur, df_content, topic_map, source_map)

            print("[DW] Upserting fact_trends_daily...")
            upsert_fact_trends_daily(cur, df_trends, topic_map)

        conn.commit()
        print("[DW] ✅ Load completed successfully.")
    except Exception as e:
        conn.rollback()
        print("[DW] ❌ Load failed:", e)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
