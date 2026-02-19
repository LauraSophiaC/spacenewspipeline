import os
import psycopg2
from datetime import datetime

def conn():
    host = os.getenv("PG_HOST", "spacenews_postgres")   # docker default
    port = int(os.getenv("PG_PORT", "5432"))
    db   = os.getenv("PG_DB", "spacenews")
    user = os.getenv("PG_USER", "spacenews")
    pwd  = os.getenv("PG_PASS", "spacenews123")

    return psycopg2.connect(
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=pwd,
    )

def upsert_state(endpoint: str, item_id: int, updated_at: datetime):
    """
    Returns: "new" | "updated" | "skipped"
    """
    with conn() as c, c.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ingest_state (
                endpoint TEXT NOT NULL,
                item_id  BIGINT NOT NULL,
                updated_at TIMESTAMPTZ NULL,
                PRIMARY KEY (endpoint, item_id)
            );
        """)
        cur.execute("ALTER TABLE ingest_state ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NULL;")

        cur.execute("""
            SELECT updated_at
            FROM ingest_state
            WHERE endpoint = %s AND item_id = %s
        """, (endpoint, item_id))
        row = cur.fetchone()

        if row is None:
            cur.execute("""
                INSERT INTO ingest_state(endpoint, item_id, updated_at)
                VALUES (%s, %s, %s)
            """, (endpoint, item_id, updated_at))
            return "new"

        prev = row[0]
        if prev is None and updated_at is None:
            return "skipped"
        if prev is None and updated_at is not None:
            cur.execute("""
                UPDATE ingest_state SET updated_at = %s
                WHERE endpoint = %s AND item_id = %s
            """, (updated_at, endpoint, item_id))
            return "updated"
        if prev is not None and updated_at is None:
            return "skipped"

        # normaliza tz-naive vs tz-aware (por si llega naive)
        if prev.tzinfo is None and updated_at.tzinfo is not None:
            prev = prev.replace(tzinfo=updated_at.tzinfo)
        if prev.tzinfo is not None and updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=prev.tzinfo)

        if updated_at > prev:
            cur.execute("""
                UPDATE ingest_state SET updated_at = %s
                WHERE endpoint = %s AND item_id = %s
            """, (updated_at, endpoint, item_id))
            return "updated"

        return "skipped"
