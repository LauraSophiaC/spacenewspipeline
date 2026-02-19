import os

from typing import Optional

def env(key: str, default: Optional[str] = None) -> str:

    v = os.getenv(key, default)
    if v is None:
        raise RuntimeError(f"Missing env var: {key}")
    return v

SPACEFLIGHT_BASE_URL = env("SPACEFLIGHT_BASE_URL", "https://api.spaceflightnewsapi.net/v4")


S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "datalake")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")
