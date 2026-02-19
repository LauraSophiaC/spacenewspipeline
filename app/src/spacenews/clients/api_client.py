import time
import requests
from typing import Optional, Dict, Any, Iterator
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from spacenews.log import log

class RateLimitError(Exception):
    pass

@retry(
    reraise=True,
    stop=stop_after_attempt(6),
    wait=wait_exponential(multiplier=1, min=1, max=30),
    retry=retry_if_exception_type((requests.RequestException, RateLimitError)),
)
def _get(session: requests.Session, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    r = session.get(url, params=params, timeout=30)

    if r.status_code == 429:
        retry_after = r.headers.get("Retry-After")
        if retry_after:
            try:
                time.sleep(int(retry_after))
            except Exception:
                pass
        raise RateLimitError("429 rate limited")

    r.raise_for_status()
    return r.json()

def iterate_results(base_url: str, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Iterator[Dict[str, Any]]:
    url = "{}/{}".format(base_url.rstrip("/"), endpoint.lstrip("/"))
    with requests.Session() as s:
        next_url = url
        next_params = params or {}
        page = 0

        while next_url:
            page += 1
            data = _get(s, next_url, next_params)
            results = data.get("results", []) or []

            for item in results:
                yield item

            next_url = data.get("next")
            next_params = None  # next ya viene con params
            log("INFO", "page_fetched", endpoint=endpoint, page=page, count=len(results), has_next=bool(next_url))
