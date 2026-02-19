import uuid
import os
from datetime import date
from dateutil.parser import isoparse

from spacenews.config import SPACEFLIGHT_BASE_URL
from spacenews.clients.api_client import iterate_results
from spacenews.storage.state_store import upsert_state
from spacenews.storage.bronze_writer import write_bronze
from spacenews.log import log

ENDPOINTS = ["articles", "blogs", "reports"]
MAX_ITEMS_PER_ENDPOINT = 300  # modo desarrollo

def main():
    run_id = str(uuid.uuid4())
    run_date = os.getenv("RUN_DATE", date.today().isoformat())

    for ep in ENDPOINTS:
        fetched = 0
        new = 0
        updated = 0
        skipped = 0
        bronze_records = []

        for item in iterate_results(SPACEFLIGHT_BASE_URL, ep, params={"limit": 100}):
            fetched += 1

            item_id = int(item["id"])
            updated_at = isoparse(item["updated_at"]) if item.get("updated_at") else None

            decision = upsert_state(ep, item_id, updated_at)

            if decision == "new":
                new += 1
            elif decision == "updated":
                updated += 1
            else:
                skipped += 1

            if decision in ("new", "updated"):
                bronze_records.append(item)

            if fetched % 200 == 0:
                log("INFO", "progress", endpoint=ep, fetched=fetched, new=new, updated=updated, skipped=skipped)

            if fetched >= MAX_ITEMS_PER_ENDPOINT:
                log("INFO", "dev_limit_reached", endpoint=ep, max_items=MAX_ITEMS_PER_ENDPOINT)
                break

        # Escribe bronze por endpoint y fecha
        write_bronze(endpoint=ep, records=bronze_records, run_date=run_date)

        log("INFO", "endpoint_done", run_id=run_id, endpoint=ep, fetched=fetched, new=new, updated=updated, skipped=skipped, bronze_written=len(bronze_records))

    log("INFO", "extract_done", run_id=run_id, run_date=run_date)

if __name__ == "__main__":
    main()
