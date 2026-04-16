#!/usr/bin/env python3
"""Daily cron: record the max CRM operator ID at day-start.

This script queries vtiger_users for the current highest operator ID and
stores it in the local fees.db watermark table. The operators dashboard
compares live IDs against this snapshot to identify truly new operators.

Server cron (runs inside the Docker container at 00:01 every night):
  1 0 * * * docker exec recon python /app/snapshot_watermark.py >> /var/log/watermark-snapshot.log 2>&1
"""
import os
import sys

# Allow imports from the app directory whether run directly or via docker exec
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime
from db import crm
import queries


def main() -> None:
    ts = datetime.now().isoformat(timespec="seconds")
    print(f"[{ts}] Operator watermark snapshot starting...", flush=True)

    # Query CRM for current max operator ID
    try:
        with crm() as cur:
            cur.execute(
                "SELECT MAX(id) AS max_id FROM report.vtiger_users WHERE id IS NOT NULL"
            )
            row = cur.fetchone()
            max_id = int(row["max_id"]) if row and row["max_id"] else 0
    except Exception as exc:
        print(f"[{ts}] ERROR querying CRM: {exc}", flush=True)
        sys.exit(1)

    if max_id == 0:
        print(f"[{ts}] WARNING: CRM returned max_id=0 — skipping update.", flush=True)
        sys.exit(0)

    # Read previous watermark and update
    prev = queries.get_operator_watermark()
    prev_id = prev["max_id"]
    queries.update_operator_watermark(max_id)

    delta = max_id - prev_id
    print(
        f"[{ts}] Watermark updated: #{prev_id} → #{max_id}  (delta: +{delta})",
        flush=True,
    )


if __name__ == "__main__":
    main()
