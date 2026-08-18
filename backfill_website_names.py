"""
Backfill: normalize `website_name` to the site hostname in the NEW DB.

Historical rows were written using the raw `websites.website` string from the
OLD DB at sync time (e.g. https://ai.jobars2.com/). When the OLD DB row's URL
spelling changed later, the same ad unit's days got split across two different
website_name values, so per-site earnings looked wrong.

This script rewrites existing rows so `website_name` is the stable hostname
(e.g. ai.jobars2.com), matching the behaviour of sync.py after the fix.

Usage:
  python backfill_website_names.py            # dry run (prints planned change)
  python backfill_website_names.py --apply    # actually update rows
"""

import os
import sys
import re

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

TABLES = [
    "ad_unit_daily_stats",
    "ad_unit_country_daily_stats",
    "ad_unit_breakdown_daily_stats",
]

APPLY = "--apply" in sys.argv


def website_host(website: str) -> str:
    s = (website or "").strip().lower()
    s = re.sub(r"^[a-z][a-z0-9+.-]*://", "", s)
    s = s.split("/")[0].split("?")[0].strip()
    s = re.sub(r"^www\.", "", s)
    return s.strip()


def main():
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("SUPABASE_URL / SUPABASE_SERVICE_KEY required in .env")
        sys.exit(1)

    client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    mode = "APPLY" if APPLY else "DRY-RUN"
    print(f"=== Backfill website_name (mode: {mode}) ===")

    for table in TABLES:
        # collect distinct website_name values
        names = set()
        offset = 0
        while True:
            resp = client.table(table).select("website_name").range(offset, offset + 999).execute()
            chunk = resp.data or []
            for r in chunk:
                names.add(r.get("website_name") or "")
            if len(chunk) < 1000:
                break
            offset += 1000

        fixed = 0
        for name in sorted(names):
            if not name:
                continue
            host = website_host(name)
            if host == name.lower() and not re.search(r"^[a-z][a-z0-9+.-]*://", name) and not name.startswith("/"):
                continue  # already canonical
            if not host:
                continue
            print(f"  [{table}] '{name}' -> '{host}'")
            if APPLY:
                upd = client.table(table).update({"website_name": host}).eq("website_name", name).execute()
                fixed += len(upd.data or [])
        print(f"  {table}: {len(names)} distinct website_name values, {fixed} rows updated")

    print("=== Done ===")


if __name__ == "__main__":
    main()