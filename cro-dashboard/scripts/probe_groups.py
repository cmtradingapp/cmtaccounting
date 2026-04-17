"""Sample deal logins and print their MT5 group distribution — used to
discover this broker's group-mask conventions.
"""
from __future__ import annotations

import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from mt5_bridge import MT5Bridge  # noqa: E402

SEP = "\\"


def _creds():
    import os
    server = os.environ.get("MT5_SERVER")
    login  = os.environ.get("MT5_LOGIN")
    pw     = os.environ.get("MT5_PASSWORD")
    if not (server and login and pw):
        cred = Path.home() / ".claude" / "plans" / "mt5-an100-credentials.md"
        if cred.exists():
            for line in cred.read_text().splitlines():
                if line.lower().startswith("- login:"):    login = line.split(":",1)[1].strip()
                if line.lower().startswith("- password:"): pw    = line.split(":",1)[1].strip()
                if line.lower().startswith("- endpoint:"): server = line.split(":",1)[1].strip()
    if not (server and login and pw):
        raise SystemExit("MT5 creds missing")
    return server, int(login), pw


def main() -> int:
    b = MT5Bridge()
    b.connect(*_creds())
    try:
        now = datetime.now(timezone.utc)
        since = now - timedelta(hours=24)
        deals = b.get_deals_by_group("*", since, now)
        print(f"total deals last 24h: {len(deals)}")
        logins = list({d["login"] for d in deals})
        print(f"distinct logins in deals: {len(logins)}")

        sample = logins[:100]
        groups = Counter()
        for lg in sample:
            try:
                u = b.get_user(lg)
                groups[u["group"]] += 1
            except Exception:
                pass

        print("\nGroup histogram (sampled 100 deal logins):")
        for g, n in groups.most_common():
            print(f"  {g:60s} {n}")

        print("\nTop-level segment histogram:")
        tops = Counter(g.split(SEP)[0] for g in groups)
        for g, n in tops.most_common():
            print(f"  {g:30s} {n}")

        print("\nSecond-level segment histogram:")
        seconds = Counter(SEP.join(g.split(SEP)[:2]) for g in groups)
        for g, n in seconds.most_common():
            print(f"  {g:40s} {n}")
    finally:
        b.disconnect()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
