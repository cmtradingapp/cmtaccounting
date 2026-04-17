"""Phase-1 smoke test: connect to MT5 Manager and print basic data shapes.

Usage (from the cro-dashboard venv):
    python scripts/test_connection.py

Credentials default to the AN100 Manager account we already have access to.
Override with env vars MT5_SERVER / MT5_LOGIN / MT5_PASSWORD if needed.
"""
from __future__ import annotations

import json
import os
import sys
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Allow "python scripts\test_connection.py" from repo root.
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

from mt5_bridge import MT5Bridge, MT5Error  # noqa: E402


def _load_mt5_creds():
    # Prefer env vars; otherwise read ~/.claude/plans/mt5-an100-credentials.md
    server = os.environ.get("MT5_SERVER")
    login = os.environ.get("MT5_LOGIN")
    pw    = os.environ.get("MT5_PASSWORD")
    if not (server and login and pw):
        cred_path = Path.home() / ".claude" / "plans" / "mt5-an100-credentials.md"
        if cred_path.exists():
            for line in cred_path.read_text().splitlines():
                if line.lower().startswith("- login:"):    login = line.split(":",1)[1].strip()
                if line.lower().startswith("- password:"): pw    = line.split(":",1)[1].strip()
                if line.lower().startswith("- endpoint:"): server = line.split(":",1)[1].strip()
    if not (server and login and pw):
        raise SystemExit("MT5 creds missing — set MT5_SERVER/MT5_LOGIN/MT5_PASSWORD or "
                         "populate ~/.claude/plans/mt5-an100-credentials.md")
    return server, int(login), pw


SERVER, LOGIN, PASSWORD = _load_mt5_creds()


def _header(title: str) -> None:
    print()
    print("=" * 72)
    print(" ", title)
    print("=" * 72)


def main() -> int:
    _header(f"Connecting to {SERVER} as {LOGIN}")
    bridge = MT5Bridge()
    try:
        bridge.connect(SERVER, LOGIN, PASSWORD)
    except MT5Error as e:
        print(f"[FAIL] {e}")
        return 2
    except Exception:
        traceback.print_exc()
        return 3

    print(f"[OK] Connected. API version: {bridge.get_version()}")

    try:
        # ── users by group masks ─────────────────────────────────────────
        for mask in ("*", "real\\*", "demo\\*"):
            try:
                logins = bridge.get_user_logins(mask)
                print(f"[OK] UserLogins({mask!r}): {len(logins)} logins")
            except Exception as e:
                print(f"[WARN] UserLogins({mask!r}) -> {e}")

        # ── last 24h of deals ────────────────────────────────────────────
        now = datetime.now(tz=timezone.utc)
        since = now - timedelta(hours=24)
        try:
            deals = bridge.get_deals_by_group("*", since, now)
            print(f"[OK] DealRequestByGroup(*) last 24h: {len(deals)} deals")
            if deals:
                _header("Sample deal (first row)")
                print(json.dumps(deals[0], indent=2, default=str))
        except Exception as e:
            print(f"[WARN] DealRequestByGroup -> {e}")

        # ── currently open positions ────────────────────────────────────
        try:
            positions = bridge.get_positions_by_group("*")
            print(f"[OK] PositionRequestByGroup(*): {len(positions)} open positions")
            if positions:
                _header("Sample position (first row)")
                print(json.dumps(positions[0], indent=2, default=str))
        except Exception as e:
            print(f"[WARN] PositionRequestByGroup -> {e}")

        # ── a single user + their account (pick first login we saw) ─────
        try:
            all_logins = bridge.get_user_logins("*")
            if all_logins:
                pick = all_logins[0]
                user = bridge.get_user(pick)
                acct = bridge.get_account(pick)
                _header(f"Sample user {pick}")
                print(json.dumps({"user": user, "account": acct}, indent=2, default=str))
        except Exception as e:
            print(f"[WARN] single-user sample -> {e}")

    finally:
        bridge.disconnect()
        print("\n[OK] Disconnected cleanly.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
