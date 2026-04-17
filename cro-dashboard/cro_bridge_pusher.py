"""Windows-only push service: polls live MT5 positions every 30s and POSTs
the floating PnL sum to https://recon.cmtrading.com/cro/feed so the Linux
dashboard can show a real-time value.

Usage:
    set CRO_BRIDGE_SECRET=G6WW7kMKTVfRN2K26k9cIKOJQK2Qxa9KHKJelnWP4f8
    set CRO_FEED_URL=https://recon.cmtrading.com/cro/feed
    python cro_bridge_pusher.py

Optional overrides (all have defaults from mt5-an100-credentials.md):
    MT5_SERVER   176.126.66.18:1950
    MT5_LOGIN    1111
    MT5_PASSWORD <from cred file>
    CRO_GROUP    CMV*
    CRO_INTERVAL 30       (seconds between polls)
"""
from __future__ import annotations

import os
import sys
import time
import json
from datetime import datetime, timezone
from pathlib import Path

import urllib.request
import urllib.error


# ── config ────────────────────────────────────────────────────────────────
FEED_URL  = os.environ.get("CRO_FEED_URL", "https://recon.cmtrading.com/cro/feed")
SECRET    = os.environ.get("CRO_BRIDGE_SECRET", "")
GROUP     = os.environ.get("CRO_GROUP", "CMV*")
INTERVAL  = int(os.environ.get("CRO_INTERVAL", "30"))


def _load_mt5_creds() -> tuple[str, int, str]:
    server = os.environ.get("MT5_SERVER")
    login  = os.environ.get("MT5_LOGIN")
    pw     = os.environ.get("MT5_PASSWORD")
    if not (server and login and pw):
        cred = Path.home() / ".claude" / "plans" / "mt5-an100-credentials.md"
        if cred.exists():
            for line in cred.read_text().splitlines():
                ll = line.lower()
                if ll.startswith("- login:"):    login  = line.split(":", 1)[1].strip()
                if ll.startswith("- password:"): pw     = line.split(":", 1)[1].strip()
                if ll.startswith("- endpoint:"): server = line.split(":", 1)[1].strip()
    if not (server and login and pw):
        raise SystemExit(
            "MT5 creds missing -- set MT5_SERVER/MT5_LOGIN/MT5_PASSWORD "
            "or populate ~/.claude/plans/mt5-an100-credentials.md"
        )
    return server, int(login), pw


def _push(floating_pnl: float, n_positions: int, source: str) -> int:
    """POST payload to the feed endpoint. Returns HTTP status code."""
    payload = json.dumps({
        "floating_pnl": floating_pnl,
        "n_positions":  n_positions,
        "source":       source,
        "group_mask":   GROUP,
        "pushed_at":    datetime.now(timezone.utc).isoformat(),
    }).encode()
    req = urllib.request.Request(
        FEED_URL,
        data=payload,
        headers={
            "Content-Type":    "application/json",
            "X-Bridge-Secret": SECRET,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception as e:
        print(f"  push error: {e}", file=sys.stderr)
        return 0


def main() -> None:
    if not SECRET:
        raise SystemExit("CRO_BRIDGE_SECRET env var is required.")

    server, login, pw = _load_mt5_creds()
    print(f"[pusher] MT5={server} login={login} group={GROUP} -> {FEED_URL}", flush=True)
    print(f"[pusher] interval={INTERVAL}s  (Ctrl-C to stop)", flush=True)

    # Import here so the module can be imported on non-Windows without crashing.
    sys.path.insert(0, str(Path(__file__).parent))
    from mt5_bridge import MT5Bridge, MT5Error

    cycle = 0
    while True:
        cycle += 1
        t0 = time.monotonic()
        try:
            bridge = MT5Bridge()
            bridge.connect(server, login, pw)
            try:
                positions = bridge.get_positions_by_group(GROUP)
            finally:
                bridge.disconnect()

            floating_pnl = sum(
                float(p.get("profit", 0)) + float(p.get("storage", 0))
                for p in positions
            )
            n_pos = len(positions)

            status = _push(floating_pnl, n_pos, "AN100")
            elapsed = time.monotonic() - t0
            ts = datetime.now().strftime("%H:%M:%S")
            ok = "OK" if status == 200 else f"HTTP {status}"
            print(
                f"[{ts}] #{cycle:04d}  floating_pnl={floating_pnl:>15,.2f}"
                f"  n_positions={n_pos:>6}  -> {ok}  ({elapsed:.1f}s)",
                flush=True,
            )

        except MT5Error as e:
            print(f"[pusher] MT5Error: {e}", file=sys.stderr)
        except KeyboardInterrupt:
            print("\n[pusher] stopped.")
            return
        except Exception as e:
            print(f"[pusher] unexpected error: {e}", file=sys.stderr)

        # Sleep the remainder of the interval, accounting for query time.
        elapsed = time.monotonic() - t0
        sleep_for = max(0.0, INTERVAL - elapsed)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
