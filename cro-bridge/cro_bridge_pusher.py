"""cro-bridge pusher -- native Linux Python 3.

Every N seconds:
  1. Exec `wine /app/MT5Bridge.exe` with MT5 + day-window env vars.
  2. Parse one JSON line from stdout.
  3. POST the payload to the recon-app's /cro/feed endpoint.

The .exe does all the MT5 SDK work (connect, pull positions + deals, FX
convert to USD). We stay on the Linux side for the HTTP push and the loop.

Env vars:
  MT5_SERVER           default 176.126.66.18:1950
  MT5_LOGIN            default 1111
  MT5_PASSWORD         required
  CRO_GROUP            default CMV*
  CRO_BRIDGE_SECRET    required
  CRO_FEED_URL         default http://recon:5050/cro/feed
  CRO_INTERVAL         default 30 seconds
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import urllib.request
import urllib.error

NICOSIA   = ZoneInfo("Europe/Nicosia")
FEED_URL  = os.environ.get("CRO_FEED_URL", "http://recon:5050/cro/feed")
SECRET    = os.environ.get("CRO_BRIDGE_SECRET", "")
INTERVAL  = int(os.environ.get("CRO_INTERVAL", "30"))
EXE_PATH  = os.environ.get("CRO_BRIDGE_EXE", "/app/MT5Bridge.exe")
HEALTH_FILE = "/tmp/last_push"


def day_bounds_utc_seconds() -> tuple[int, int]:
    """Return (start_seconds, now_seconds) for today in Europe/Nicosia, in UTC."""
    today_nic = datetime.now(NICOSIA).date()
    start_local = datetime.combine(today_nic, datetime.min.time(), NICOSIA)
    start_utc   = start_local.astimezone(timezone.utc)
    now_utc     = datetime.now(timezone.utc)
    epoch       = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return int((start_utc - epoch).total_seconds()), int((now_utc - epoch).total_seconds())


def run_bridge_exe() -> dict | None:
    """Invoke MT5Bridge.exe under Wine; return parsed JSON or None on error."""
    day_start, now_s = day_bounds_utc_seconds()
    env = os.environ.copy()
    env["CRO_DAY_START"] = str(day_start)
    env["CRO_NOW"]       = str(now_s)
    try:
        result = subprocess.run(
            ["wine", EXE_PATH],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=60,
            text=True,
        )
    except subprocess.TimeoutExpired:
        print("[pusher] wine exe timed out (60s)", file=sys.stderr, flush=True)
        return None
    except FileNotFoundError as e:
        print(f"[pusher] wine or exe missing: {e}", file=sys.stderr, flush=True)
        return None

    if result.returncode != 0:
        print(f"[pusher] exe exit={result.returncode}  stderr={result.stderr.strip()[:500]}",
              file=sys.stderr, flush=True)
        return None

    # Wine often prints a pile of 'fixme:' lines before our JSON. Find the
    # last non-empty line that starts with '{' and parse that.
    for line in reversed(result.stdout.splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                return json.loads(line)
            except json.JSONDecodeError:
                continue
    print(f"[pusher] no JSON in stdout: {result.stdout[-500:]!r}",
          file=sys.stderr, flush=True)
    return None


def push(payload: dict) -> int:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        FEED_URL,
        data=data,
        headers={"Content-Type": "application/json", "X-Bridge-Secret": SECRET},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status
    except urllib.error.HTTPError as e:
        return e.code
    except Exception as e:
        print(f"[pusher] push error: {e}", file=sys.stderr, flush=True)
        return 0


def main() -> None:
    if not SECRET:
        raise SystemExit("CRO_BRIDGE_SECRET env var is required.")
    if not os.environ.get("MT5_PASSWORD"):
        raise SystemExit("MT5_PASSWORD env var is required.")

    print(f"[pusher] exe={EXE_PATH}  feed={FEED_URL}  interval={INTERVAL}s", flush=True)
    cycle = 0
    consecutive_failures = 0
    while True:
        cycle += 1
        t0 = time.monotonic()
        try:
            payload = run_bridge_exe()
            if payload is not None:
                # backward-compat: expose legacy key
                payload.setdefault("floating_pnl", payload.get("floating_pnl_usd", 0.0))
                status = push(payload)
                ts = datetime.now().strftime("%H:%M:%S")
                if status == 200:
                    consecutive_failures = 0
                    try:
                        Path(HEALTH_FILE).touch()
                    except Exception:
                        pass
                    ok = "OK"
                else:
                    consecutive_failures += 1
                    ok = f"HTTP {status}"
                print(
                    f"[{ts}] #{cycle:04d}"
                    f"  float={payload.get('floating_pnl_usd', 0):>15,.2f}"
                    f"  closed={payload.get('closed_pnl_usd', 0):>13,.2f}"
                    f"  pos={payload.get('n_positions', 0):>6}"
                    f"  deals={payload.get('n_closing_deals', 0):>5}"
                    f"  -> {ok}  ({time.monotonic()-t0:.0f}s)",
                    flush=True,
                )
            else:
                consecutive_failures += 1
        except KeyboardInterrupt:
            print("\n[pusher] stopped by SIGINT.", flush=True)
            return
        except Exception:
            consecutive_failures += 1
            import traceback
            traceback.print_exc()

        if consecutive_failures > 0:
            backoff = min(INTERVAL, 5 * (2 ** min(consecutive_failures - 1, 4)))
            time.sleep(backoff)
        else:
            elapsed = time.monotonic() - t0
            time.sleep(max(0.0, INTERVAL - elapsed))


if __name__ == "__main__":
    main()
