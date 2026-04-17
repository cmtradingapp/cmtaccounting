"""cro-live-pusher — long-lived Wine process; streams JSON lines to /cro/feed.

Starts MT5LivePusher.exe once under Wine and reads its stdout indefinitely.
The exe outputs one JSON line every CRO_INTERVAL seconds while staying
connected via PUMP_MODE_POSITIONS — no reconnect overhead per cycle.

On any failure the process is restarted with exponential backoff (cap 60s).

Env vars:
  MT5_SERVER, MT5_LOGIN, MT5_PASSWORD, CRO_GROUP, CRO_INTERVAL
  CRO_BRIDGE_SECRET    required
  CRO_FEED_URL         default http://recon:5050/cro/feed
  CRO_LIVE_EXE         default /app/MT5LivePusher.exe
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import urllib.request
import urllib.error

FEED_URL    = os.environ.get("CRO_FEED_URL", "http://recon:5050/cro/feed")
SECRET      = os.environ.get("CRO_BRIDGE_SECRET", "")
EXE_PATH    = os.environ.get("CRO_LIVE_EXE", "/app/MT5LivePusher.exe")
HEALTH_FILE = "/tmp/last_push"


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
        print(f"[live-pusher] push error: {e}", file=sys.stderr, flush=True)
        return 0


def run_exe_session() -> None:
    """Start the exe, forward stderr, read JSON lines until exit."""
    print(f"[live-pusher] starting {EXE_PATH}", flush=True)
    env = os.environ.copy()
    proc = subprocess.Popen(
        ["wine", EXE_PATH],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )

    # Forward exe stderr (includes Wine fixme lines and [pusher] logs) to our stderr.
    def fwd_stderr() -> None:
        for line in proc.stderr:
            print(f"[exe] {line.rstrip()}", file=sys.stderr, flush=True)

    t = threading.Thread(target=fwd_stderr, daemon=True)
    t.start()

    cycle = 0
    for raw_line in proc.stdout:
        line = raw_line.strip()
        if not line.startswith("{"):
            continue
        cycle += 1
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            print(f"[live-pusher] bad JSON: {line[:200]}", file=sys.stderr, flush=True)
            continue

        payload.setdefault("floating_pnl", payload.get("floating_pnl_usd", 0.0))

        status = push(payload)
        ts = datetime.now().strftime("%H:%M:%S")
        if status == 200:
            try:
                Path(HEALTH_FILE).touch()
            except Exception:
                pass
            ok = "OK"
        else:
            ok = f"HTTP {status}"
        print(
            f"[{ts}] #{cycle:04d}"
            f"  float={payload.get('floating_pnl_usd', 0):>15,.2f}"
            f"  closed={payload.get('closed_pnl_usd', 0):>13,.2f}"
            f"  pos={payload.get('n_positions', 0):>6}"
            f"  deals={payload.get('n_closing_deals', 0):>5}"
            f"  -> {ok}",
            flush=True,
        )

    proc.wait()
    print(f"[live-pusher] exe exited code={proc.returncode}", file=sys.stderr, flush=True)


def main() -> None:
    if not SECRET:
        raise SystemExit("CRO_BRIDGE_SECRET env var is required.")
    if not os.environ.get("MT5_PASSWORD"):
        raise SystemExit("MT5_PASSWORD env var is required.")

    print(f"[live-pusher] exe={EXE_PATH}  feed={FEED_URL}", flush=True)

    failures = 0
    while True:
        try:
            run_exe_session()
            # Clean exit (returncode == 0 shouldn't happen normally)
            failures = 0
        except KeyboardInterrupt:
            print("\n[live-pusher] stopped.", flush=True)
            return
        except Exception:
            import traceback
            traceback.print_exc()

        failures += 1
        backoff = min(60, 5 * (2 ** min(failures - 1, 4)))
        print(f"[live-pusher] restart #{failures} in {backoff}s...", file=sys.stderr, flush=True)
        time.sleep(backoff)


if __name__ == "__main__":
    main()
