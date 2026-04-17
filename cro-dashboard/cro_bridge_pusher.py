"""Windows-only push service: polls live MT5 data every 30s and POSTs to
https://recon.cmtrading.com/cro/feed so the Linux dashboard shows real-time
values.

What it computes and pushes:
  floating_pnl_usd  -- sum(profit + storage) for all open positions, in USD
  n_positions       -- count of open positions
  closed_pnl_usd    -- sum(profit + storage + commission) for today's closing
                       deals, converted to USD per account currency
  n_closing_deals   -- count of closing deals today
  source / group_mask / pushed_at

Currency conversion (closed PnL):
  MT5 deal.Profit() is in the account's deposit currency (USD for ~98% of
  CMV* accounts). For non-USD accounts (ZAR, EUR, etc.):
    profit_usd = (profit + storage + commission) / rate_profit
  where rate_profit is the FX rate MT5 applied to convert the symbol's
  profit currency into the account's currency. Inverting it gives USD.

  For USD accounts the formula simplifies to:
    profit_usd = profit + storage + commission   (rate_profit was used to
    convert e.g. JPY -> USD but profit is already in USD)

  To distinguish, we batch-fetch the group for each login that appears in
  today's deals, then parse the currency from the group name.

Usage:
    set CRO_BRIDGE_SECRET=<value>
    set CRO_FEED_URL=https://recon.cmtrading.com/cro/feed   # default
    python cro_bridge_pusher.py

Optional:
    MT5_SERVER   MT5_LOGIN   MT5_PASSWORD   (or ~/.claude/plans/mt5-...-credentials.md)
    CRO_GROUP    CMV*         (group mask)
    CRO_INTERVAL 30           (seconds between polls)
    CRO_SOURCE   AN100
"""
from __future__ import annotations

import os
import sys
import time
import json
from collections import defaultdict
from datetime import datetime, date, timezone, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
import urllib.request
import urllib.error

NICOSIA = ZoneInfo("Europe/Nicosia")

FEED_URL  = os.environ.get("CRO_FEED_URL", "https://recon.cmtrading.com/cro/feed")
SECRET    = os.environ.get("CRO_BRIDGE_SECRET", "")
GROUP     = os.environ.get("CRO_GROUP", "CMV*")
INTERVAL  = int(os.environ.get("CRO_INTERVAL", "30"))
SOURCE    = os.environ.get("CRO_SOURCE", "AN100")

# MT5 deal action / entry codes
_ACT_BUY = 0; _ACT_SELL = 1
_ENTRY_IN = 0


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
        raise SystemExit("MT5 creds missing")
    return server, int(login), pw


def _day_bounds_utc() -> tuple[datetime, datetime]:
    """Today's day boundaries in Europe/Nicosia, converted to UTC."""
    today_nicosia = datetime.now(NICOSIA).date()
    start = datetime.combine(today_nicosia, datetime.min.time(), NICOSIA)
    end   = start + timedelta(days=1)
    return start.astimezone(timezone.utc), end.astimezone(timezone.utc)


def _to_usd(native_amount: float, account_currency: str, rate_profit: float) -> float:
    """Convert an amount in account_currency to USD.

    For USD accounts: profit is already in USD regardless of rate_profit.
    For non-USD accounts: profit = symbol_profit * rate_profit (e.g. *USDZAR),
      so profit_usd = profit / rate_profit.
    """
    if account_currency == "USD":
        return native_amount
    if rate_profit and rate_profit > 0:
        return native_amount / rate_profit
    return native_amount  # fallback


def _compute(bridge) -> dict:
    """Pull live MT5 data and return the payload for /cro/feed."""
    # -- open positions (floating PnL) --
    positions = bridge.get_positions_by_group(GROUP)

    # For floating PnL: batch-fetch groups of logins in positions
    pos_logins = list({int(p["login"]) for p in positions})

    # Batch fetch in chunks of 500 (API limit may vary)
    CHUNK = 500
    login_group: dict[int, str] = {}
    for i in range(0, len(pos_logins), CHUNK):
        chunk = pos_logins[i : i + CHUNK]
        try:
            for u in bridge.get_users_by_logins(chunk):
                login_group[int(u["login"])] = str(u["group"])
        except Exception:
            pass  # fallback to USD if lookup fails

    # Cache group -> currency (same for every call, cheap)
    grp_currency_cache: dict[str, str] = {}
    def currency_of(login: int) -> str:
        grp = login_group.get(login, "")
        if grp not in grp_currency_cache:
            grp_currency_cache[grp] = bridge.group_currency(grp) if grp else "USD"
        return grp_currency_cache[grp]

    floating_pnl_usd = sum(
        _to_usd(
            float(p["profit"]) + float(p["storage"]),
            currency_of(int(p["login"])),
            float(p.get("rate_profit", 1.0)),
        )
        for p in positions
    )

    # -- today's closing deals (closed PnL) --
    day_start, day_end = _day_bounds_utc()
    deals = bridge.get_deals_by_group(GROUP, day_start, day_end)

    closing = [
        d for d in deals
        if d["action"] in (_ACT_BUY, _ACT_SELL) and d["entry"] != _ENTRY_IN
    ]

    # Add any new logins from deals to the lookup
    deal_logins = list({int(d["login"]) for d in closing} - set(login_group))
    for i in range(0, len(deal_logins), CHUNK):
        chunk = deal_logins[i : i + CHUNK]
        try:
            for u in bridge.get_users_by_logins(chunk):
                login_group[int(u["login"])] = str(u["group"])
        except Exception:
            pass

    closed_pnl_usd = sum(
        _to_usd(
            float(d["profit"]) + float(d.get("storage", 0)) + float(d.get("commission", 0)),
            currency_of(int(d["login"])),
            float(d.get("rate_profit", 1.0)),
        )
        for d in closing
    )

    return {
        "floating_pnl_usd": floating_pnl_usd,
        "closed_pnl_usd":   closed_pnl_usd,
        "n_positions":      len(positions),
        "n_closing_deals":  len(closing),
        "source":           SOURCE,
        "group_mask":       GROUP,
        "pushed_at":        datetime.now(timezone.utc).isoformat(),
        # legacy key kept for backward compat
        "floating_pnl":     floating_pnl_usd,
    }


def _push(payload: dict) -> int:
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
        print(f"  push error: {e}", file=sys.stderr, flush=True)
        return 0


def main() -> None:
    if not SECRET:
        raise SystemExit("CRO_BRIDGE_SECRET env var is required.")

    server, login, pw = _load_mt5_creds()
    print(f"[pusher] MT5={server} login={login} group={GROUP} -> {FEED_URL}", flush=True)
    print(f"[pusher] interval={INTERVAL}s  (Ctrl-C to stop)", flush=True)

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
                payload = _compute(bridge)
            finally:
                bridge.disconnect()

            status = _push(payload)
            elapsed = time.monotonic() - t0
            ts = datetime.now().strftime("%H:%M:%S")
            ok = "OK" if status == 200 else f"HTTP {status}"
            print(
                f"[{ts}] #{cycle:04d}"
                f"  float={payload['floating_pnl_usd']:>15,.2f}"
                f"  closed={payload['closed_pnl_usd']:>13,.2f}"
                f"  pos={payload['n_positions']:>6}"
                f"  deals={payload['n_closing_deals']:>5}"
                f"  -> {ok}  ({elapsed:.0f}s)",
                flush=True,
            )
        except MT5Error as e:
            print(f"[pusher] MT5Error: {e}", file=sys.stderr, flush=True)
        except KeyboardInterrupt:
            print("\n[pusher] stopped.")
            return
        except Exception as e:
            import traceback; traceback.print_exc()

        elapsed = time.monotonic() - t0
        time.sleep(max(0.0, INTERVAL - elapsed))


if __name__ == "__main__":
    main()
