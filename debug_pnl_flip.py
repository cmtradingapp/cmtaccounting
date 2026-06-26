"""Debug: show current PnL values vs broker-perspective (negated) across all three
sections and the volume_distribution table.

Run from cmtaccounting root:
    python debug_pnl_flip.py

Reads DB URL from recon-app/config.py or falls back to the hardcoded prod DSN.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "recon-app"))

import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta, timezone
from cro_metrics import (
    closed_pnl_usd,
    total_floating_usd,
    yesterday_floating_usd,
    total_floating_usd_eod,
    volume_distribution,
)

DSN = "postgresql://cro:bTiBZzbU2gtAfA5BfPdR5PFcpLqcteu@213.199.45.213:5432/cro_db"

def fmt(v: float) -> str:
    return f"${v:>14,.0f}"

def fmt_pair(label: str, current: float, flipped: float) -> None:
    marker = " <-- broker view" if flipped != current else ""
    print(f"  {label:<35s}  current={fmt(current)}   flipped={fmt(flipped)}{marker}")

def main():
    conn = psycopg2.connect(DSN)
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    now_utc         = datetime.now(timezone.utc)
    today_start     = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_start = today_start - timedelta(days=1)
    day_before      = today_start - timedelta(days=2)
    month_start     = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    prev_month_end  = month_start - timedelta(days=1)

    ts_today    = int(today_start.timestamp())
    ts_yest     = int(yesterday_start.timestamp())
    ts_dayb     = int(day_before.timestamp())
    ts_month    = int(month_start.timestamp())
    ts_pme      = int(prev_month_end.timestamp())
    ts_end      = ts_today + 86400

    # ── Component aggregates
    settled_today  = closed_pnl_usd(cur, ts_today,  ts_end)
    settled_yest   = closed_pnl_usd(cur, ts_yest,   ts_today)
    settled_mtd    = closed_pnl_usd(cur, ts_month,  ts_end)

    float_now      = total_floating_usd(cur)
    float_yest_eod = total_floating_usd_eod(cur, ts_yest, ts_today)
    float_dayb_eod = total_floating_usd_eod(cur, ts_dayb, ts_yest)
    float_month_st = total_floating_usd_eod(cur, ts_pme,  ts_month)
    float_yest_fn  = yesterday_floating_usd(cur, ts_yest, ts_today)

    delta_today = float_now      - float_yest_fn
    delta_yest  = float_yest_eod - float_dayb_eod
    delta_mtd   = float_now      - float_month_st

    daily_today  = delta_today + settled_today
    daily_yest   = delta_yest  + settled_yest
    monthly_pnl  = delta_mtd   + settled_mtd

    print("=" * 80)
    print(f"  PnL SIGN FLIP DEBUG - {now_utc.strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 80)

    print("\n-- TODAY'S STATUS" + "-" * 63)
    fmt_pair("Settled PnL",         settled_today,  -settled_today)
    fmt_pair("Delta Floating",      delta_today,    -delta_today)
    fmt_pair("Daily PnL (card)",    daily_today,    -daily_today)

    print("\n-- YESTERDAY'S STATUS" + "-" * 59)
    fmt_pair("Settled PnL",         settled_yest,   -settled_yest)
    fmt_pair("Delta Floating",      delta_yest,     -delta_yest)
    fmt_pair("Daily PnL (card)",    daily_yest,     -daily_yest)

    print("\n-- MONTH-TO-DATE" + "-" * 64)
    fmt_pair("Settled PnL (MTD)",   settled_mtd,    -settled_mtd)
    fmt_pair("Delta Floating (MTD)",delta_mtd,      -delta_mtd)
    fmt_pair("Monthly PnL (card)",  monthly_pnl,    -monthly_pnl)

    # volume_distribution daily + monthly PnL per symbol
    print("\n-- VOLUME DISTRIBUTION: Daily / Monthly PnL per symbol" + "-" * 26)
    print(f"  {'Symbol':<10}  {'Daily(cur)':>14}  {'Daily(flip)':>12}  {'Monthly(cur)':>14}  {'Monthly(flip)':>14}")
    print("  " + "-" * 72)

    rows = volume_distribution(cur, ts_today, ts_end, ts_month)
    total_daily = sum(r["daily_pnl_usd"] for r in rows)
    total_monthly = sum(r["monthly_pnl_usd"] for r in rows)
    for r in rows:
        d = r["daily_pnl_usd"]
        m = r["monthly_pnl_usd"]
        if abs(d) < 100 and abs(m) < 100:
            continue  # skip near-zero rows
        print(f"  {r['symbol']:<10}  {d:>14,.0f}  {-d:>12,.0f}  {m:>14,.0f}  {-m:>14,.0f}")
    print("  " + "-" * 72)
    print(f"  {'TOTAL':<10}  {total_daily:>14,.0f}  {-total_daily:>12,.0f}  {total_monthly:>14,.0f}  {-total_monthly:>14,.0f}")

    print("\n-- SUMMARY" + "-" * 70)
    print(f"  Client perspective (current):  Daily={fmt(daily_today)}  Monthly={fmt(monthly_pnl)}")
    print(f"  Broker perspective (flipped):  Daily={fmt(-daily_today)}  Monthly={fmt(-monthly_pnl)}")
    print()

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
