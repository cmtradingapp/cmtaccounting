"""Compare card monthly PnL components vs vol table monthly PnL components.

Card uses daily_reports for month-start floating.
Vol table uses positions_sod backfill for month-start floating.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "recon-app"))

import psycopg2, psycopg2.extras
from datetime import datetime, timedelta, timezone
from cro_metrics import closed_pnl_usd, total_floating_usd, total_floating_usd_eod

DSN = "postgresql://cro:bTiBZzbU2gtAfA5BfPdR5PFcpLqcteu@213.199.45.213:5432/cro_db"

def main():
    conn = psycopg2.connect(DSN)
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    now_utc      = datetime.now(timezone.utc)
    today_start  = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    month_start  = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    prev_month_end = month_start - timedelta(days=1)

    ts_today   = int(today_start.timestamp())
    ts_month   = int(month_start.timestamp())
    ts_pme     = int(prev_month_end.timestamp())
    ts_end     = ts_today + 86400
    month_date = month_start.date()

    print(f"month_start={month_start.date()}  today={today_start.date()}")
    print()

    # 1. Card components
    float_today     = total_floating_usd(cur)
    float_month_st  = total_floating_usd_eod(cur, ts_pme, ts_month)
    settled_mtd     = closed_pnl_usd(cur, ts_month, ts_end)
    delta_mtd       = float_today - float_month_st
    card_monthly    = -(delta_mtd + settled_mtd)

    print("=== CARD monthly PnL breakdown ===")
    print(f"  float_today (accounts_snapshot.floating, USD):  {float_today:>14,.2f}")
    print(f"  float_month_st (daily_reports Apr30 EOD, USD):  {float_month_st:>14,.2f}")
    print(f"  delta_mtd  = float_today - float_month_st:      {delta_mtd:>14,.2f}")
    print(f"  settled_mtd (closed_positions MTD, client):     {settled_mtd:>14,.2f}")
    print(f"  card monthly PnL (broker):                      {card_monthly:>14,.2f}")
    print()

    # 2. Vol table SOD backfill floating at month-start
    cur.execute("""
        SELECT
            SUM(
                ps.profit *
                CASE WHEN COALESCE(a.currency,'USD')='USD' THEN 1.0
                     WHEN ir.bid>0 AND ir.ask>0 THEN
                       CASE WHEN ir.usd_base THEN 2.0/(ir.bid+ir.ask)
                            ELSE (ir.bid+ir.ask)/2.0 END
                     ELSE 1.0 END
            ) AS sod_floating,
            COUNT(*) AS n_rows,
            COUNT(DISTINCT ps.symbol) AS n_symbols
        FROM positions_sod ps
        LEFT JOIN accounts_snapshot a
            ON a.login = ps.login
            AND NOT (a.balance=0 AND a.equity=0)
            AND a.group_name NOT ILIKE '%%test%%'
        LEFT JOIN internal_rates ir ON ir.currency = COALESCE(a.currency,'USD')
        WHERE ps.snapshot_date = %(d)s
    """, {"d": month_date})
    row = cur.fetchone() or {}
    sod_float  = float(row.get("sod_floating") or 0)
    sod_rows   = int(row.get("n_rows") or 0)
    sod_syms   = int(row.get("n_symbols") or 0)

    # Current float from positions_snapshot.profit (what vol table uses for current_float)
    cur.execute("""
        SELECT SUM(
            ps.profit *
            CASE WHEN COALESCE(a.currency,'USD')='USD' THEN 1.0
                 WHEN ir.bid>0 AND ir.ask>0 THEN
                   CASE WHEN ir.usd_base THEN 2.0/(ir.bid+ir.ask)
                        ELSE (ir.bid+ir.ask)/2.0 END
                 ELSE 1.0 END
        ) AS cur_float
        FROM positions_snapshot ps
        LEFT JOIN accounts_snapshot a ON a.login=ps.login
            AND NOT(a.balance=0 AND a.equity=0) AND a.group_name NOT ILIKE '%%test%%'
        LEFT JOIN internal_rates ir ON ir.currency=COALESCE(a.currency,'USD')
        WHERE ps.symbol NOT ILIKE 'Zeroing%%' AND ps.symbol NOT ILIKE '%%inactivity%%'
    """)
    cur_float_profit_only = float((cur.fetchone() or {}).get("cur_float") or 0)

    delta_vol  = cur_float_profit_only - sod_float
    vol_monthly = -(settled_mtd + delta_vol)

    print("=== VOL TABLE monthly PnL breakdown ===")
    print(f"  sod_floating (positions_sod {month_date}, USD):  {sod_float:>14,.2f}  ({sod_rows} rows, {sod_syms} symbols)")
    print(f"  cur_float_profit_only (positions_snapshot):      {cur_float_profit_only:>14,.2f}")
    print(f"  delta_monthly = cur - sod:                       {delta_vol:>14,.2f}")
    print(f"  settled_mtd (same as card):                      {settled_mtd:>14,.2f}")
    print(f"  vol table monthly PnL (broker):                  {vol_monthly:>14,.2f}")
    print()

    print("=== COMPARISON ===")
    print(f"  Card monthly (daily_reports baseline): {card_monthly:>14,.2f}")
    print(f"  Vol  monthly (sod backfill baseline):  {vol_monthly:>14,.2f}")
    print(f"  Difference:                            {vol_monthly - card_monthly:>14,.2f}")
    print()
    print(f"  float_month_st  (daily_reports Apr30): {float_month_st:>14,.2f}")
    print(f"  sod_float       (backfill May1 00:00): {sod_float:>14,.2f}")
    print(f"  Baseline diff:                         {sod_float - float_month_st:>14,.2f}")
    print()
    print(f"  float_today  (accounts_snapshot.floating incl storage): {float_today:>14,.2f}")
    print(f"  cur_float    (positions_snapshot.profit only, no stor):  {cur_float_profit_only:>14,.2f}")
    print(f"  Storage in current floating:            {float_today - cur_float_profit_only:>14,.2f}")

    cur.close(); conn.close()

if __name__ == "__main__":
    main()
