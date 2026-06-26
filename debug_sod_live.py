"""Check what the SOD query actually returns on prod right now."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "recon-app"))

import psycopg2, psycopg2.extras
from datetime import datetime, date, timezone

DSN = "postgresql://cro:bTiBZzbU2gtAfA5BfPdR5PFcpLqcteu@213.199.45.213:5432/cro_db"

def main():
    conn = psycopg2.connect(DSN)
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    now_utc = datetime.now(timezone.utc)
    today_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    today_ts = int(today_start.timestamp())
    today_date = datetime.fromtimestamp(today_ts, tz=timezone.utc).date()
    print(f"today_ts={today_ts}  today_date={today_date}")

    # 1. SOD query exactly as cro_metrics.py runs it
    cur.execute("""
        SELECT ps.symbol,
               SUM(
                   ps.profit *
                   CASE WHEN COALESCE(a.currency, 'USD') = 'USD' THEN 1.0
                        WHEN ir_a.bid > 0 AND ir_a.ask > 0 THEN
                          CASE WHEN ir_a.usd_base THEN 2.0/(ir_a.bid + ir_a.ask)
                               ELSE (ir_a.bid + ir_a.ask) / 2.0 END
                        ELSE 1.0 END
               ) AS sod_floating
        FROM positions_sod ps
        LEFT JOIN accounts_snapshot a
            ON  a.login = ps.login
            AND NOT (a.balance = 0 AND a.equity = 0)
            AND a.group_name NOT ILIKE '%%test%%'
        LEFT JOIN internal_rates ir_a ON ir_a.currency = COALESCE(a.currency, 'USD')
        WHERE ps.snapshot_date = %(snap_date)s
          AND ps.symbol NOT ILIKE 'Zeroing%%'
          AND ps.symbol NOT ILIKE '%%inactivity%%'
        GROUP BY ps.symbol
    """, {"snap_date": today_date})
    sod_rows = cur.fetchall() or []
    print(f"\nSOD query returned {len(sod_rows)} symbols")

    # Show XAUUSD specifically
    xau_sod = next((r for r in sod_rows if r["symbol"] == "XAUUSD"), None)
    print(f"XAUUSD sod_floating = {float(xau_sod['sod_floating']):,.2f}" if xau_sod else "XAUUSD NOT in SOD results")

    # 2. Current floating for XAUUSD
    cur.execute("""
        SELECT SUM(
            ps.profit *
            CASE WHEN COALESCE(a.currency,'USD')='USD' THEN 1.0
                 WHEN ir_a.bid>0 AND ir_a.ask>0 THEN
                   CASE WHEN ir_a.usd_base THEN 2.0/(ir_a.bid+ir_a.ask)
                        ELSE (ir_a.bid+ir_a.ask)/2.0 END
                 ELSE 1.0 END
        ) AS floating_pnl
        FROM positions_snapshot ps
        LEFT JOIN accounts_snapshot a ON a.login=ps.login
            AND NOT(a.balance=0 AND a.equity=0) AND a.group_name NOT ILIKE '%%test%%'
        LEFT JOIN internal_rates ir_a ON ir_a.currency=COALESCE(a.currency,'USD')
        WHERE ps.symbol='XAUUSD'
    """)
    cur_floating = float((cur.fetchone() or {}).get("floating_pnl") or 0)
    print(f"XAUUSD current_floating = {cur_floating:,.2f}")

    if xau_sod:
        sod_f = float(xau_sod["sod_floating"])
        delta = cur_floating - sod_f
        print(f"XAUUSD delta_floating   = {delta:,.2f}  (current - sod)")

    # 3. XAUUSD settled today
    cur.execute("""
        SELECT SUM(profit+storage+commission+fee) AS settled
        FROM closed_positions
        WHERE symbol='XAUUSD' AND close_time >= %(t)s AND close_time < %(te)s
    """, {"t": today_ts, "te": today_ts + 86400})
    settled = float((cur.fetchone() or {}).get("settled") or 0)
    print(f"XAUUSD settled_today    = {settled:,.2f}")
    if xau_sod:
        print(f"XAUUSD expected daily   = {settled + delta:,.2f}")

    cur.close(); conn.close()

if __name__ == "__main__":
    main()
