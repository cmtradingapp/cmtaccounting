"""Check SOD query with (profit+storage) and current_float with storage."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "recon-app"))
import psycopg2, psycopg2.extras
from datetime import datetime, timedelta, timezone
from cro_metrics import closed_pnl_usd

DSN = "postgresql://cro:bTiBZzbU2gtAfA5BfPdR5PFcpLqcteu@213.199.45.213:5432/cro_db"
conn = psycopg2.connect(DSN)
conn.set_session(readonly=True, autocommit=True)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

now_utc     = datetime.now(timezone.utc)
today_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
month_start = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
prev_month_end = month_start - timedelta(days=1)
ts_month    = int(month_start.timestamp())
ts_pme      = int(prev_month_end.timestamp())
ts_end      = int(today_start.timestamp()) + 86400
month_date  = month_start.date()

settled_mtd = closed_pnl_usd(cur, ts_month, ts_end)

# SOD floating with (profit + storage)
cur.execute("""
    SELECT COALESCE(SUM(
        (ps.profit + ps.storage) *
        CASE WHEN COALESCE(a.currency,'USD')='USD' THEN 1.0
             WHEN ir.bid>0 AND ir.ask>0 THEN
               CASE WHEN ir.usd_base THEN 2.0/(ir.bid+ir.ask)
                    ELSE (ir.bid+ir.ask)/2.0 END
             ELSE 1.0 END
    ), 0) AS sod_float
    FROM positions_sod ps
    LEFT JOIN accounts_snapshot a ON a.login=ps.login
        AND NOT(a.balance=0 AND a.equity=0) AND a.group_name NOT ILIKE '%%test%%'
    LEFT JOIN internal_rates ir ON ir.currency=COALESCE(a.currency,'USD')
    WHERE ps.snapshot_date = %(d)s
""", {"d": month_date})
sod_float_with_storage = float((cur.fetchone() or {}).get("sod_float") or 0)

# Current float = profit + storage (what Flask now uses)
cur.execute("""
    SELECT COALESCE(SUM(
        (ps.profit + ps.storage) *
        CASE WHEN COALESCE(a.currency,'USD')='USD' THEN 1.0
             WHEN ir.bid>0 AND ir.ask>0 THEN
               CASE WHEN ir.usd_base THEN 2.0/(ir.bid+ir.ask)
                    ELSE (ir.bid+ir.ask)/2.0 END
             ELSE 1.0 END
    ), 0) AS cur_float
    FROM positions_snapshot ps
    LEFT JOIN accounts_snapshot a ON a.login=ps.login
        AND NOT(a.balance=0 AND a.equity=0) AND a.group_name NOT ILIKE '%%test%%'
    LEFT JOIN internal_rates ir ON ir.currency=COALESCE(a.currency,'USD')
    WHERE ps.symbol NOT ILIKE 'Zeroing%%' AND ps.symbol NOT ILIKE '%%inactivity%%'
""")
cur_float_with_storage = float((cur.fetchone() or {}).get("cur_float") or 0)

delta = cur_float_with_storage - sod_float_with_storage
monthly_vol = -(settled_mtd + delta)

print(f"sod_float  (profit+storage, USD-conv): {sod_float_with_storage:>14,.2f}")
print(f"cur_float  (profit+storage, USD-conv): {cur_float_with_storage:>14,.2f}")
print(f"delta_monthly:                         {delta:>14,.2f}")
print(f"settled_mtd:                           {settled_mtd:>14,.2f}")
print(f"vol monthly (broker):                  {monthly_vol:>14,.2f}")
print(f"card monthly (broker):                      433,407.38")

cur.close(); conn.close()
