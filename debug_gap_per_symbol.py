"""Per-symbol breakdown of the baseline gap: float_month_st vs sod_float.

For each symbol: what is its sod_float and what should it be
(estimated from daily_reports total proportionally, or directly computed).
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "recon-app"))
import psycopg2, psycopg2.extras
from datetime import datetime, timedelta, timezone

DSN = "postgresql://cro:bTiBZzbU2gtAfA5BfPdR5PFcpLqcteu@213.199.45.213:5432/cro_db"
conn = psycopg2.connect(DSN)
conn.set_session(readonly=True, autocommit=True)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

now_utc      = datetime.now(timezone.utc)
month_start  = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
prev_month_end = month_start - timedelta(days=1)
ts_month     = int(month_start.timestamp())
ts_pme       = int(prev_month_end.timestamp())
month_date   = month_start.date()

# Per-symbol sod_float (backfill)
cur.execute("""
    SELECT ps.symbol,
           SUM((ps.profit + ps.storage) *
               CASE WHEN COALESCE(a.currency,'USD')='USD' THEN 1.0
                    WHEN ir.bid>0 AND ir.ask>0 THEN
                      CASE WHEN ir.usd_base THEN 2.0/(ir.bid+ir.ask)
                           ELSE (ir.bid+ir.ask)/2.0 END
                    ELSE 1.0 END) AS sod_float,
           COUNT(*) AS n_pos
    FROM positions_sod ps
    LEFT JOIN accounts_snapshot a ON a.login=ps.login
        AND NOT(a.balance=0 AND a.equity=0) AND a.group_name NOT ILIKE '%%test%%'
    LEFT JOIN internal_rates ir ON ir.currency=COALESCE(a.currency,'USD')
    WHERE ps.snapshot_date = %(d)s
      AND ps.symbol NOT ILIKE 'Zeroing%%' AND ps.symbol NOT ILIKE '%%inactivity%%'
    GROUP BY ps.symbol ORDER BY ABS(SUM(ps.profit + ps.storage)) DESC
""", {"d": month_date})
sod_by_sym = {r["symbol"]: r for r in (cur.fetchall() or [])}

# Per-symbol current float (positions_snapshot profit+storage)
cur.execute("""
    SELECT ps.symbol,
           SUM((ps.profit + ps.storage) *
               CASE WHEN COALESCE(a.currency,'USD')='USD' THEN 1.0
                    WHEN ir.bid>0 AND ir.ask>0 THEN
                      CASE WHEN ir.usd_base THEN 2.0/(ir.bid+ir.ask)
                           ELSE (ir.bid+ir.ask)/2.0 END
                    ELSE 1.0 END) AS cur_float
    FROM positions_snapshot ps
    LEFT JOIN accounts_snapshot a ON a.login=ps.login
        AND NOT(a.balance=0 AND a.equity=0) AND a.group_name NOT ILIKE '%%test%%'
    LEFT JOIN internal_rates ir ON ir.currency=COALESCE(a.currency,'USD')
    WHERE ps.symbol NOT ILIKE 'Zeroing%%' AND ps.symbol NOT ILIKE '%%inactivity%%'
    GROUP BY ps.symbol
""")
cur_by_sym = {r["symbol"]: float(r["cur_float"] or 0) for r in (cur.fetchall() or [])}

# Also get the daily_reports total at Apr30 per login, then approximate per symbol
# using sod proportional share
float_month_st_total = -52_646_517.0  # from diagnostic
sod_total = sum(float(r["sod_float"] or 0) for r in sod_by_sym.values())

print(f"{'Symbol':<12} {'cur_float':>14} {'sod_float':>14} {'delta':>12} {'sod%':>6}")
print("-" * 62)
all_syms = sorted(set(sod_by_sym) | set(cur_by_sym),
                  key=lambda s: abs(cur_by_sym.get(s, 0)), reverse=True)
total_delta = 0
for sym in all_syms[:25]:
    cur_f = cur_by_sym.get(sym, 0)
    sod_r = sod_by_sym.get(sym, {})
    sod_f = float(sod_r.get("sod_float") or 0) if sod_r else 0
    delta = cur_f - sod_f
    total_delta += delta
    sod_pct = (sod_f / float_month_st_total * 100) if float_month_st_total else 0
    print(f"{sym:<12} {cur_f:>14,.0f} {sod_f:>14,.0f} {delta:>12,.0f} {sod_pct:>5.1f}%")

print("-" * 62)
print(f"{'TOTAL':<12} {sum(cur_by_sym.values()):>14,.0f} {sod_total:>14,.0f}")
print(f"\nfloat_month_st (daily_reports): {float_month_st_total:>14,.0f}")
print(f"sod_total (backfill):           {sod_total:>14,.0f}")
print(f"Baseline gap:                   {float_month_st_total - sod_total:>14,.0f}")

# Which accounts are in daily_reports Apr30 but NOT represented in positions_sod?
cur.execute("""
    SELECT COUNT(DISTINCT login) AS n_logins,
           SUM(d.profit + d.profit_storage) AS total_float
    FROM daily_reports d
    LEFT JOIN internal_rates ir ON ir.currency=d.currency
    WHERE d.datetime >= %(from_ts)s AND d.datetime < %(to_ts)s
      AND d.group_name NOT ILIKE '%%test%%'
      AND NOT (d.balance=0 AND d.profit_equity=0)
""", {"from_ts": ts_pme, "to_ts": ts_month})
dr = cur.fetchone() or {}
print(f"\ndaily_reports Apr30 logins: {dr.get('n_logins')}  raw_total: {float(dr.get('total_float') or 0):,.0f}")

cur.execute("SELECT COUNT(DISTINCT login) FROM positions_sod WHERE snapshot_date = %(d)s",
            {"d": month_date})
print(f"positions_sod May1 logins:  {(cur.fetchone() or {}).get('count')}")

cur.close(); conn.close()
