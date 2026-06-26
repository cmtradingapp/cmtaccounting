"""Test Option A: scale positions_sod per-login by daily_reports actual floating.

For each login that has BOTH daily_reports (Apr30) AND positions_sod (May1):
    scale_factor = dr_float_native / sod_native
    scaled_sod_profit = sod_profit * scale_factor

This corrects the ChartRequest price-reference error per account, reducing
the baseline gap from ~$2.65M toward ~$0.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "recon-app"))
import psycopg2, psycopg2.extras
from datetime import datetime, timedelta, timezone

DSN = "postgresql://cro:bTiBZzbU2gtAfA5BfPdR5PFcpLqcteu@213.199.45.213:5432/cro_db"
conn = psycopg2.connect(DSN)
conn.set_session(readonly=True, autocommit=True)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

now_utc        = datetime.now(timezone.utc)
month_start    = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
prev_month_end = month_start - timedelta(days=1)
ts_month       = int(month_start.timestamp())
ts_pme         = int(prev_month_end.timestamp())
month_date     = month_start.date()

# ── Option A: scale SOD by daily_reports per login ─────────────────────────
cur.execute("""
    WITH dr_float AS (
        -- Per-login actual floating from daily_reports at Apr30 23:59 UTC
        -- in native account currency (profit + swaps)
        SELECT login,
               SUM(profit + profit_storage) AS dr_native
        FROM daily_reports
        WHERE datetime >= %(pme)s AND datetime < %(ms)s
          AND group_name NOT ILIKE '%%test%%'
          AND NOT (balance = 0 AND profit_equity = 0)
        GROUP BY login
    ),
    sod_per_login AS (
        -- Per-login sum of positions_sod in native account currency
        SELECT login, SUM(profit + storage) AS sod_native
        FROM positions_sod
        WHERE snapshot_date = %(snap)s
        GROUP BY login
    ),
    scaling AS (
        SELECT
            s.login,
            -- If both exist and sod_native != 0: scale to match DR actual floating.
            -- If no DR record: keep scale=1 (ChartRequest as-is).
            -- If sod_native=0: scale=1 (can't distribute).
            CASE
                WHEN d.dr_native IS NOT NULL AND s.sod_native != 0
                THEN d.dr_native::float / s.sod_native::float
                ELSE 1.0
            END AS scale_factor,
            d.dr_native IS NOT NULL AS has_dr,
            s.sod_native
        FROM sod_per_login s
        LEFT JOIN dr_float d ON d.login = s.login
    )
    SELECT
        ps.symbol,
        SUM(
            (ps.profit + ps.storage) * sc.scale_factor *
            CASE WHEN COALESCE(a.currency, 'USD') = 'USD' THEN 1.0
                 WHEN ir.bid > 0 AND ir.ask > 0 THEN
                   CASE WHEN ir.usd_base THEN 2.0/(ir.bid+ir.ask)
                        ELSE (ir.bid+ir.ask)/2.0 END
                 ELSE 1.0 END
        ) AS sod_scaled,
        COUNT(DISTINCT ps.login) AS n_logins,
        COUNT(*) FILTER (WHERE sc.has_dr) AS n_with_dr,
        COUNT(*) FILTER (WHERE NOT sc.has_dr) AS n_no_dr,
        -- Sanity: extreme scale factors
        SUM(CASE WHEN ABS(sc.scale_factor) > 10 THEN 1 ELSE 0 END) AS n_extreme_scale
    FROM positions_sod ps
    JOIN scaling sc ON sc.login = ps.login
    LEFT JOIN accounts_snapshot a
        ON a.login = ps.login
        AND NOT (a.balance = 0 AND a.equity = 0)
        AND a.group_name NOT ILIKE '%%test%%'
    LEFT JOIN internal_rates ir ON ir.currency = COALESCE(a.currency, 'USD')
    WHERE ps.snapshot_date = %(snap)s
      AND ps.symbol NOT ILIKE 'Zeroing%%'
      AND ps.symbol NOT ILIKE '%%inactivity%%'
    GROUP BY ps.symbol
    ORDER BY ABS(SUM((ps.profit+ps.storage)*sc.scale_factor)) DESC
""", {"pme": ts_pme, "ms": ts_month, "snap": month_date})
rows = cur.fetchall() or []

sod_a_total = sum(float(r["sod_scaled"] or 0) for r in rows)
float_month_st = -52_646_517.0  # from diagnostic

print("=== OPTION A — scaled SOD per symbol (top 15) ===")
print(f"{'Symbol':<12} {'sod_scaled':>14}  n_logins  with_DR  extreme_scale")
print("-" * 65)
for r in rows[:15]:
    print(f"  {r['symbol']:<10} {float(r['sod_scaled'] or 0):>14,.0f}  "
          f"{r['n_logins']:>8}  {r['n_with_dr']:>7}  {r['n_extreme_scale']:>13}")
print("-" * 65)
print(f"  {'TOTAL':<10} {sod_a_total:>14,.0f}")

print(f"\n=== BASELINE COMPARISON ===")
print(f"  float_month_st  (daily_reports Apr30, USD): {float_month_st:>14,.0f}")
print(f"  sod_float orig  (ChartRequest raw):         {-49_994_909:>14,.0f}")
print(f"  sod_float opt-A (DR-scaled):                {sod_a_total:>14,.0f}")
print(f"\n  Gap before Option A:  {float_month_st - (-49_994_909):>10,.0f}")
print(f"  Gap after  Option A:  {float_month_st - sod_a_total:>10,.0f}")
print(f"  Improvement:          {abs((float_month_st - (-49_994_909)) - (float_month_st - sod_a_total)):>10,.0f}")

# Coverage stats
cur.execute("""
    SELECT
        COUNT(DISTINCT ps.login) AS sod_logins,
        COUNT(DISTINCT CASE WHEN d.login IS NOT NULL THEN ps.login END) AS matched_logins,
        SUM(CASE WHEN d.login IS NOT NULL THEN ps.profit+ps.storage ELSE 0 END) AS matched_native,
        SUM(ps.profit+ps.storage) AS total_native
    FROM positions_sod ps
    LEFT JOIN (
        SELECT login FROM daily_reports
        WHERE datetime >= %(pme)s AND datetime < %(ms)s
          AND NOT (balance=0 AND profit_equity=0) AND group_name NOT ILIKE '%%test%%'
        GROUP BY login
    ) d ON d.login = ps.login
    WHERE ps.snapshot_date = %(snap)s
""", {"pme": ts_pme, "ms": ts_month, "snap": month_date})
cov = cur.fetchone() or {}
print(f"\n=== COVERAGE ===")
print(f"  SOD logins total:   {cov['sod_logins']}")
print(f"  Matched to DR:      {cov['matched_logins']}")
print(f"  Matched native:     {float(cov['matched_native'] or 0):>14,.0f}")
print(f"  Total native:       {float(cov['total_native'] or 0):>14,.0f}")
coverage_pct = float(cov['matched_logins'] or 0) / float(cov['sod_logins'] or 1) * 100
print(f"  Login coverage:     {coverage_pct:.1f}%")

# Scale factor distribution
cur.execute("""
    WITH dr_float AS (
        SELECT login, SUM(profit+profit_storage) AS dr_native
        FROM daily_reports
        WHERE datetime >= %(pme)s AND datetime < %(ms)s
          AND NOT (balance=0 AND profit_equity=0) AND group_name NOT ILIKE '%%test%%'
        GROUP BY login
    ),
    sod_per_login AS (
        SELECT login, SUM(profit+storage) AS sod_native
        FROM positions_sod WHERE snapshot_date = %(snap)s GROUP BY login
    )
    SELECT
        COUNT(*) FILTER (WHERE ABS(d.dr_native/NULLIF(s.sod_native,0)) > 2)  AS n_factor_gt2,
        COUNT(*) FILTER (WHERE ABS(d.dr_native/NULLIF(s.sod_native,0)) > 5)  AS n_factor_gt5,
        COUNT(*) FILTER (WHERE ABS(d.dr_native/NULLIF(s.sod_native,0)) > 10) AS n_factor_gt10,
        COUNT(*) FILTER (WHERE (d.dr_native/NULLIF(s.sod_native,0)) < 0)     AS n_negative_scale,
        COUNT(*) AS total_matched
    FROM sod_per_login s JOIN dr_float d ON d.login=s.login
""", {"pme": ts_pme, "ms": ts_month, "snap": month_date})
sf = cur.fetchone() or {}
print(f"\n=== SCALE FACTOR QUALITY ===")
print(f"  Total matched logins: {sf['total_matched']}")
print(f"  |scale| > 2:          {sf['n_factor_gt2']}")
print(f"  |scale| > 5:          {sf['n_factor_gt5']}")
print(f"  |scale| > 10:         {sf['n_factor_gt10']}")
print(f"  negative scale:       {sf['n_negative_scale']}  (ChartRequest sign flip)")

cur.close(); conn.close()
