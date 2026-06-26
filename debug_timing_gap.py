"""How many positions closed BETWEEN daily_reports EOD (~21:00 UTC Apr30)
and our backfill target (midnight UTC May 1)?  Those positions are in
daily_reports but NOT in positions_sod, creating a systematic gap.
"""
import psycopg2, psycopg2.extras
DSN = "postgresql://cro:bTiBZzbU2gtAfA5BfPdR5PFcpLqcteu@213.199.45.213:5432/cro_db"
conn = psycopg2.connect(DSN)
conn.set_session(readonly=True, autocommit=True)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# Midnight UTC May 1
may1_midnight = 1777593600

# Test several candidate EOD times (broker is UTC+2/+3)
for hours_before in (1, 2, 3, 4, 5, 6, 7):
    eod_ts = may1_midnight - hours_before * 3600
    cur.execute("""
        SELECT COUNT(*) AS n_closed,
               SUM(profit + storage + commission + fee) AS settled
        FROM closed_positions
        WHERE close_time >= %(eod)s AND close_time < %(mid)s
          AND symbol NOT ILIKE 'Zeroing%%' AND symbol NOT ILIKE '%%inactivity%%'
    """, {"eod": eod_ts, "mid": may1_midnight})
    r = cur.fetchone() or {}
    print(f"  Positions closed in [{hours_before}h before midnight, midnight): "
          f"n={r['n_closed']}  settled_sum={float(r['settled'] or 0):>12,.2f}")

# Also check what the broker EOD time is by looking at daily_reports datetime distribution
print("\nSample daily_reports datetimes for Apr30:")
cur.execute("""
    SELECT datetime, COUNT(*) AS n
    FROM daily_reports
    WHERE datetime >= 1777507200 AND datetime < 1777593600
    GROUP BY datetime ORDER BY datetime DESC LIMIT 5
""")
for r in cur.fetchall():
    from datetime import datetime, timezone
    dt = datetime.fromtimestamp(r['datetime'], tz=timezone.utc)
    print(f"  {dt.strftime('%Y-%m-%d %H:%M UTC')} ({r['datetime']})  n={r['n']}")

cur.close(); conn.close()
