"""
Active Traders (distinct CRM users) who opened a position today.

Logic:
  1. Get all logins that opened a position today from:
     - positions_snapshot (time_create >= today)
     - closed_positions   (open_time  >= today)
  2. Join those logins to accounts_snapshot to get the CRM user ID (comment field)
  3. COUNT(DISTINCT comment) = distinct CRM users
"""
import psycopg2, psycopg2.extras
from datetime import datetime, timezone

DSN = "postgresql://cro:bTiBZzbU2gtAfA5BfPdR5PFcpLqcteu@213.199.45.213:5432/cro_db"
conn = psycopg2.connect(DSN)
conn.set_session(readonly=True, autocommit=True)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

now_utc     = datetime.now(timezone.utc)
today_start = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
ts_today    = int(today_start.timestamp())
ts_end      = ts_today + 86400

print(f"Today: {today_start.strftime('%Y-%m-%d')}  [{ts_today}, {ts_end})")
print()

# ── 1. Inspect comment field in accounts_snapshot
print("=== accounts_snapshot.comment samples ===")
cur.execute("""
    SELECT login, comment
    FROM accounts_snapshot
    WHERE comment IS NOT NULL AND comment != '' AND comment != '0'
    LIMIT 20
""")
for r in cur.fetchall():
    print(f"  login={r['login']}  comment={repr(r['comment'][:60])}")

print()
cur.execute("""
    SELECT
        COUNT(*) AS total,
        COUNT(*) FILTER (WHERE comment IS NOT NULL AND comment != '' AND comment != '0') AS with_crm_comment,
        COUNT(DISTINCT comment) FILTER (WHERE comment IS NOT NULL AND comment != '' AND comment != '0') AS distinct_comments
    FROM accounts_snapshot
""")
r = cur.fetchone()
print(f"accounts_snapshot: total={r['total']}  with_crm_comment={r['with_crm_comment']}  distinct_crm={r['distinct_comments']}")

print()

# ── 2. Logins that opened positions today (from both tables)
print("=== Logins that opened positions today ===")
cur.execute("""
    SELECT COUNT(DISTINCT login) AS n_logins FROM positions_snapshot
    WHERE time_create >= %(t)s AND time_create < %(te)s
      AND symbol NOT ILIKE 'Zeroing%%' AND symbol NOT ILIKE '%%inactivity%%'
""", {"t": ts_today, "te": ts_end})
print(f"  From positions_snapshot (still open):  {cur.fetchone()['n_logins']} logins")

cur.execute("""
    SELECT COUNT(DISTINCT login) AS n_logins FROM closed_positions
    WHERE open_time >= %(t)s AND open_time < %(te)s
      AND symbol NOT ILIKE 'Zeroing%%' AND symbol NOT ILIKE '%%inactivity%%'
""", {"t": ts_today, "te": ts_end})
print(f"  From closed_positions  (opened+closed): {cur.fetchone()['n_logins']} logins")

cur.execute("""
    SELECT COUNT(DISTINCT login) AS n_logins FROM (
        SELECT login FROM positions_snapshot
        WHERE time_create >= %(t)s AND time_create < %(te)s
          AND symbol NOT ILIKE 'Zeroing%%' AND symbol NOT ILIKE '%%inactivity%%'
        UNION
        SELECT login FROM closed_positions
        WHERE open_time >= %(t)s AND open_time < %(te)s
          AND symbol NOT ILIKE 'Zeroing%%' AND symbol NOT ILIKE '%%inactivity%%'
    ) combined
""", {"t": ts_today, "te": ts_end})
print(f"  UNION (distinct MT5 logins):           {cur.fetchone()['n_logins']} logins")

print()

# ── 3. Join to accounts_snapshot → distinct CRM users
cur.execute("""
    WITH active_logins AS (
        SELECT DISTINCT login FROM positions_snapshot
        WHERE time_create >= %(t)s AND time_create < %(te)s
          AND symbol NOT ILIKE 'Zeroing%%' AND symbol NOT ILIKE '%%inactivity%%'
        UNION
        SELECT DISTINCT login FROM closed_positions
        WHERE open_time >= %(t)s AND open_time < %(te)s
          AND symbol NOT ILIKE 'Zeroing%%' AND symbol NOT ILIKE '%%inactivity%%'
    )
    SELECT
        COUNT(DISTINCT a.login)   AS distinct_mt5_logins,
        COUNT(DISTINCT
            CASE WHEN a.comment IS NOT NULL AND a.comment != '' AND a.comment != '0'
                 THEN a.comment ELSE a.login::text END
        ) AS distinct_crm_users,
        COUNT(DISTINCT a.comment) FILTER (
            WHERE a.comment IS NOT NULL AND a.comment != '' AND a.comment != '0'
        ) AS with_comment,
        COUNT(al.login) FILTER (WHERE a.comment IS NULL OR a.comment IN ('', '0'))
            AS no_crm_comment
    FROM active_logins al
    LEFT JOIN accounts_snapshot a ON a.login = al.login
""", {"t": ts_today, "te": ts_end})
r = cur.fetchone()
print("=== Distinct CRM active traders today ===")
print(f"  Distinct MT5 logins:             {r['distinct_mt5_logins']}")
print(f"  With CRM comment:                {r['with_comment']}")
print(f"  Without CRM comment (fallback):  {r['no_crm_comment']}")
print(f"  Distinct CRM users (total):      {r['distinct_crm_users']}")

print()

# ── 4. Breakdown: how many MT5 accounts share the same CRM comment?
cur.execute("""
    WITH active_logins AS (
        SELECT DISTINCT login FROM positions_snapshot
        WHERE time_create >= %(t)s AND time_create < %(te)s
          AND symbol NOT ILIKE 'Zeroing%%' AND symbol NOT ILIKE '%%inactivity%%'
        UNION
        SELECT DISTINCT login FROM closed_positions
        WHERE open_time >= %(t)s AND open_time < %(te)s
          AND symbol NOT ILIKE 'Zeroing%%' AND symbol NOT ILIKE '%%inactivity%%'
    )
    SELECT a.comment, COUNT(DISTINCT al.login) AS n_mt5_accounts
    FROM active_logins al
    JOIN accounts_snapshot a ON a.login = al.login
    WHERE a.comment IS NOT NULL AND a.comment != '' AND a.comment != '0'
    GROUP BY a.comment
    HAVING COUNT(DISTINCT al.login) > 1
    ORDER BY n_mt5_accounts DESC
    LIMIT 15
""", {"t": ts_today, "te": ts_end})
rows = cur.fetchall()
if rows:
    print("=== CRM users with multiple active MT5 accounts today ===")
    for r in rows:
        print(f"  CRM={repr(r['comment'][:40]):<45} mt5_accounts={r['n_mt5_accounts']}")
else:
    print("  No CRM users with multiple MT5 accounts active today.")

cur.close(); conn.close()
