"""Investigate what fields in accounts_snapshot could give office/agent breakdown."""
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

# 1. All columns in accounts_snapshot
print("=== accounts_snapshot columns ===")
cur.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'accounts_snapshot'
    ORDER BY ordinal_position
""")
for r in cur.fetchall():
    print(f"  {r['column_name']:<30} {r['data_type']}")

print()

# 2. Sample group_name patterns
print("=== group_name distribution ===")
cur.execute("""
    SELECT group_name, COUNT(*) AS n
    FROM accounts_snapshot
    WHERE group_name NOT ILIKE '%%test%%'
    GROUP BY group_name
    ORDER BY n DESC
    LIMIT 20
""")
for r in cur.fetchall():
    print(f"  {r['group_name']:<50} n={r['n']}")

print()

# 3. agent field — what does it contain?
print("=== agent field samples (non-zero) ===")
cur.execute("""
    SELECT login, agent, group_name, comment
    FROM accounts_snapshot
    WHERE agent IS NOT NULL AND agent != 0
    LIMIT 20
""")
for r in cur.fetchall():
    print(f"  login={r['login']}  agent={r['agent']}  group={r['group_name']!r}  crm={r['comment']!r}")

print()
cur.execute("""
    SELECT COUNT(*) AS total,
           COUNT(*) FILTER (WHERE agent IS NOT NULL AND agent != 0) AS with_agent
    FROM accounts_snapshot WHERE group_name NOT ILIKE '%%test%%'
""")
r = cur.fetchone()
print(f"  agent coverage: {r['with_agent']}/{r['total']}")

print()

# 4. lead_source / lead_campaign
print("=== lead_source distribution (top 15) ===")
cur.execute("""
    SELECT lead_source, COUNT(*) AS n FROM accounts_snapshot
    WHERE lead_source IS NOT NULL AND lead_source != ''
    GROUP BY lead_source ORDER BY n DESC LIMIT 15
""")
for r in cur.fetchall():
    print(f"  {r['lead_source']!r:<50} n={r['n']}")

print()

# 5. Among today's active traders — what groups/agents do they belong to?
print("=== Today's active traders — group_name breakdown ===")
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
    SELECT a.group_name, COUNT(DISTINCT al.login) AS logins,
           COUNT(DISTINCT CASE WHEN a.comment != '' AND a.comment != '0'
                               THEN a.comment ELSE al.login::text END) AS crm_users
    FROM active_logins al
    LEFT JOIN accounts_snapshot a ON a.login = al.login
    GROUP BY a.group_name
    ORDER BY crm_users DESC
    LIMIT 20
""", {"t": ts_today, "te": ts_end})
for r in cur.fetchall():
    print(f"  {str(r['group_name']):<50} logins={r['logins']}  crm_users={r['crm_users']}")

print()

# 6. Is there an "agent" account we can look up for agent name?
print("=== Agent lookup — do agent IDs map back to accounts? ===")
cur.execute("""
    SELECT a.login, a.agent, b.login AS agent_login, b.group_name AS agent_group,
           b.comment AS agent_crm
    FROM accounts_snapshot a
    JOIN accounts_snapshot b ON b.login = a.agent
    WHERE a.agent IS NOT NULL AND a.agent != 0
    LIMIT 10
""")
for r in cur.fetchall():
    print(f"  client={r['login']}  agent_login={r['agent_login']}  agent_group={r['agent_group']!r}  agent_crm={r['agent_crm']!r}")

cur.close(); conn.close()
