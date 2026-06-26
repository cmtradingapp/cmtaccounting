"""Find where CRM user ID is stored in accounts_snapshot."""
import psycopg2, psycopg2.extras
from datetime import datetime, timezone

DSN = "postgresql://cro:bTiBZzbU2gtAfA5BfPdR5PFcpLqcteu@213.199.45.213:5432/cro_db"
conn = psycopg2.connect(DSN)
conn.set_session(readonly=True, autocommit=True)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# 1. What columns does accounts_snapshot have that look like CRM links?
cur.execute("""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'accounts_snapshot'
    ORDER BY ordinal_position
""")
print("=== accounts_snapshot columns ===")
for r in cur.fetchall():
    print(f"  {r['column_name']:<30} {r['data_type']}")

print()

# 2. Sample the candidate fields
cur.execute("""
    SELECT login, comment, client_id, crm_id, user_id
    FROM accounts_snapshot
    WHERE login IN (150005717, 141964544, 141827677, 141729480)
    LIMIT 10
""")
print("=== Sample accounts for logins seen in positions today ===")
for r in cur.fetchall():
    print(f"  login={r['login']}  comment={repr(str(r.get('comment',''))[:40])}  "
          f"client_id={r.get('client_id')}  crm_id={r.get('crm_id')}  user_id={r.get('user_id')}")

print()

# 3. Non-empty counts for candidate CRM fields
for field in ('comment', 'client_id', 'crm_id', 'user_id'):
    try:
        cur.execute(f"""
            SELECT COUNT(*) FILTER (WHERE {field} IS NOT NULL AND {field}::text NOT IN ('', '0'))
                AS non_empty,
                COUNT(*) AS total
            FROM accounts_snapshot
        """)
        r = cur.fetchone()
        print(f"  {field:<15}: non_empty={r['non_empty']}  total={r['total']}")
    except Exception as e:
        print(f"  {field}: ERROR {e}")

print()

# 4. Sample non-empty crm_id / client_id values
for field in ('crm_id', 'client_id', 'user_id'):
    try:
        cur.execute(f"""
            SELECT login, {field} AS val
            FROM accounts_snapshot
            WHERE {field}::text NOT IN ('', '0')
              AND {field} IS NOT NULL
            LIMIT 5
        """)
        rows = cur.fetchall()
        if rows:
            print(f"=== Sample {field} values ===")
            for r in rows:
                print(f"  login={r['login']}  {field}={repr(str(r['val'])[:60])}")
    except Exception as e:
        print(f"  {field}: {e}")

cur.close(); conn.close()
