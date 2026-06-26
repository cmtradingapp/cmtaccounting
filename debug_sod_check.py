"""Quick check: does positions_sod exist and have data on prod?"""
import psycopg2, psycopg2.extras
DSN = "postgresql://cro:bTiBZzbU2gtAfA5BfPdR5PFcpLqcteu@213.199.45.213:5432/cro_db"
conn = psycopg2.connect(DSN)
conn.set_session(readonly=True, autocommit=True)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# 1. Does the table exist?
cur.execute("SELECT to_regclass('positions_sod') AS t")
print("positions_sod exists:", cur.fetchone()["t"])

# 2. How many rows, which dates?
try:
    cur.execute("SELECT snapshot_date, COUNT(*) AS n FROM positions_sod GROUP BY snapshot_date ORDER BY snapshot_date DESC LIMIT 5")
    rows = cur.fetchall()
    print("Rows by date:", [(str(r["snapshot_date"]), r["n"]) for r in rows] or "EMPTY")
except Exception as e:
    print("Query error:", e)

cur.close(); conn.close()
