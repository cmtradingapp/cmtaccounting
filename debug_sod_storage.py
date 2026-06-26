"""Check positions_sod storage values for May 1."""
import psycopg2, psycopg2.extras
DSN = "postgresql://cro:bTiBZzbU2gtAfA5BfPdR5PFcpLqcteu@213.199.45.213:5432/cro_db"
conn = psycopg2.connect(DSN)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

cur.execute("""
    SELECT snapshot_date,
           COUNT(*) AS n_rows,
           SUM(CASE WHEN storage = 0 THEN 1 ELSE 0 END) AS storage_zero,
           SUM(CASE WHEN storage != 0 THEN 1 ELSE 0 END) AS storage_nonzero,
           SUM(storage) AS total_storage
    FROM positions_sod
    GROUP BY snapshot_date ORDER BY snapshot_date DESC
""")
for r in cur.fetchall():
    print(f"  {r['snapshot_date']}  rows={r['n_rows']}  "
          f"storage_zero={r['storage_zero']}  storage_nonzero={r['storage_nonzero']}  "
          f"total_storage={float(r['total_storage'] or 0):,.2f}")
cur.close(); conn.close()
