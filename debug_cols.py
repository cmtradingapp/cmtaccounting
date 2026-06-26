import psycopg2, psycopg2.extras
DSN = "postgresql://cro:bTiBZzbU2gtAfA5BfPdR5PFcpLqcteu@213.199.45.213:5432/cro_db"
conn = psycopg2.connect(DSN)
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='closed_positions' ORDER BY ordinal_position")
print("closed_positions:", [r['column_name'] for r in cur.fetchall()])
