import psycopg2
DSN = "postgresql://cro:bTiBZzbU2gtAfA5BfPdR5PFcpLqcteu@213.199.45.213:5432/cro_db"
conn = psycopg2.connect(DSN)
cur = conn.cursor()
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='positions_sod' ORDER BY ordinal_position")
print("positions_sod columns:", [r[0] for r in cur.fetchall()])
conn.close()
