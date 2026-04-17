"""Probe the Dealio replica from the recon-app container — lists tables and
columns for the tables we expect the CRO dashboard to read.
"""
from __future__ import annotations

from pathlib import Path
import paramiko


def _server_creds():
    """Read putty-creds.txt from the repo root (gitignored)."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        f = parent / "putty-creds.txt"
        if f.exists():
            kv = {}
            for line in f.read_text().splitlines():
                if ":" in line:
                    k, v = line.split(":", 1)
                    kv[k.strip().lower()] = v.strip()
            return kv["host"], kv["username"], kv["password"]
    raise SystemExit("putty-creds.txt not found in repo tree")


HOST, USER, PASS = _server_creds()

REMOTE_PY = r'''
import os, psycopg2
certs = "/app/certs"
kw = dict(
    host=os.environ["DEALIO_HOST"],
    port=int(os.environ.get("DEALIO_PORT", 5106)),
    dbname=os.environ["DEALIO_DB"],
    user=os.environ["DEALIO_USER"],
    password=os.environ["DEALIO_PASS"],
    connect_timeout=20,
)
if os.path.isdir(certs):
    kw.update(sslmode="require",
              sslcert=f"{certs}/client.crt",
              sslkey=f"{certs}/client.key")
conn = psycopg2.connect(**kw)
cur = conn.cursor()

WANT = ("users","positions","trades_mt5","trades_mt4","daily_profits")
LIKE = ["%deal%","%trade%","%position%","%profit%","%user%"]

cur.execute("SELECT table_schema, table_name FROM information_schema.tables "
            " WHERE table_schema NOT IN ('pg_catalog','information_schema') "
            "   AND (table_name = ANY(%s) OR table_name ILIKE ANY(%s)) "
            " ORDER BY table_schema, table_name", (list(WANT), LIKE))
rows = cur.fetchall()
print("=== Matching tables ===")
for r in rows: print("  " + r[0] + "." + r[1])

for schema, name in rows:
    if name not in WANT: continue
    cur.execute("SELECT column_name, data_type FROM information_schema.columns "
                " WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
                (schema, name))
    cols = cur.fetchall()
    print()
    print("=== " + schema + "." + name + " (" + str(len(cols)) + " cols) ===")
    for c in cols: print("  " + c[0].ljust(40) + " " + c[1])

for schema, name in rows:
    if name not in WANT: continue
    try:
        cur.execute("SELECT * FROM " + schema + "." + name + " LIMIT 1")
        row = cur.fetchone()
        if not row: continue
        cur2 = conn.cursor()
        cur2.execute("SELECT column_name FROM information_schema.columns "
                     " WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position",
                     (schema, name))
        colnames = [c[0] for c in cur2.fetchall()]
        print()
        print("=== " + schema + "." + name + " sample row ===")
        for k, v in zip(colnames, row):
            if isinstance(v,(bytes,bytearray)): v = "<bytes>"
            print("  " + k.ljust(35) + " = " + repr(v))
    except Exception as e:
        print("  sample fail on " + schema + "." + name + ": " + str(e))
        conn.rollback()

conn.close()
'''


def _shell_quote(s: str) -> str:
    return "'" + s.replace("'", "'\\''") + "'"


def main() -> int:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(HOST, username=USER, password=PASS, timeout=15)
    cmd = "docker exec -i recon-app-recon-1 python -c " + _shell_quote(REMOTE_PY)
    _, stdout, stderr = client.exec_command(cmd, timeout=180)
    print(stdout.read().decode("utf-8", errors="replace"))
    err = stderr.read().decode("utf-8", errors="replace")
    if err:
        print("\n--- stderr ---")
        print(err)
    client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
