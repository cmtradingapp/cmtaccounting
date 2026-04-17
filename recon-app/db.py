import os
import sqlite3
import psycopg2
import psycopg2.extras
import pymssql
from contextlib import contextmanager

# ── Fee DB mode ────────────────────────────────────────────────────────────
# FEES_MODE=demo  → local SQLite (default, zero-config)
# FEES_MODE=live  → PostgreSQL container (fees_postgres service)
FEES_MODE = os.environ.get("FEES_MODE", "demo").lower()


# ── PostgreSQL adapter ─────────────────────────────────────────────────────
class _PgConnAdapter:
    """
    Makes a psycopg2 connection behave like sqlite3.Connection as used in
    queries.py — so all CRUD code works unchanged in LIVE mode.

    Key translations:
    - execute(sql, params): replaces ? → %s placeholders, returns cursor
    - executemany(sql, seq): same placeholder swap
    - executescript(sql): splits on ; and executes each statement
    - fetchone() / fetchall(): delegated to internal RealDictCursor
    - commit() / rollback() / close(): delegated to connection
    """

    def __init__(self, conn):
        self._conn = conn
        self._cur  = psycopg2.extras.RealDictCursor(conn)

    # ── Core execute interface ─────────────────────────────────────────────
    @staticmethod
    def _translate(sql, params):
        """Translate SQLite SQL idioms to PostgreSQL equivalents."""
        import re
        s = sql

        # Named params :foo → %(foo)s (used with dict params)
        if isinstance(params, dict):
            s = re.sub(r':(\w+)', r'%(\1)s', s)
        else:
            s = s.replace("?", "%s")

        # INSERT OR IGNORE → INSERT … ON CONFLICT DO NOTHING
        s = re.sub(r'(?i)\bINSERT\s+OR\s+IGNORE\b', 'INSERT', s)
        if re.search(r'(?i)\bINSERT\b', s) and 'ON CONFLICT' not in s.upper():
            # Only add if we stripped OR IGNORE
            if 'INSERT OR IGNORE' in sql.upper():
                s = s.rstrip().rstrip(';') + ' ON CONFLICT DO NOTHING'

        # INSERT OR REPLACE → INSERT … ON CONFLICT DO NOTHING
        # (full upsert would need column list; DO NOTHING is safe for our cache use)
        s = re.sub(r'(?i)\bINSERT\s+OR\s+REPLACE\b', 'INSERT', s)
        if 'INSERT OR REPLACE' in sql.upper() and 'ON CONFLICT' not in s.upper():
            s = s.rstrip().rstrip(';') + ' ON CONFLICT DO NOTHING'

        return s

    def execute(self, sql, params=None):
        pg_sql = self._translate(sql, params)
        is_insert = pg_sql.strip().upper().startswith("INSERT")
        # Append RETURNING id for INSERT so lastrowid works
        if is_insert and "RETURNING" not in pg_sql.upper():
            pg_sql = pg_sql.rstrip().rstrip(";") + " RETURNING id"
        self._cur.execute(pg_sql, params or ())
        if is_insert:
            try:
                row = self._cur.fetchone()
                self._last_id = row["id"] if row else None
            except Exception:
                self._last_id = None
        return self._cur

    def executemany(self, sql, seq):
        seq = list(seq)
        params0 = seq[0] if seq else None
        pg_sql = self._translate(sql, params0)
        self._cur.executemany(pg_sql, seq)
        return self._cur

    def executescript(self, sql):
        """Run multiple ';'-separated statements (psycopg2 doesn't support multi-stmt)."""
        for stmt in (s.strip() for s in sql.split(";") if s.strip()):
            self._cur.execute(stmt)

    # ── Fetch shortcuts (mirror sqlite3 cursor API) ────────────────────────
    def fetchone(self):   return self._cur.fetchone()
    def fetchall(self):   return self._cur.fetchall()

    @property
    def lastrowid(self):
        return getattr(self, "_last_id", None)

    @property
    def description(self): return self._cur.description

    # ── Transaction control ────────────────────────────────────────────────
    def commit(self):   self._conn.commit()
    def rollback(self): self._conn.rollback()
    def close(self):
        try:
            self._cur.close()
        finally:
            self._conn.close()


# ── Connection factories ───────────────────────────────────────────────────

def _conn_dealio():
    _certs = os.path.join(os.path.dirname(__file__), "certs")
    _ssl = {}
    if os.path.isdir(_certs):
        _ssl = {
            "sslmode": "require",          # client cert auth; server cert not verified (ca.crt is client CA)
            "sslcert": os.path.join(_certs, "client.crt"),
            "sslkey":  os.path.join(_certs, "client.key"),
        }
    return psycopg2.connect(
        host=os.environ["DEALIO_HOST"],
        port=int(os.environ.get("DEALIO_PORT", 5106)),
        dbname=os.environ["DEALIO_DB"],
        user=os.environ["DEALIO_USER"],
        password=os.environ["DEALIO_PASS"],
        connect_timeout=30,
        **_ssl,
    )

def _conn_praxis():
    return psycopg2.connect(
        host=os.environ["PRAXIS_HOST"],
        port=int(os.environ.get("PRAXIS_PORT", 5432)),
        dbname=os.environ["PRAXIS_DB"],
        user=os.environ["PRAXIS_USER"],
        password=os.environ["PRAXIS_PASS"],
        connect_timeout=10,
    )

def _conn_crm():
    """Antelope CRM — Azure SQL (MSSQL). Direct TLS connection, no SSH tunnel needed."""
    return pymssql.connect(
        server=os.environ["CRM_HOST"],
        port=int(os.environ.get("CRM_PORT", 1433)),
        database=os.environ["CRM_DB"],
        user=os.environ["CRM_USER"],
        password=os.environ["CRM_PASS"],
        tds_version="7.4",   # required for Azure SQL
        login_timeout=10,
        timeout=120,         # 2-min query timeout — prevents indefinite hangs
    )

def _conn_fees_pg():
    return psycopg2.connect(
        host=os.environ["FEES_PG_HOST"],
        port=int(os.environ.get("FEES_PG_PORT", 5432)),
        dbname=os.environ.get("FEES_PG_DB", "fees"),
        user=os.environ["FEES_PG_USER"],
        password=os.environ["FEES_PG_PASS"],
        connect_timeout=10,
    )


# ── Context managers ───────────────────────────────────────────────────────

@contextmanager
def dealio():
    conn = _conn_dealio()
    try:
        yield psycopg2.extras.RealDictCursor(conn)
    finally:
        conn.close()

@contextmanager
def praxis():
    """Read-only connection to the Praxis operations database."""
    conn = _conn_praxis()
    try:
        yield psycopg2.extras.RealDictCursor(conn)
    finally:
        conn.close()

@contextmanager
def crm():
    """Read-only connection to Antelope CRM (Azure SQL / MSSQL).
    Direct TLS connection to cmtmainserver.database.windows.net — no SSH tunnel.
    Table: report.vtiger_mttransactions (deposits/withdrawals with PSP transaction IDs).
    """
    conn = _conn_crm()
    try:
        yield conn.cursor(as_dict=True)
    finally:
        conn.close()

_FEES_DB = os.path.join(os.path.dirname(__file__), "fees.db")

@contextmanager
def fees_db():
    """
    PSP fee management database connection.

    FEES_MODE=demo (default) → SQLite fees.db  (zero-config, local file)
    FEES_MODE=live            → PostgreSQL fees_postgres container
    """
    if FEES_MODE == "live":
        conn    = _conn_fees_pg()
        adapter = _PgConnAdapter(conn)
        try:
            yield adapter
            adapter.commit()
        except Exception:
            adapter.rollback()
            raise
        finally:
            adapter.close()
    else:
        conn = sqlite3.connect(_FEES_DB)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
