import os
import psycopg2
import psycopg2.extras
from contextlib import contextmanager

def _conn_dealio():
    return psycopg2.connect(
        host=os.environ["DEALIO_HOST"],
        port=int(os.environ.get("DEALIO_PORT", 5106)),
        dbname=os.environ["DEALIO_DB"],
        user=os.environ["DEALIO_USER"],
        password=os.environ["DEALIO_PASS"],
        connect_timeout=10,
    )

def _conn_backoffice():
    return psycopg2.connect(
        host=os.environ["BACKOFFICE_HOST"],
        port=int(os.environ.get("BACKOFFICE_PORT", 5432)),
        dbname=os.environ["BACKOFFICE_DB"],
        user=os.environ["BACKOFFICE_USER"],
        password=os.environ["BACKOFFICE_PASS"],
        connect_timeout=10,
    )

@contextmanager
def dealio():
    conn = _conn_dealio()
    try:
        yield psycopg2.extras.RealDictCursor(conn)
    finally:
        conn.close()

@contextmanager
def backoffice():
    conn = _conn_backoffice()
    try:
        yield psycopg2.extras.RealDictCursor(conn)
    finally:
        conn.close()

@contextmanager
def backoffice_rw():
    """Backoffice connection with commit/rollback for write operations."""
    conn = _conn_backoffice()
    try:
        cur = psycopg2.extras.RealDictCursor(conn)
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
