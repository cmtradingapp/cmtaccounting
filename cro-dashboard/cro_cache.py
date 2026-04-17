"""Tiny in-process cache for CRO dashboard snapshots.

Two layers:
  1. In-memory TTL cache — repeated page loads within TTL return immediately
  2. SQLite persistence — nightly aggregates survive restarts

Keys: (kind, label, group_mask) — kind ∈ {'day','month'}, label is the date /
YYYY-MM string, group_mask is the MT5 filter used to compute the snapshot.
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time as _time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Callable, Optional

from cro_metrics import Snapshot

_DB_PATH = Path(__file__).with_name("cro_cache.db")
_LOCK = threading.Lock()

# In-memory TTL cache (seconds). 5 min by default.
_TTL = int(os.environ.get("CRO_CACHE_TTL", "300"))
_MEM: dict[tuple[str, str, str], tuple[float, Snapshot]] = {}


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_DB_PATH)
    c.row_factory = sqlite3.Row
    c.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            kind       TEXT NOT NULL,
            label      TEXT NOT NULL,
            group_mask TEXT NOT NULL,
            computed_at INTEGER NOT NULL,
            payload    TEXT NOT NULL,
            PRIMARY KEY (kind, label, group_mask)
        )
    """)
    c.commit()
    return c


def _from_row(row: sqlite3.Row) -> Snapshot:
    d = json.loads(row["payload"])
    return Snapshot(**d)


def get(kind: str, label: str, group_mask: str) -> Optional[Snapshot]:
    """Return cached snapshot (memory first, then SQLite) or None."""
    key = (kind, label, group_mask)
    now = _time.time()
    with _LOCK:
        hit = _MEM.get(key)
        if hit and (now - hit[0]) < _TTL:
            return hit[1]
    with _conn() as c:
        row = c.execute(
            "SELECT * FROM snapshots WHERE kind=? AND label=? AND group_mask=?",
            (kind, label, group_mask),
        ).fetchone()
    if not row:
        return None
    snap = _from_row(row)
    # Re-warm the memory layer
    with _LOCK:
        _MEM[key] = (now, snap)
    return snap


def put(kind: str, snap: Snapshot) -> None:
    key = (kind, snap.label, snap.group_mask)
    payload = json.dumps(asdict(snap), default=str)
    with _LOCK:
        _MEM[key] = (_time.time(), snap)
    with _conn() as c:
        c.execute(
            "INSERT OR REPLACE INTO snapshots (kind, label, group_mask, computed_at, payload)"
            " VALUES (?, ?, ?, strftime('%s','now'), ?)",
            (kind, snap.label, snap.group_mask, payload),
        )
        c.commit()


def invalidate(kind: str, label: str, group_mask: str) -> None:
    key = (kind, label, group_mask)
    with _LOCK:
        _MEM.pop(key, None)
    with _conn() as c:
        c.execute(
            "DELETE FROM snapshots WHERE kind=? AND label=? AND group_mask=?",
            (kind, label, group_mask),
        )
        c.commit()


def or_compute(
    kind: str, label: str, group_mask: str,
    compute: Callable[[], Snapshot],
    *, force: bool = False,
) -> Snapshot:
    if not force:
        hit = get(kind, label, group_mask)
        if hit is not None:
            return hit
    snap = compute()
    put(kind, snap)
    return snap


def last_computed_at(kind: str, label: str, group_mask: str) -> Optional[int]:
    with _conn() as c:
        row = c.execute(
            "SELECT computed_at FROM snapshots WHERE kind=? AND label=? AND group_mask=?",
            (kind, label, group_mask),
        ).fetchone()
    return int(row["computed_at"]) if row else None
