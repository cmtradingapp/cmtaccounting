"""CRO 'All in One' dashboard — Dealio replica queries.

Reads the same tables Metabase reads (`dealio.daily_profits`, `dealio.trades_mt5`,
`dealio.users`) so our numbers line up card-for-card with the original panel.

Primary source: `dealio.daily_profits` — a pre-aggregated per-login per-day
snapshot with closedpnl, floatingpnl, netdeposit, deltafloatingpnl, equity,
balance, credit. All `converted*` columns are already in the user's calculation
currency (USD on AN100), so we use those.

Key assumptions:
  * `sourceid` defaults to 'AN100' (the live MT5 server).
  * `groupname` mask defaults to 'CMV%' (live trading groups on this broker).
  * Date columns are plain `date` — we just pass `%(date)s`.
"""
from __future__ import annotations

import time
from calendar import monthrange
from datetime import date, timedelta
from typing import Any

import psycopg2

import db

DEFAULT_SOURCE = "AN100"
DEFAULT_GROUP_MASK = "CMV%"


# ── helpers ────────────────────────────────────────────────────────────────
def _fetchone(cur, sql: str, params: dict) -> dict:
    cur.execute(sql, params)
    row = cur.fetchone()
    return dict(row) if row else {}


def _fetchall(cur, sql: str, params: dict) -> list[dict]:
    cur.execute(sql, params)
    return [dict(r) for r in cur.fetchall()]


def _coalesce(d: dict, *keys) -> float:
    return float(d.get(keys[0]) or 0.0) if len(keys) == 1 else sum(
        float(d.get(k) or 0.0) for k in keys
    )


# ── totals (both daily and monthly share shape) ────────────────────────────
_TOTALS_SQL = """
SELECT
    COALESCE(SUM(converteddeltafloatingpnl), 0) AS delta_floating,
    COALESCE(SUM(convertedclosedpnl),        0) AS closed_pnl,
    COALESCE(SUM(convertednetdeposit),       0) AS net_deposits,
    COUNT(DISTINCT CASE WHEN convertednetdeposit > 0 THEN login END) AS n_depositors,
    COUNT(DISTINCT login)                         AS n_accounts
  FROM dealio.daily_profits
 WHERE date BETWEEN %(start)s AND %(end)s
   AND sourceid = %(source)s
   AND groupname LIKE %(group_mask)s
"""

_LATEST_EQUITY_SQL = """
WITH latest AS (
    SELECT login, MAX(date) AS d
      FROM dealio.daily_profits
     WHERE date <= %(end)s
       AND sourceid = %(source)s
       AND groupname LIKE %(group_mask)s
     GROUP BY login
)
SELECT
    COALESCE(SUM(dp.convertedequity),     0) AS equity,
    COALESCE(SUM(dp.convertedfloatingpnl),0) AS floating_pnl,
    COALESCE(SUM(dp.convertedbalance),    0) AS balance,
    COUNT(*)                                  AS n_accounts
  FROM dealio.daily_profits dp
  JOIN latest l ON l.login = dp.login AND l.d = dp.date
 WHERE dp.sourceid = %(source)s
   AND dp.groupname LIKE %(group_mask)s
"""

# credit is on dealio.users only (not daily_profits), so one extra roundtrip.
_CREDIT_SQL = """
SELECT COALESCE(SUM(credit), 0) AS credit
  FROM dealio.users
 WHERE sourceid = %(source)s
   AND groupname LIKE %(group_mask)s
"""


_TRADE_AGG_SQL = """
SELECT
    COUNT(*)                                                       AS n_deals,
    COUNT(DISTINCT CASE WHEN opentime::date  BETWEEN %(start)s AND %(end)s
                         THEN login END)                            AS n_active_traders,
    COUNT(DISTINCT login)                                           AS n_traders,
    COALESCE(SUM(notionalvalue * conversionrate), 0)                AS volume_usd,
    COALESCE(SUM(computedprofit),    0)                             AS closed_pnl_trades,
    COALESCE(SUM(computedswap),      0)                             AS swap,
    COALESCE(SUM(computedcommission),0)                             AS commission
  FROM dealio.trades_mt5
 WHERE (opentime::date  BETWEEN %(start)s AND %(end)s
     OR closetime::date BETWEEN %(start)s AND %(end)s)
   AND sourceid = %(source)s
   AND groupname LIKE %(group_mask)s
   AND symbolplain <> ''
"""


_FTD_SQL = """
-- FTD = a login whose first-ever positive net-deposit falls in the window.
-- Start from the small window-set and prove no earlier deposit exists —
-- avoids a full-table MIN()/GROUP BY which was taking ~30s on the replica.
SELECT COUNT(DISTINCT dp.login) AS n_ftd
  FROM dealio.daily_profits dp
 WHERE dp.date BETWEEN %(start)s AND %(end)s
   AND dp.convertednetdeposit > 0
   AND dp.sourceid = %(source)s
   AND dp.groupname LIKE %(group_mask)s
   AND NOT EXISTS (
       SELECT 1 FROM dealio.daily_profits pre
        WHERE pre.login = dp.login
          AND pre.date < dp.date
          AND pre.convertednetdeposit > 0
          AND pre.sourceid = %(source)s
   )
"""


def _window_snapshot(cur, start: date, end: date, source: str, mask: str, label: str) -> dict:
    params = {"start": start, "end": end, "source": source, "group_mask": mask}
    totals = _fetchone(cur, _TOTALS_SQL, params)
    trades = _fetchone(cur, _TRADE_AGG_SQL, params)
    ftd    = _fetchone(cur, _FTD_SQL, params)
    # Latest equity is a point-in-time view (end of window).
    eq_params = {"end": end, "source": source, "group_mask": mask}
    eq     = _fetchone(cur, _LATEST_EQUITY_SQL, eq_params)
    credit = _fetchone(cur, _CREDIT_SQL,
                       {"source": source, "group_mask": mask})

    delta_floating = _coalesce(totals, "delta_floating")
    closed_pnl     = _coalesce(totals, "closed_pnl")
    pnl_total      = delta_floating + closed_pnl
    net_deposits   = _coalesce(totals, "net_deposits")
    n_depositors   = int(totals.get("n_depositors") or 0)
    n_ftd          = int(ftd.get("n_ftd") or 0)

    return {
        "label": label,
        "start": start.isoformat(),
        "end":   end.isoformat(),
        "source": source,
        "group_mask": mask,
        # money
        "pnl":            pnl_total,
        "delta_floating": delta_floating,
        "closed_pnl":     closed_pnl,
        "net_deposits":   net_deposits,
        "volume_usd":     _coalesce(trades, "volume_usd"),
        "swap":           _coalesce(trades, "swap"),
        "commission":     _coalesce(trades, "commission"),
        # equity (point-in-time at end of window)
        "equity":       _coalesce(eq, "equity"),
        "floating_pnl": _coalesce(eq, "floating_pnl"),
        "balance":      _coalesce(eq, "balance"),
        "credit":       _coalesce(credit, "credit"),
        "wd_equity":    max(_coalesce(eq, "equity") - _coalesce(credit, "credit"), 0.0),
        # counts
        "n_accounts":        int(totals.get("n_accounts") or 0),
        "n_accounts_latest": int(eq.get("n_accounts") or 0),
        "n_active_traders":  int(trades.get("n_active_traders") or 0),
        "n_traders":         int(trades.get("n_traders") or 0),
        "n_depositors":      n_depositors,
        "n_ftd":             n_ftd,
        "n_retention_depositors": max(n_depositors - n_ftd, 0),
        "n_deals":           int(trades.get("n_deals") or 0),
    }


# ── public API ─────────────────────────────────────────────────────────────
def day_snapshot(d: date, source: str = DEFAULT_SOURCE, mask: str = DEFAULT_GROUP_MASK) -> dict:
    with db.dealio() as cur:
        return _window_snapshot(cur, d, d, source, mask, d.isoformat())


def month_snapshot(year: int, month: int, source: str = DEFAULT_SOURCE,
                   mask: str = DEFAULT_GROUP_MASK) -> dict:
    last = monthrange(year, month)[1]
    start = date(year, month, 1)
    end   = date(year, month, last)
    with db.dealio() as cur:
        return _window_snapshot(cur, start, end, source, mask, f"{year:04d}-{month:02d}")


def range_snapshot(start: date, end: date, source: str = DEFAULT_SOURCE,
                   mask: str = DEFAULT_GROUP_MASK) -> dict:
    with db.dealio() as cur:
        return _window_snapshot(cur, start, end, source, mask, f"{start}→{end}")


# ── Volume by symbol ───────────────────────────────────────────────────────
_BY_SYMBOL_SQL = """
SELECT
    symbolplain                                      AS symbol,
    COUNT(*)                                         AS n_deals,
    COUNT(DISTINCT login)                            AS n_traders,
    COALESCE(SUM(notionalvalue * conversionrate), 0) AS notional_usd,
    COALESCE(SUM(CASE WHEN cmd = 0 THEN notionalvalue * conversionrate END), 0) AS notional_buy,
    COALESCE(SUM(CASE WHEN cmd = 1 THEN notionalvalue * conversionrate END), 0) AS notional_sell,
    COALESCE(SUM(computedprofit),    0)              AS pnl,
    COALESCE(SUM(computedswap),      0)              AS swap,
    COALESCE(SUM(computedcommission),0)              AS commission
  FROM dealio.trades_mt5
 WHERE (opentime::date  BETWEEN %(start)s AND %(end)s
     OR closetime::date BETWEEN %(start)s AND %(end)s)
   AND sourceid = %(source)s
   AND groupname LIKE %(group_mask)s
   AND symbolplain <> ''
 GROUP BY symbolplain
 ORDER BY notional_usd DESC
 LIMIT 50
"""


def volume_by_symbol(start: date, end: date, source: str = DEFAULT_SOURCE,
                     mask: str = DEFAULT_GROUP_MASK) -> list[dict]:
    with db.dealio() as cur:
        return _fetchall(cur, _BY_SYMBOL_SQL, {
            "start": start, "end": end, "source": source, "group_mask": mask
        })


# ── Per-group breakdown (the "Daily Performance by CRM fields" table) ──────
_BY_GROUP_SQL = """
SELECT
    groupname,
    COUNT(DISTINCT login)                         AS n_accounts,
    COUNT(DISTINCT CASE WHEN convertednetdeposit > 0 THEN login END) AS n_depositors,
    COALESCE(SUM(converteddeltafloatingpnl), 0)  AS delta_floating,
    COALESCE(SUM(convertedclosedpnl),        0)  AS closed_pnl,
    COALESCE(SUM(convertednetdeposit),       0)  AS net_deposits,
    COALESCE(SUM(convertedequity),           0)  AS equity,
    COALESCE(SUM(convertedbalance),          0)  AS balance
  FROM dealio.daily_profits
 WHERE date BETWEEN %(start)s AND %(end)s
   AND sourceid = %(source)s
   AND groupname LIKE %(group_mask)s
 GROUP BY groupname
 ORDER BY equity DESC NULLS LAST
 LIMIT 50
"""


def perf_by_group(start: date, end: date, source: str = DEFAULT_SOURCE,
                  mask: str = DEFAULT_GROUP_MASK) -> list[dict]:
    with db.dealio() as cur:
        return _fetchall(cur, _BY_GROUP_SQL, {
            "start": start, "end": end, "source": source, "group_mask": mask
        })


# ── Daily series over a window (for trend tables / charts) ─────────────────
_DAILY_SERIES_SQL = """
SELECT
    date,
    COUNT(DISTINCT login)                         AS n_accounts,
    COUNT(DISTINCT CASE WHEN convertednetdeposit > 0 THEN login END) AS n_depositors,
    COALESCE(SUM(converteddeltafloatingpnl), 0)  AS delta_floating,
    COALESCE(SUM(convertedclosedpnl),        0)  AS closed_pnl,
    COALESCE(SUM(convertednetdeposit),       0)  AS net_deposits,
    COALESCE(SUM(convertedfloatingpnl),      0)  AS floating_pnl,
    COALESCE(SUM(convertedequity),           0)  AS equity,
    COALESCE(SUM(convertedbalance),          0)  AS balance
  FROM dealio.daily_profits
 WHERE date BETWEEN %(start)s AND %(end)s
   AND sourceid = %(source)s
   AND groupname LIKE %(group_mask)s
 GROUP BY date
 ORDER BY date DESC
"""


def daily_series(start: date, end: date, source: str = DEFAULT_SOURCE,
                 mask: str = DEFAULT_GROUP_MASK) -> list[dict]:
    with db.dealio() as cur:
        rows = _fetchall(cur, _DAILY_SERIES_SQL, {
            "start": start, "end": end, "source": source, "group_mask": mask
        })
    for r in rows:
        r["date"] = r["date"].isoformat() if r.get("date") else None
        r["pnl"] = _coalesce(r, "delta_floating") + _coalesce(r, "closed_pnl")
    return rows


# ── retry wrapper ──────────────────────────────────────────────────────────
def _retry_on_replica_conflict(fn, *, attempts: int = 3, backoff: float = 1.5):
    """Dealio is a hot-standby. Long-running queries can be cancelled by the WAL
    apply process ("canceling statement due to conflict with recovery"). Retry
    a bounded number of times before giving up.
    """
    last_exc = None
    for i in range(attempts):
        try:
            return fn()
        except psycopg2.errors.QueryCanceled as e:
            last_exc = e
        except psycopg2.OperationalError as e:
            msg = str(e).lower()
            if "conflict with recovery" not in msg and "canceling statement" not in msg:
                raise
            last_exc = e
        time.sleep(backoff ** i)
    raise last_exc


def _latest_data_date(cur, source: str, mask: str, before: date) -> date | None:
    cur.execute(
        "SELECT MAX(date) AS d FROM dealio.daily_profits "
        " WHERE date <= %(before)s AND sourceid = %(source)s "
        "   AND groupname LIKE %(mask)s",
        {"before": before, "source": source, "mask": mask},
    )
    row = cur.fetchone()
    return (dict(row) or {}).get("d")


# ── dashboard bundle ───────────────────────────────────────────────────────
def dashboard_bundle(day: date, source: str = DEFAULT_SOURCE,
                     mask: str = DEFAULT_GROUP_MASK) -> dict:
    """One call that gathers everything the dashboard needs.

    If the requested day has no daily_profits rows yet (e.g. EOD snapshot hasn't
    been written), we silently fall back to the most recent day that does.
    """
    def _go():
        with db.dealio() as cur:
            effective = day
            latest = _latest_data_date(cur, source, mask, day)
            if latest and latest < day:
                effective = latest

            month_start = effective.replace(day=1)
            last = monthrange(effective.year, effective.month)[1]
            month_end   = date(effective.year, effective.month, last)
            trend_start = effective - timedelta(days=29)

            daily   = _window_snapshot(cur, effective, effective, source, mask,
                                       effective.isoformat())
            monthly = _window_snapshot(cur, month_start, month_end, source, mask,
                                       f"{effective.year:04d}-{effective.month:02d}")
            by_grp  = _fetchall(cur, _BY_GROUP_SQL,
                                {"start": effective, "end": effective,
                                 "source": source, "group_mask": mask})
            by_sym  = _fetchall(cur, _BY_SYMBOL_SQL,
                                {"start": effective, "end": effective,
                                 "source": source, "group_mask": mask})
            series  = _fetchall(cur, _DAILY_SERIES_SQL,
                                {"start": trend_start, "end": effective,
                                 "source": source, "group_mask": mask})

        for r in series:
            r["date"] = r["date"].isoformat() if r.get("date") else None
            r["pnl"] = _coalesce(r, "delta_floating") + _coalesce(r, "closed_pnl")

        return {
            "date":      effective.isoformat(),
            "requested": day.isoformat(),
            "fellback":  effective != day,
            "source":    source,
            "group_mask": mask,
            "daily":   daily,
            "monthly": monthly,
            "by_group":  by_grp,
            "by_symbol": by_sym,
            "trend":     series,
        }

    return _retry_on_replica_conflict(_go)
