"""Read-only SQL queries that produce the CRO dashboard metrics.

Ported from MT5-CRO-Frontend/app/metrics.py — same SQL, but executed via
psycopg2 RealDictCursor instead of SQLAlchemy text() so it shares the
existing connection pattern in db.py (`cro()` context manager).

Conversion conventions (matches the MT5-CRO-Backend convention):
  - Total Balance / Credit / Floating       -> internal_rates
  - WD Equity / WD Equity Z / Net Deposits  -> external_rates
                                              (per Mt5MonitorApiBundle.cs:4498)
  - Settled / Closed P&L                    -> internal_rates
                                              with rate_profit fallback
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone


_ACTIVE_ACCOUNT_FILTER = (
    "NOT (a.balance = 0 AND a.equity = 0) AND a.group_name NOT ILIKE '%%test%%'"
)
_ACTIVE_DAILY_FILTER = (
    "d.group_name NOT ILIKE '%%test%%' AND NOT (d.balance = 0 AND d.profit_equity = 0)"
)


def _convert_case(amount_col: str, currency_col: str, rate_alias: str) -> str:
    """SQL CASE expression converting `amount_col` (native) -> USD via `rate_alias`.

    Uses MID rate (average of bid and ask) for the conversion factor, matching
    the convention MT5 Manager uses to display per-account USD figures. Earlier
    revisions toggled between `positive_to_usd` (=1/ask) and `negative_to_usd`
    (=1/bid) based on the sign of the value -- that's a transactional
    convention (sell at bid, buy at ask) which leaves a systematic gap of
    ~half-spread per converted dollar vs. the broker's display number. MID
    rate closes that gap.
    """
    return f"""
        CASE
            WHEN {amount_col} = 0 THEN 0
            WHEN {currency_col} = 'USD' THEN {amount_col}
            WHEN {rate_alias}.currency IS NOT NULL
                AND {rate_alias}.bid > 0 AND {rate_alias}.ask > 0 THEN
                {amount_col} * (
                    CASE WHEN {rate_alias}.usd_base
                         THEN 2.0::numeric / ({rate_alias}.bid + {rate_alias}.ask)
                         ELSE ({rate_alias}.bid + {rate_alias}.ask) / 2.0::numeric
                    END
                )
            ELSE {amount_col}
        END
    """


def _scalar(cur, sql: str, params: dict | None = None) -> float:
    """Execute and return the first column of the first row as float, default 0."""
    cur.execute(sql, params or {})
    row = cur.fetchone()
    if not row:
        return 0.0
    # RealDictCursor returns dict; grab the only value.
    return float(next(iter(row.values())) or 0.0)


# ───────────────────────────── live snapshot helpers (accounts_snapshot)

def _sum_account_field_usd(cur, field: str) -> float:
    sql = f"""
        SELECT COALESCE(SUM({_convert_case(f"a.{field}", "a.currency", "ir")}), 0)::float AS v
        FROM accounts_snapshot a
        LEFT JOIN internal_rates ir ON ir.currency = a.currency
        WHERE {_ACTIVE_ACCOUNT_FILTER}
    """
    return _scalar(cur, sql)


def total_balance_usd(cur) -> float:
    return _sum_account_field_usd(cur, "balance")


def total_credit_usd(cur) -> float:
    return _sum_account_field_usd(cur, "credit")


def total_floating_usd(cur) -> float:
    """Sum of accounts_snapshot.floating (= IMTAccount.Floating() = Profit + Storage)."""
    return _sum_account_field_usd(cur, "floating")


# ────────────────────────────────────────────────────────── WD Equity

def wd_equity(cur) -> dict:
    """WD Equity / WD Equity Z over live accounts_snapshot, external_rates,
    bonuses cumulative through now."""
    sql = f"""
        WITH bonus_per_login AS (
            SELECT login, COALESCE(SUM(amount), 0)::float AS bonus_native
            FROM deposits_withdrawals
            WHERE action = 2 AND comment ILIKE '%%bonus%%'
            GROUP BY login
        ),
        account_with_bonus AS (
            SELECT a.login, COALESCE(a.currency, 'USD') AS currency,
                   a.equity, a.credit, COALESCE(b.bonus_native, 0) AS bonus_native
            FROM accounts_snapshot a
            LEFT JOIN bonus_per_login b ON b.login = a.login
            WHERE {_ACTIVE_ACCOUNT_FILTER}
        ),
        converted AS (
            SELECT awb.login,
                   {_convert_case("awb.equity",       "awb.currency", "er")} AS equity_usd,
                   {_convert_case("awb.credit",       "awb.currency", "er")} AS credit_usd,
                   {_convert_case("awb.bonus_native", "awb.currency", "er")} AS bonus_usd,
                   (er.currency IS NOT NULL OR awb.currency = 'USD') AS has_rate
            FROM account_with_bonus awb
            LEFT JOIN external_rates er ON er.currency = awb.currency
        ),
        per_login AS (
            SELECT login, equity_usd - credit_usd - bonus_usd AS raw_wd_usd
            FROM converted WHERE has_rate
        )
        SELECT
            COALESCE(SUM(raw_wd_usd), 0)::float                AS wd_equity_usd,
            COALESCE(SUM(GREATEST(raw_wd_usd, 0)), 0)::float   AS wd_equity_z_usd,
            COUNT(*)::int                                      AS contributing_logins,
            COUNT(*) FILTER (WHERE raw_wd_usd > 0)::int        AS positive_logins,
            COUNT(*) FILTER (WHERE raw_wd_usd < 0)::int        AS negative_logins
        FROM per_login;
    """
    cur.execute(sql)
    row = cur.fetchone() or {}
    return {
        "wd_equity_usd": float(row.get("wd_equity_usd") or 0.0),
        "wd_equity_z_usd": float(row.get("wd_equity_z_usd") or 0.0),
        "contributing_logins": int(row.get("contributing_logins") or 0),
        "positive_logins": int(row.get("positive_logins") or 0),
        "negative_logins": int(row.get("negative_logins") or 0),
    }


# ─────────────────────────────────────────────────────────── Exposure

def exposure(cur) -> dict:
    """Net / Positive / Absolute exposure aggregates over exposure_snapshot."""
    sql = """
        SELECT
            COALESCE(SUM(volume_net), 0)::float                  AS net_total_usd,
            COALESCE(SUM(GREATEST(volume_net, 0)), 0)::float     AS positive_usd,
            COALESCE(SUM(ABS(volume_net)), 0)::float             AS absolute_usd,
            COUNT(*)::int                                        AS asset_count,
            COUNT(*) FILTER (WHERE volume_net > 0)::int          AS long_assets,
            COUNT(*) FILTER (WHERE volume_net < 0)::int          AS short_assets
        FROM exposure_snapshot;
    """
    cur.execute(sql)
    row = cur.fetchone() or {}
    return {
        "net_total_usd": float(row.get("net_total_usd") or 0.0),
        "positive_usd": float(row.get("positive_usd") or 0.0),
        "absolute_usd": float(row.get("absolute_usd") or 0.0),
        "asset_count": int(row.get("asset_count") or 0),
        "long_assets": int(row.get("long_assets") or 0),
        "short_assets": int(row.get("short_assets") or 0),
    }



def volume_distribution(cur, today_ts: int, today_end_ts: int, month_ts: int) -> list:
    """Per-symbol Volume Distribution: Buy/Sell/Net lots, ABS/signed Notional,
    Swaps, Total Floating PnL, Daily settled PnL, Monthly settled PnL, Commission.

    Sources:
      positions_snapshot — live open positions (buy & sell lots, floating PnL, swaps)
      closed_positions   — settled PnL and commission per symbol for today + MTD
      internal_rates     — mid-rate FX conversion for non-USD-quoted symbols

    Lot scale: volume_ext / 1e8 (validated on XAUUSD, EURUSD — matches
    MT5 Manager display lots).

    Net Volume is from the BROKER's perspective (negative = broker short),
    mirroring the reference "Volume Distribution" panel.
    """
    # Step 1: aggregate live positions per symbol with FX meta
    cur.execute("""
        WITH pos AS (
            SELECT
                ps.symbol,
                CASE WHEN length(ps.symbol) = 6
                          AND right(ps.symbol, 3) ~ '^[A-Z]{3}$'
                     THEN right(ps.symbol, 3)
                     ELSE 'USD'
                END AS quote_ccy,
                SUM(CASE WHEN ps.action = 0 THEN ps.volume_ext / 100000000.0 ELSE 0 END) AS buy_lots,
                SUM(CASE WHEN ps.action = 1 THEN ps.volume_ext / 100000000.0 ELSE 0 END) AS sell_lots,
                SUM(ps.volume_ext / 100000000.0 * ps.contract_size * ps.price_current) AS gross_native,
                SUM((CASE WHEN ps.action = 1 THEN 1.0 ELSE -1.0 END)
                    * ps.volume_ext / 100000000.0 * ps.contract_size * ps.price_current) AS net_native,
                -- Floating P&L and Swaps: convert using ACCOUNT currency (same as
                -- _sum_account_field_usd on the main cards) so the total matches MetaTrader.
                -- Non-USD accounts (KES/ZAR/NGN) have their floating stored by MT5 at the
                -- broker's internal cross-rate; we must apply the same rate here.
                SUM(
                    ps.profit *
                    CASE WHEN COALESCE(a.currency, 'USD') = 'USD' THEN 1.0
                         WHEN ir_a.bid > 0 AND ir_a.ask > 0 THEN
                           CASE WHEN ir_a.usd_base THEN 2.0/(ir_a.bid + ir_a.ask)
                                ELSE (ir_a.bid + ir_a.ask) / 2.0 END
                         ELSE 1.0 END
                ) AS floating_pnl,
                SUM(
                    ps.storage *
                    CASE WHEN COALESCE(a.currency, 'USD') = 'USD' THEN 1.0
                         WHEN ir_a.bid > 0 AND ir_a.ask > 0 THEN
                           CASE WHEN ir_a.usd_base THEN 2.0/(ir_a.bid + ir_a.ask)
                                ELSE (ir_a.bid + ir_a.ask) / 2.0 END
                         ELSE 1.0 END
                ) AS swaps_usd
            FROM positions_snapshot ps
            LEFT JOIN accounts_snapshot a
                ON  a.login = ps.login
                AND NOT (a.balance = 0 AND a.equity = 0)
                AND a.group_name NOT ILIKE '%%test%%'
            LEFT JOIN internal_rates ir_a ON ir_a.currency = COALESCE(a.currency, 'USD')
            WHERE ps.symbol NOT ILIKE 'Zeroing%%'
              AND ps.symbol NOT ILIKE '%%inactivity%%'
            GROUP BY ps.symbol
        )
        SELECT pos.*, ir.bid, ir.ask, ir.usd_base
        FROM pos
        LEFT JOIN internal_rates ir ON ir.currency = pos.quote_ccy
    """)
    pos_rows = {r["symbol"]: r for r in (cur.fetchall() or [])}

    # Step 2: settled PnL (today + MTD) from closed_positions
    cur.execute("""
        SELECT symbol,
            SUM(CASE WHEN close_time >= %(t)s AND close_time < %(te)s
                     THEN profit + storage + commission + fee ELSE 0 END) AS daily_pnl,
            SUM(CASE WHEN close_time >= %(m)s AND close_time < %(te)s
                     THEN profit + storage + commission + fee ELSE 0 END) AS monthly_pnl,
            SUM(CASE WHEN close_time >= %(t)s AND close_time < %(te)s
                     THEN commission ELSE 0 END) AS commission_today
        FROM closed_positions
        WHERE close_time >= %(m)s AND close_time < %(te)s
          AND symbol NOT ILIKE 'Zeroing%%'
          AND symbol NOT ILIKE '%%inactivity%%'
        GROUP BY symbol
    """, {"t": today_ts, "te": today_end_ts, "m": month_ts})
    closed_map = {r["symbol"]: r for r in (cur.fetchall() or [])}

    # Steps 2a/2b: SOD floating per symbol for daily-delta and MTD-delta.
    # Uses account-currency FX (same as the live floating query above) so
    # delta = current_floating - sod_floating is a clean USD difference.
    # If no snapshot exists for a date the dict is empty → delta = 0 (fallback).
    _SOD_SQL = """
        SELECT ps.symbol,
               SUM(
                   (ps.profit + ps.storage) *
                   CASE WHEN COALESCE(a.currency, 'USD') = 'USD' THEN 1.0
                        WHEN ir_a.bid > 0 AND ir_a.ask > 0 THEN
                          CASE WHEN ir_a.usd_base THEN 2.0/(ir_a.bid + ir_a.ask)
                               ELSE (ir_a.bid + ir_a.ask) / 2.0 END
                        ELSE 1.0 END
               ) AS sod_floating
        FROM positions_sod ps
        LEFT JOIN accounts_snapshot a
            ON  a.login = ps.login
            AND NOT (a.balance = 0 AND a.equity = 0)
            AND a.group_name NOT ILIKE '%%%%test%%%%'
        LEFT JOIN internal_rates ir_a ON ir_a.currency = COALESCE(a.currency, 'USD')
        WHERE ps.snapshot_date = %(snap_date)s
          AND ps.symbol NOT ILIKE 'Zeroing%%%%'
          AND ps.symbol NOT ILIKE '%%%%inactivity%%%%'
        GROUP BY ps.symbol
    """
    today_date       = datetime.fromtimestamp(today_ts, tz=timezone.utc).date()
    month_start_date = datetime.fromtimestamp(month_ts, tz=timezone.utc).date()
    cur.execute(_SOD_SQL, {"snap_date": today_date})
    sod_today_map = {r["symbol"]: r for r in (cur.fetchall() or [])}
    cur.execute(_SOD_SQL, {"snap_date": month_start_date})
    sod_month_map = {r["symbol"]: r for r in (cur.fetchall() or [])}

    # Step 3: merge, apply FX conversion, sort by ABS net_native DESC
    # (net = long minus short, same basis Dealio uses for ABS Notional)
    all_symbols = sorted(
        set(pos_rows) | set(closed_map),
        key=lambda s: abs(float(pos_rows.get(s, {}).get("net_native") or 0)),
        reverse=True,
    )
    result = []
    for sym in all_symbols:
        p = pos_rows.get(sym, {})
        c = closed_map.get(sym, {})
        bid       = float(p.get("bid")       or 0)
        ask       = float(p.get("ask")       or 0)
        usd_base  = bool(p.get("usd_base"))
        gross     = float(p.get("gross_native") or 0)
        net_nat   = float(p.get("net_native")   or 0)
        quote_ccy = p.get("quote_ccy") or "USD"
        if bid > 0 and ask > 0:
            fx = (2.0 / (bid + ask)) if usd_base else ((bid + ask) / 2.0)
        else:
            fx = 1.0       # rate not yet in internal_rates
        buy_lots      = float(p.get("buy_lots")  or 0)
        sell_lots     = float(p.get("sell_lots") or 0)
        floating_pnl = float(p.get("floating_pnl") or 0)
        swaps        = float(p.get("swaps_usd")    or 0)
        # Delta uses profit+storage to match accounts_snapshot.floating (card convention).
        current_float = floating_pnl + swaps

        # Delta floating: current minus SOD.  When no SOD snapshot exists for
        # the date (new deployment, first day) the symbol won't be in the map →
        # delta = 0 → falls back to settled-only (existing behaviour).
        if sym in sod_today_map:
            delta_daily = current_float - float(sod_today_map[sym].get("sod_floating") or 0)
        else:
            delta_daily = 0.0
        if sym in sod_month_map:
            delta_monthly = current_float - float(sod_month_map[sym].get("sod_floating") or 0)
        else:
            delta_monthly = 0.0

        result.append({
            "symbol":             sym,
            "buy_lots":           buy_lots,
            "sell_lots":          sell_lots,
            "net_lots":           sell_lots - buy_lots,   # broker perspective
            "abs_notional_usd":   abs(net_nat) * fx,
            "notional_usd":       net_nat * fx,
            "floating_pnl_usd":   floating_pnl,
            "swaps_usd":          swaps,
            "total_floating_usd": current_float,  # = floating_pnl + swaps
            "daily_pnl_usd":   -(float(c.get("daily_pnl")   or 0) + delta_daily),
            "monthly_pnl_usd": -(float(c.get("monthly_pnl") or 0) + delta_monthly),
            "commission_usd":     float(c.get("commission_today") or 0),
        })
    return result


# ─────────────────────────── EOD-snapshot helpers (daily_reports)

def _sum_daily_field_usd_eod(cur, field: str, eod_from_ts: int, eod_to_ts: int) -> float:
    sql = f"""
        SELECT COALESCE(SUM({_convert_case(f"d.{field}", "d.currency", "ir")}), 0)::float AS v
        FROM daily_reports d
        LEFT JOIN internal_rates ir ON ir.currency = d.currency
        WHERE d.datetime >= %(from_ts)s AND d.datetime < %(to_ts)s
          AND {_ACTIVE_DAILY_FILTER}
    """
    return _scalar(cur, sql, {"from_ts": eod_from_ts, "to_ts": eod_to_ts})


def total_balance_usd_eod(cur, eod_from_ts: int, eod_to_ts: int) -> float:
    return _sum_daily_field_usd_eod(cur, "balance", eod_from_ts, eod_to_ts)


def total_credit_usd_eod(cur, eod_from_ts: int, eod_to_ts: int) -> float:
    return _sum_daily_field_usd_eod(cur, "credit", eod_from_ts, eod_to_ts)


def total_floating_usd_eod(cur, eod_from_ts: int, eod_to_ts: int) -> float:
    """EOD floating = sum of (profit + profit_storage) from daily_reports for that day."""
    sql = f"""
        SELECT COALESCE(SUM({_convert_case('(d.profit + d.profit_storage)', 'd.currency', 'ir')}), 0)::float AS v
        FROM daily_reports d
        LEFT JOIN internal_rates ir ON ir.currency = d.currency
        WHERE d.datetime >= %(from_ts)s AND d.datetime < %(to_ts)s
          AND {_ACTIVE_DAILY_FILTER}
    """
    return _scalar(cur, sql, {"from_ts": eod_from_ts, "to_ts": eod_to_ts})


def wd_equity_eod(cur, eod_from_ts: int, eod_to_ts: int) -> dict:
    """WD Equity / WD Equity Z as of the given EOD. Uses daily_reports.profit_equity
    + daily_reports.credit + bonus deals where time < eod_to_ts."""
    sql = f"""
        WITH bonus_per_login AS (
            SELECT login, COALESCE(SUM(amount), 0)::float AS bonus_native
            FROM deposits_withdrawals
            WHERE action = 2 AND comment ILIKE '%%bonus%%' AND time < %(eod_to_ts)s
            GROUP BY login
        ),
        daily_with_bonus AS (
            SELECT d.login, COALESCE(NULLIF(d.currency, ''), 'USD') AS currency,
                   d.profit_equity AS equity, d.credit AS credit,
                   COALESCE(b.bonus_native, 0) AS bonus_native
            FROM daily_reports d
            LEFT JOIN bonus_per_login b ON b.login = d.login
            WHERE d.datetime >= %(eod_from_ts)s AND d.datetime < %(eod_to_ts)s
              AND {_ACTIVE_DAILY_FILTER}
        ),
        converted AS (
            SELECT dwb.login,
                   {_convert_case("dwb.equity",       "dwb.currency", "er")} AS equity_usd,
                   {_convert_case("dwb.credit",       "dwb.currency", "er")} AS credit_usd,
                   {_convert_case("dwb.bonus_native", "dwb.currency", "er")} AS bonus_usd,
                   (er.currency IS NOT NULL OR dwb.currency = 'USD') AS has_rate
            FROM daily_with_bonus dwb
            LEFT JOIN external_rates er ON er.currency = dwb.currency
        ),
        per_login AS (
            SELECT login, equity_usd - credit_usd - bonus_usd AS raw_wd_usd
            FROM converted WHERE has_rate
        )
        SELECT
            COALESCE(SUM(raw_wd_usd), 0)::float                AS wd_equity_usd,
            COALESCE(SUM(GREATEST(raw_wd_usd, 0)), 0)::float   AS wd_equity_z_usd,
            COUNT(*)::int                                      AS contributing_logins
        FROM per_login;
    """
    cur.execute(sql, {"eod_from_ts": eod_from_ts, "eod_to_ts": eod_to_ts})
    row = cur.fetchone() or {}
    return {
        "wd_equity_usd": float(row.get("wd_equity_usd") or 0.0),
        "wd_equity_z_usd": float(row.get("wd_equity_z_usd") or 0.0),
        "contributing_logins": int(row.get("contributing_logins") or 0),
    }


# ──────────────────────────────────────────── Closed / Net deps

def closed_pnl_usd(cur, from_ts: int, to_ts: int) -> float:
    """Sum of (profit + storage + commission + fee) over closing deals in [from, to).

    Mid-rate conversion to match MT5 Manager (see _convert_case for rationale).
    Falls back to deal.rate_profit if internal_rates is missing the currency.
    """
    sql = """
        WITH deals AS (
            SELECT cp.profit + cp.storage + cp.commission + cp.fee AS native_pnl,
                   COALESCE(NULLIF(cp.currency, ''), 'USD') AS currency,
                   cp.rate_profit
            FROM closed_positions cp
            WHERE cp.close_time >= %(from_ts)s AND cp.close_time < %(to_ts)s
        )
        SELECT COALESCE(SUM(
            CASE WHEN d.native_pnl = 0 THEN 0
                 WHEN d.currency = 'USD' THEN d.native_pnl
                 WHEN ir.currency IS NOT NULL
                      AND ir.bid > 0 AND ir.ask > 0 THEN
                     d.native_pnl * (
                         CASE WHEN ir.usd_base
                              THEN 2.0::numeric / (ir.bid + ir.ask)
                              ELSE (ir.bid + ir.ask) / 2.0::numeric
                         END
                     )
                 WHEN d.rate_profit > 1.5 THEN d.native_pnl / d.rate_profit
                 ELSE d.native_pnl
            END
        ), 0)::float AS v
        FROM deals d
        LEFT JOIN internal_rates ir ON ir.currency = d.currency
    """
    return _scalar(cur, sql, {"from_ts": from_ts, "to_ts": to_ts})


def net_deposits_usd(cur, from_ts: int, to_ts: int) -> float:
    """Sum of net deposits in [from, to) via external_rates. Excludes bonus / fees /
    spread-charge comments per Mt5MonitorApiBundle.cs:8856-8862."""
    sql = f"""
        SELECT COALESCE(SUM({_convert_case('dw.amount', 'dw.currency', 'er')}), 0)::float AS v
        FROM deposits_withdrawals dw
        LEFT JOIN external_rates er ON er.currency = dw.currency
        WHERE dw.action = 2
          AND dw.time >= %(from_ts)s AND dw.time < %(to_ts)s
          AND COALESCE(lower(dw.comment), '') NOT LIKE '%%bonus%%'
          AND COALESCE(lower(dw.comment), '') NOT LIKE '%%fees placeholder%%'
          AND COALESCE(lower(dw.comment), '') NOT LIKE '%%spread charge%%'
    """
    return _scalar(cur, sql, {"from_ts": from_ts, "to_ts": to_ts})


def cumulative_bonus_usd(cur, through_ts: int) -> float:
    """Sum of all bonus deals up to `through_ts`, USD-converted via external_rates.

    Mirrors the bonus subtraction inside WDZ, aggregated to a single number.
    """
    sql = f"""
        SELECT COALESCE(SUM({_convert_case('dw.amount', 'dw.currency', 'er')}), 0)::float AS v
        FROM deposits_withdrawals dw
        LEFT JOIN external_rates er ON er.currency = dw.currency
        WHERE dw.action = 2
          AND dw.comment ILIKE '%%bonus%%'
          AND dw.time < %(through_ts)s
    """
    return _scalar(cur, sql, {"through_ts": through_ts})


def yesterday_floating_usd(cur, yesterday_start: int, today_start: int) -> float:
    """Sum of daily_reports.(profit + profit_storage) for yesterday."""
    sql = f"""
        SELECT COALESCE(SUM({_convert_case('(d.profit + d.profit_storage)', 'd.currency', 'ir')}), 0)::float AS v
        FROM daily_reports d
        LEFT JOIN internal_rates ir ON ir.currency = d.currency
        WHERE d.datetime >= %(from_ts)s AND d.datetime < %(to_ts)s
          AND {_ACTIVE_DAILY_FILTER}
    """
    return _scalar(cur, sql, {"from_ts": yesterday_start, "to_ts": today_start})


# ──────────────────────────────────── Activity & Counts metrics
#
# Mirrors Mt5MonitorApiBundle.cs lines 7896-7897 / 8876-8892. Trade-symbol
# exclusions:
#   - Hard prefix:   "Zeroing*"   (case-insensitive)
#   - Soft contains: "*inactivity*"
# Spread-symbol-specific extra exclusions ("SPREAD"/"CORRECTION"/"CASHBACK")
# only apply to the Spread metric, which is deferred — see plan.

# Embedded into trade-symbol filters via SQL `NOT (... OR ...)`.
_TRADER_SYMBOL_FILTER = (
    "symbol NOT ILIKE 'Zeroing%%' AND symbol NOT ILIKE '%%inactivity%%'"
)


def n_traders(cur, from_ts: int, to_ts: int) -> int:
    """Distinct logins with any closing-leg deal in [from, to) — excludes
    Zeroing* and *inactivity* synthetic symbols (matches C# bundle)."""
    sql = f"""
        SELECT COUNT(DISTINCT login)::int AS v
        FROM closed_positions
        WHERE close_time >= %(from_ts)s AND close_time < %(to_ts)s
          AND {_TRADER_SYMBOL_FILTER}
    """
    cur.execute(sql, {"from_ts": from_ts, "to_ts": to_ts})
    row = cur.fetchone() or {}
    return int(row.get("v") or 0)


def n_active_traders_live(cur) -> int:
    """Distinct logins currently holding open positions (live snapshot)."""
    sql = f"""
        SELECT COUNT(DISTINCT login)::int AS v
        FROM positions_snapshot
        WHERE {_TRADER_SYMBOL_FILTER}
    """
    cur.execute(sql)
    row = cur.fetchone() or {}
    return int(row.get("v") or 0)


def n_active_traders_period(cur, from_ts: int, to_ts: int) -> int:
    """Historical (yesterday / monthly): distinct logins who opened OR
    closed positions within the window. Approximates the C# definition
    (logins with opening-leg deals) — see plan note 2 on tradeoff."""
    sql = f"""
        SELECT COUNT(DISTINCT login)::int AS v
        FROM closed_positions
        WHERE {_TRADER_SYMBOL_FILTER}
          AND ((open_time  >= %(from_ts)s AND open_time  < %(to_ts)s)
            OR (close_time >= %(from_ts)s AND close_time < %(to_ts)s))
    """
    cur.execute(sql, {"from_ts": from_ts, "to_ts": to_ts})
    row = cur.fetchone() or {}
    return int(row.get("v") or 0)


def n_depositors(cur, from_ts: int, to_ts: int) -> int:
    """Distinct logins with positive deposits in [from, to). Excludes
    bonus / fees-placeholder / spread-charge comments per Mt5MonitorApiBundle.cs."""
    sql = """
        SELECT COUNT(DISTINCT login)::int AS v
        FROM deposits_withdrawals
        WHERE action = 2 AND amount > 0
          AND time >= %(from_ts)s AND time < %(to_ts)s
          AND COALESCE(lower(comment), '') NOT LIKE '%%bonus%%'
          AND COALESCE(lower(comment), '') NOT LIKE '%%fees placeholder%%'
          AND COALESCE(lower(comment), '') NOT LIKE '%%spread charge%%'
    """
    cur.execute(sql, {"from_ts": from_ts, "to_ts": to_ts})
    row = cur.fetchone() or {}
    return int(row.get("v") or 0)


def n_new_registrations(cur, from_ts: int, to_ts: int) -> int:
    """Accounts whose IMTUser.Registration timestamp falls in [from, to).

    Note: Mt5MonitorApiBundle.cs uses broker-local-Cyprus date for the
    period match. We use UTC to stay consistent with the rest of the
    dashboard's windowing (small drift at day boundaries; see plan).
    """
    sql = """
        SELECT COUNT(*)::int AS v
        FROM accounts_snapshot
        WHERE registration >= %(from_ts)s AND registration < %(to_ts)s
          AND group_name NOT ILIKE '%%test%%'
    """
    cur.execute(sql, {"from_ts": from_ts, "to_ts": to_ts})
    row = cur.fetchone() or {}
    return int(row.get("v") or 0)


def n_ftd(cur, from_ts: int, to_ts: int) -> int:
    """Count of logins whose first-EVER positive deposit falls in [from, to).

    Matches Mt5MonitorApiBundle.cs CollectFirstValidDepositDates: bonus
    deposits ARE candidates for first-deposit (NOT excluded here), only
    fees-placeholder + spread-charge comments are excluded.
    """
    sql = """
        WITH first_dep AS (
            SELECT login, MIN(time) AS first_time
            FROM deposits_withdrawals
            WHERE action = 2 AND amount > 0
              AND COALESCE(lower(comment), '') NOT LIKE '%%fees placeholder%%'
              AND COALESCE(lower(comment), '') NOT LIKE '%%spread charge%%'
            GROUP BY login
        )
        SELECT COUNT(*)::int AS v
        FROM first_dep
        WHERE first_time >= %(from_ts)s AND first_time < %(to_ts)s
    """
    cur.execute(sql, {"from_ts": from_ts, "to_ts": to_ts})
    row = cur.fetchone() or {}
    return int(row.get("v") or 0)


def volume_usd(cur, from_ts: int, to_ts: int) -> float:
    """Σ deals.notional_usd over [from, to). Both opening and closing legs
    counted (matches C# Mt5MonitorApiBundle.cs convention).

    `notional_usd` is computed once at ingest by MT5-CRO-Backend, using
    broker-time-exact MarketBid/MarketAsk for symbols where the symbol IS
    the USD-cross pair, and `rate_profit` as fallback. So this metric is
    deal-time-exact USD with no FX-time-skew.
    """
    sql = """
        SELECT COALESCE(SUM(notional_usd), 0)::float AS v
        FROM deals
        WHERE time >= %(from_ts)s AND time < %(to_ts)s
          AND action IN (0, 1)
          AND symbol NOT ILIKE 'Zeroing%%'
          AND symbol NOT ILIKE '%%inactivity%%'
    """
    return _scalar(cur, sql, {"from_ts": from_ts, "to_ts": to_ts})


def spread_usd(cur, from_ts: int, to_ts: int) -> float:
    """Σ deals.spread_cost_usd over [from, to). Computed at ingest as
    `volume_lots × contract_size × (MarketAsk − MarketBid)`, USD-converted
    with the same priority logic as notional. Bid/ask captured directly
    off the IMTDeal at trade time — exact, not period-averaged."""
    sql = """
        SELECT COALESCE(SUM(spread_cost_usd), 0)::float AS v
        FROM deals
        WHERE time >= %(from_ts)s AND time < %(to_ts)s
          AND action IN (0, 1)
          AND symbol NOT ILIKE 'Zeroing%%'
          AND symbol NOT ILIKE '%%inactivity%%'
    """
    return _scalar(cur, sql, {"from_ts": from_ts, "to_ts": to_ts})


def ftd_amount_usd(cur, from_ts: int, to_ts: int) -> float:
    """Sum of all positive deposits in [from, to) for FTD logins,
    USD-converted via external_rates (mid-rate per _convert_case).

    Mt5MonitorApiBundle.cs: bonus IS excluded for the *amount* sum (unlike
    the FTD-login set itself), to match dashboard semantics that "FTD
    Amount" represents real cash deposit value the trader brought in.
    """
    sql = f"""
        WITH first_dep AS (
            SELECT login, MIN(time) AS first_time
            FROM deposits_withdrawals
            WHERE action = 2 AND amount > 0
              AND COALESCE(lower(comment), '') NOT LIKE '%%fees placeholder%%'
              AND COALESCE(lower(comment), '') NOT LIKE '%%spread charge%%'
            GROUP BY login
        ),
        ftd AS (
            SELECT login FROM first_dep
            WHERE first_time >= %(from_ts)s AND first_time < %(to_ts)s
        )
        SELECT COALESCE(SUM({_convert_case('dw.amount', 'dw.currency', 'er')}), 0)::float AS v
        FROM deposits_withdrawals dw
        JOIN ftd USING (login)
        LEFT JOIN external_rates er ON er.currency = dw.currency
        WHERE dw.action = 2 AND dw.amount > 0
          AND dw.time >= %(from_ts)s AND dw.time < %(to_ts)s
          AND COALESCE(lower(dw.comment), '') NOT LIKE '%%bonus%%'
          AND COALESCE(lower(dw.comment), '') NOT LIKE '%%fees placeholder%%'
          AND COALESCE(lower(dw.comment), '') NOT LIKE '%%spread charge%%'
    """
    return _scalar(cur, sql, {"from_ts": from_ts, "to_ts": to_ts})


# ────────────────────────────────────────────── Public top-level

def collect_all_metrics(cur) -> dict:
    """Compute all three status sections in one call. Returns a dict ready
    for JSON serialisation by the Flask handler."""
    now_utc            = datetime.now(timezone.utc)
    today_start        = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_start    = today_start - timedelta(days=1)
    day_before_start   = today_start - timedelta(days=2)
    month_start        = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    prev_month_end     = month_start - timedelta(days=1)

    today_start_ts     = int(today_start.timestamp())
    yesterday_start_ts = int(yesterday_start.timestamp())
    day_before_ts      = int(day_before_start.timestamp())
    month_start_ts     = int(month_start.timestamp())
    prev_month_end_ts  = int(prev_month_end.timestamp())
    # `closed_positions.close_time` and `deposits_withdrawals.time` are stored
    # in broker-local time treated as UTC seconds (Cyprus broker, UTC+3). Use
    # end-of-day instead of "now" so the upper bound covers the broker's full
    # day -- broker can't return future deals.
    today_end_ts       = today_start_ts + 86400

    # ── live aggregates
    balance_today      = total_balance_usd(cur)
    credit_today       = total_credit_usd(cur)
    floating_today     = total_floating_usd(cur)
    wd_t               = wd_equity(cur)
    exp_t              = exposure(cur)
    vol_dist           = volume_distribution(cur, today_start_ts, today_end_ts, month_start_ts)

    # ── yesterday EOD
    balance_yest_eod   = total_balance_usd_eod(cur, yesterday_start_ts, today_start_ts)
    credit_yest_eod    = total_credit_usd_eod(cur, yesterday_start_ts, today_start_ts)
    floating_yest_eod  = total_floating_usd_eod(cur, yesterday_start_ts, today_start_ts)
    wd_y               = wd_equity_eod(cur, yesterday_start_ts, today_start_ts)

    # ── day-before-yesterday EOD (for yesterday's delta)
    floating_dayb_eod  = total_floating_usd_eod(cur, day_before_ts, yesterday_start_ts)
    wd_db              = wd_equity_eod(cur, day_before_ts, yesterday_start_ts)

    # ── prev-month-end EOD (for MTD)
    floating_month_st  = total_floating_usd_eod(cur, prev_month_end_ts, month_start_ts)
    wd_pme             = wd_equity_eod(cur, prev_month_end_ts, month_start_ts)
    balance_month_st   = total_balance_usd_eod(cur, prev_month_end_ts, month_start_ts)
    credit_month_st    = total_credit_usd_eod(cur, prev_month_end_ts, month_start_ts)

    # ── Cumulative Bonus (COB) per reference time
    cob_today          = cumulative_bonus_usd(cur, today_end_ts)
    cob_yesterday      = cumulative_bonus_usd(cur, today_start_ts)
    cob_month_start    = cumulative_bonus_usd(cur, month_start_ts)

    # ── Settled (closed P&L) per period
    settled_today      = closed_pnl_usd(cur, today_start_ts, today_end_ts)
    settled_yest       = closed_pnl_usd(cur, yesterday_start_ts, today_start_ts)
    settled_mtd        = closed_pnl_usd(cur, month_start_ts, today_end_ts)

    # ── Net Deposits per period
    netdep_today       = net_deposits_usd(cur, today_start_ts, today_end_ts)
    netdep_yest        = net_deposits_usd(cur, yesterday_start_ts, today_start_ts)
    netdep_mtd         = net_deposits_usd(cur, month_start_ts, today_end_ts)

    # ── Delta Floating per period
    delta_today        = floating_today - yesterday_floating_usd(cur, yesterday_start_ts, today_start_ts)
    delta_yest         = floating_yest_eod - floating_dayb_eod
    delta_mtd          = floating_today - floating_month_st

    # ── Daily / Monthly P&L (= Delta Floating + Settled)
    daily_pnl_today    = delta_today + settled_today
    daily_pnl_yest     = delta_yest + settled_yest
    monthly_pnl        = delta_mtd + settled_mtd

    # ── Delta WDZ + Daily P&L Cash (= Delta WDZ - Net Deposits)
    delta_wdz_today    = wd_t["wd_equity_z_usd"] - wd_y["wd_equity_z_usd"]
    delta_wdz_yest     = wd_y["wd_equity_z_usd"] - wd_db["wd_equity_z_usd"]
    delta_wdz_mtd      = wd_t["wd_equity_z_usd"] - wd_pme["wd_equity_z_usd"]
    pnl_cash_today     = delta_wdz_today - netdep_today
    pnl_cash_yest      = delta_wdz_yest  - netdep_yest
    pnl_cash_mtd       = delta_wdz_mtd   - netdep_mtd

    # ── Activity & Counts (mirrors Mt5MonitorApiBundle.cs)
    n_traders_today      = n_traders(cur, today_start_ts, today_end_ts)
    n_traders_yest       = n_traders(cur, yesterday_start_ts, today_start_ts)
    n_traders_mtd        = n_traders(cur, month_start_ts, today_end_ts)

    n_active_today       = n_active_traders_live(cur)
    n_active_yest        = n_active_traders_period(cur, yesterday_start_ts, today_start_ts)
    n_active_mtd         = n_active_traders_period(cur, month_start_ts, today_end_ts)

    n_dep_today          = n_depositors(cur, today_start_ts, today_end_ts)
    n_dep_yest           = n_depositors(cur, yesterday_start_ts, today_start_ts)
    n_dep_mtd            = n_depositors(cur, month_start_ts, today_end_ts)

    n_regs_today         = n_new_registrations(cur, today_start_ts, today_end_ts)
    n_regs_yest          = n_new_registrations(cur, yesterday_start_ts, today_start_ts)
    n_regs_mtd           = n_new_registrations(cur, month_start_ts, today_end_ts)

    n_ftd_today          = n_ftd(cur, today_start_ts, today_end_ts)
    n_ftd_yest           = n_ftd(cur, yesterday_start_ts, today_start_ts)
    n_ftd_mtd            = n_ftd(cur, month_start_ts, today_end_ts)

    ftd_amt_today        = ftd_amount_usd(cur, today_start_ts, today_end_ts)
    ftd_amt_yest         = ftd_amount_usd(cur, yesterday_start_ts, today_start_ts)
    ftd_amt_mtd          = ftd_amount_usd(cur, month_start_ts, today_end_ts)

    # Volume + Spread (read from the new `deals` table populated by
    # MT5-CRO-Backend's store_deals stage).
    volume_today         = volume_usd(cur, today_start_ts, today_end_ts)
    volume_yest          = volume_usd(cur, yesterday_start_ts, today_start_ts)
    volume_mtd           = volume_usd(cur, month_start_ts, today_end_ts)

    spread_today         = spread_usd(cur, today_start_ts, today_end_ts)
    spread_yest          = spread_usd(cur, yesterday_start_ts, today_start_ts)
    spread_mtd           = spread_usd(cur, month_start_ts, today_end_ts)

    return {
        "as_of_utc": now_utc.isoformat(),
        "today_label": today_start.strftime("%Y-%m-%d"),
        "yesterday_label": yesterday_start.strftime("%Y-%m-%d"),
        "month_start_label": month_start.strftime("%Y-%m-%d"),
        "prev_month_end_label": prev_month_end.strftime("%Y-%m-%d"),

        # Per-symbol volume breakdown (live positions + today/MTD settled PnL).
        # Top-level (not under today/yesterday/monthly) because it blends
        # live data with today-and-MTD windows simultaneously.
        "volume_distribution": vol_dist,

        "today": {
            "total_balance_usd": balance_today,
            "total_credit_usd": credit_today,
            "cumulative_bonus_usd": cob_today,
            "floating_usd": floating_today,
            "wd_equity_usd": wd_t["wd_equity_usd"],
            "wd_equity_z_usd": wd_t["wd_equity_z_usd"],
            "wd_logins": wd_t["contributing_logins"],
            "wd_positive_logins": wd_t["positive_logins"],
            "wd_negative_logins": wd_t["negative_logins"],
            "exposure_net_usd": exp_t["net_total_usd"],
            "exposure_positive_usd": exp_t["positive_usd"],
            "exposure_absolute_usd": exp_t["absolute_usd"],
            "exposure_assets": exp_t["asset_count"],
            "exposure_long_assets": exp_t["long_assets"],
            "exposure_short_assets": exp_t["short_assets"],
            "settled_pnl_usd": -settled_today,
            "delta_floating_usd": -delta_today,
            "daily_pnl_usd": -daily_pnl_today,
            "net_deposits_usd": netdep_today,
            "delta_wdz_usd": -delta_wdz_today,
            "daily_pnl_cash_usd": -pnl_cash_today,
            # Activity & Counts
            "n_traders": n_traders_today,
            "n_active_traders": n_active_today,
            "n_depositors": n_dep_today,
            "n_new_regs": n_regs_today,
            "n_ftd": n_ftd_today,
            "ftd_amount_usd": ftd_amt_today,
            "volume_usd": volume_today,
            "spread_usd": spread_today,
        },
        "yesterday": {
            "total_balance_usd": balance_yest_eod,
            "total_credit_usd": credit_yest_eod,
            "cumulative_bonus_usd": cob_yesterday,
            "floating_usd": floating_yest_eod,
            "wd_equity_usd": wd_y["wd_equity_usd"],
            "wd_equity_z_usd": wd_y["wd_equity_z_usd"],
            "wd_logins": wd_y["contributing_logins"],
            "settled_pnl_usd": -settled_yest,
            "delta_floating_usd": -delta_yest,
            "daily_pnl_usd": -daily_pnl_yest,
            "net_deposits_usd": netdep_yest,
            "delta_wdz_usd": -delta_wdz_yest,
            "daily_pnl_cash_usd": -pnl_cash_yest,
            # Activity & Counts
            "n_traders": n_traders_yest,
            "n_active_traders": n_active_yest,
            "n_depositors": n_dep_yest,
            "n_new_regs": n_regs_yest,
            "n_ftd": n_ftd_yest,
            "ftd_amount_usd": ftd_amt_yest,
            "volume_usd": volume_yest,
            "spread_usd": spread_yest,
        },
        "monthly": {
            "wd_equity_z_month_start_usd": wd_pme["wd_equity_z_usd"],
            "monthly_pnl_usd": -monthly_pnl,
            "monthly_pnl_cash_usd": -pnl_cash_mtd,
            "total_balance_month_start_usd": balance_month_st,
            "total_credit_month_start_usd": credit_month_st,
            "cumulative_bonus_usd": cob_month_start,
            "wd_equity_month_start_usd": wd_pme["wd_equity_usd"],
            "floating_month_start_usd": floating_month_st,
            "floating_today_usd": floating_today,
            "delta_floating_usd": -delta_mtd,
            "settled_pnl_usd": -settled_mtd,
            "net_deposits_usd": netdep_mtd,
            "delta_wdz_usd": -delta_wdz_mtd,
            "wd_equity_z_today_usd": wd_t["wd_equity_z_usd"],
            # Activity & Counts
            "n_traders": n_traders_mtd,
            "n_active_traders": n_active_mtd,
            "n_depositors": n_dep_mtd,
            "n_new_regs": n_regs_mtd,
            "n_ftd": n_ftd_mtd,
            "ftd_amount_usd": ftd_amt_mtd,
            "volume_usd": volume_mtd,
            "spread_usd": spread_mtd,
        },
    }
