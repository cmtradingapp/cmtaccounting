"""MT5 datawarehouse query layer — platform deposits/withdrawals from mt5_deals.

Replaces the Dealio replica (`dealio.daily_profits`) as the source of per-login
net deposits used by reconcile(). Reads through the read-only `recon_reader`
connection (`db.dw()`). Amounts are converted to USD with EXTERNAL FX rates
(`fx_rates`) — the spread-free basis CRM/Praxis use for cash movements — NOT the
broker's internal (spread-loaded) MT5 prices.

MT5 deal model (verified on the datawarehouse):
    "Action" = 2  -> BALANCE op: deposit if "Profit" > 0, withdrawal if "Profit" < 0
    "Action" = 3  -> credit/bonus, = 4 charge, 0/1 buy/sell   (excluded here)
    "Profit"      -> in the account's GROUP currency
                     (mt5_users."Group" -> mt5_groups."Currency")
    "Comment"     -> "D:<cid>,IP:..." (deposit) / "W:<cid>,IP:..." (withdrawal),
                     where <cid> = Praxis session_cid / CRM account id.

Notes:
- mt5_deals_{year} are standalone yearly tables (not partitions); a monthly query
  targets a single year table. Columns are case-sensitive (quoted).
- TEST accounts (group name contains "test", e.g. CMVtest3US) are EXCLUDED — they
  are not real client cash and Dealio omits them too.
- psycopg2 parameterised queries: literal % in LIKE/ILIKE must be doubled (%%).
"""

import datetime
import re

import db
import fx_rates


def _utc_month_epoch(year: int, month: int):
    """(epoch_start, epoch_end) for the month at UTC midnight boundaries.

    mt5_daily."Datetime" is the EOD snapshot stamped at 23:59:59 UTC of the day it
    summarises, so a day-D snapshot has epoch in [epoch(D), epoch(D+1)). Filtering
    "Datetime" in [epoch(month_start), epoch(month_end)) (UTC) therefore selects
    exactly that month's EOD snapshots, and `(to_timestamp("Datetime") AT TIME ZONE
    'UTC')::date` is the day each row summarises (matches Dealio's date — no offset).
    """
    start = datetime.datetime(year, month, 1, tzinfo=datetime.timezone.utc)
    end = (datetime.datetime(year + 1, 1, 1, tzinfo=datetime.timezone.utc) if month == 12
           else datetime.datetime(year, month + 1, 1, tzinfo=datetime.timezone.utc))
    return int(start.timestamp()), int(end.timestamp())


def _daily_table(year: int) -> str:
    y = int(year)
    if y < 2012 or y > 2100:
        raise ValueError(f"unexpected daily year: {year}")
    return f"mt5_daily_{y}"

# Balance-op deals that are NOT real client cash movements — excluded from the
# net-deposit figure, mirroring cro_metrics / Mt5MonitorApiBundle conventions.
# (`d.` = the mt5_deals alias; %% because these run in parameterised queries.)
_NONCASH_COMMENT_SQL = (
    " AND COALESCE(lower(d.\"Comment\"), '') NOT LIKE '%%bonus%%'"
    " AND COALESCE(lower(d.\"Comment\"), '') NOT LIKE '%%fees placeholder%%'"
    " AND COALESCE(lower(d.\"Comment\"), '') NOT LIKE '%%spread charge%%'"
)

# Exclude test accounts (group name contains "test", any case).
_EXCLUDE_TEST_GROUP_SQL = " AND u.\"Group\" NOT ILIKE '%%test%%'"

_BALANCE_COMMENT_RE = re.compile(r"^\s*([DW]):(\d+)")


def _month_bounds(year: int, month: int):
    start = datetime.date(year, month, 1)
    end = datetime.date(year + 1, 1, 1) if month == 12 else datetime.date(year, month + 1, 1)
    return start, end


def _deal_table(year: int) -> str:
    """Yearly deal table name. (Validate the year so it can't be injected into SQL.)"""
    y = int(year)
    if y < 2012 or y > 2100:
        raise ValueError(f"unexpected deal year: {year}")
    return f"mt5_deals_{y}"


def parse_balance_comment(comment: str):
    """('deposit'|'withdrawal', cid) from a balance-op Comment, else None.
    e.g. 'D:26643552,IP:1.2.3.4' -> ('deposit', '26643552')."""
    if not comment:
        return None
    m = _BALANCE_COMMENT_RE.match(comment)
    if not m:
        return None
    return ("deposit" if m.group(1) == "D" else "withdrawal"), m.group(2)


def mt5_summary(year: int, month: int) -> dict:
    """Per-login net deposit for the month, computed from mt5_deals (Action=2).

    Drop-in replacement for queries.mt5_summary — returns the identical shape:
        { login(int): {"login", "net_usd", "groupcurrency", "avg_fx"} }
    so reconcile() is unchanged. Test-group accounts are excluded. net_usd is
    converted with the month-average EXTERNAL FX rate (USD groups = identity);
    the legacy `avg_fx` key carries that rate. Rows that net to 0 USD are dropped
    (mirrors dealio's netdeposit != 0).
    """
    start, end = _month_bounds(year, month)
    table = _deal_table(year)

    with db.dw() as cur:
        cur.execute(
            f'SELECT d."Login" AS login, g."Currency" AS currency, SUM(d."Profit") AS net_native '
            f"FROM {table} d "
            f'JOIN mt5_users u ON u."Login" = d."Login" '
            f'LEFT JOIN mt5_groups g ON g."Group" = u."Group" '
            f'WHERE d."Action" = 2 AND d."Time" >= %s AND d."Time" < %s'
            f"{_NONCASH_COMMENT_SQL}{_EXCLUDE_TEST_GROUP_SQL} "
            f'GROUP BY d."Login", g."Currency"',
            (start, end),
        )
        rows = cur.fetchall()

    result = {}
    for r in rows:
        login = int(r["login"])
        ccy = r["currency"] or "USD"
        net_native = float(r["net_native"] or 0)
        rate = 1.0 if ccy == "USD" else fx_rates.monthly_rate(ccy, year, month)
        net_usd = round(net_native * rate, 2)
        if net_usd == 0:
            continue
        result[login] = {
            "login": login,
            "net_usd": net_usd,
            "groupcurrency": ccy,
            "avg_fx": rate,
        }
    return result


def deposits_withdrawals(year: int, month: int) -> dict:
    """Per-login gross deposits and withdrawals (group currency + USD) for the month.
    Returns { login(int): {"dep_native","wd_native","dep_usd","wd_usd","currency","tx_count"} }.
    Test-group accounts excluded. Useful for detail views / parity diagnostics;
    reconcile() uses mt5_summary above."""
    start, end = _month_bounds(year, month)
    table = _deal_table(year)
    with db.dw() as cur:
        cur.execute(
            f'SELECT d."Login" AS login, g."Currency" AS currency, '
            f'  SUM(CASE WHEN d."Profit" > 0 THEN d."Profit" ELSE 0 END) AS dep_native, '
            f'  SUM(CASE WHEN d."Profit" < 0 THEN d."Profit" ELSE 0 END) AS wd_native, '
            f'  COUNT(*) AS tx_count '
            f"FROM {table} d "
            f'JOIN mt5_users u ON u."Login" = d."Login" '
            f'LEFT JOIN mt5_groups g ON g."Group" = u."Group" '
            f'WHERE d."Action" = 2 AND d."Time" >= %s AND d."Time" < %s'
            f"{_NONCASH_COMMENT_SQL}{_EXCLUDE_TEST_GROUP_SQL} "
            f'GROUP BY d."Login", g."Currency"',
            (start, end),
        )
        rows = cur.fetchall()

    out = {}
    for r in rows:
        login = int(r["login"])
        ccy = r["currency"] or "USD"
        rate = 1.0 if ccy == "USD" else fx_rates.monthly_rate(ccy, year, month)
        dep = float(r["dep_native"] or 0)
        wd = float(r["wd_native"] or 0)
        out[login] = {
            "dep_native": round(dep, 2),
            "wd_native": round(wd, 2),
            "dep_usd": round(dep * rate, 2),
            "wd_usd": round(wd * rate, 2),
            "currency": ccy,
            "tx_count": int(r["tx_count"] or 0),
        }
    return out


def equity_by_client(year: int, month: int) -> list:
    """Last balance & equity per login for the month, from mt5_daily (MRS Export #1).
    Same shape as queries.equity_by_client: [{login, currency, balance, equity, date}].
    Native balance/equity (no FX conversion — Dealio returns native too); test groups excluded.
    """
    ep_start, ep_end = _utc_month_epoch(year, month)
    table = _daily_table(year)
    with db.dw() as cur:
        cur.execute(
            f'SELECT DISTINCT ON ("Login") "Login" AS login, "Currency" AS currency, '
            f'  "Balance" AS balance, "ProfitEquity" AS equity, '
            f"  (to_timestamp(\"Datetime\") AT TIME ZONE 'UTC')::date AS date "
            f"FROM {table} "
            f'WHERE "Datetime" >= %s AND "Datetime" < %s '
            f"  AND \"Group\" NOT ILIKE '%%test%%' "
            f'  AND ("Balance" <> 0 OR "ProfitEquity" <> 0) '   # active book only (match Dealio scope)
            f'ORDER BY "Login", "Datetime" DESC',
            (ep_start, ep_end),
        )
        return [
            {"login": int(r["login"]), "currency": r["currency"] or "USD",
             "balance": float(r["balance"] or 0), "equity": float(r["equity"] or 0),
             "date": r["date"]}
            for r in cur.fetchall()
        ]


def profitability_by_day(year: int, month: int) -> list:
    """Daily realised + unrealised P&L per login for the month, from mt5_daily (MRS Export #3).
    Same shape as queries.profitability_by_day:
      [{login, date, currency, realised_pnl, unrealised_pnl_eod, balance, equity}].
    realised_pnl=DailyProfit, unrealised_pnl_eod=Profit (MT5-native floating). Active-book
    scope (nonzero balance/equity or daily P&L), test groups excluded, UTC-date aligned.
    """
    ep_start, ep_end = _utc_month_epoch(year, month)
    table = _daily_table(year)
    with db.dw() as cur:
        cur.execute(
            f'SELECT "Login" AS login, '
            f"  (to_timestamp(\"Datetime\") AT TIME ZONE 'UTC')::date AS date, "
            f'  "Currency" AS currency, '
            # Dealio closedpnl = NET realised result: gross trade profit + swaps + commissions
            f'  ("DailyProfit" + "DailyStorage" + "DailyCommInstant" + "DailyCommFee" + "DailyCommRound") AS realised_pnl, '
            f'  "Profit" AS unrealised_pnl_eod, "Balance" AS balance, "ProfitEquity" AS equity '
            f"FROM {table} "
            f'WHERE "Datetime" >= %s AND "Datetime" < %s '
            f"  AND \"Group\" NOT ILIKE '%%test%%' "
            f'  AND ("Balance" <> 0 OR "ProfitEquity" <> 0 OR "DailyProfit" <> 0) '
            f'ORDER BY "Login", "Datetime"',
            (ep_start, ep_end),
        )
        return [
            {"login": int(r["login"]), "date": r["date"], "currency": r["currency"] or "USD",
             "realised_pnl": float(r["realised_pnl"] or 0),
             "unrealised_pnl_eod": float(r["unrealised_pnl_eod"] or 0),
             "balance": float(r["balance"] or 0), "equity": float(r["equity"] or 0)}
            for r in cur.fetchall()
        ]
