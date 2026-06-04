"""External FX rates for currency conversion (replaces dealio.ticks).

Cash movements (deposits/withdrawals) must be valued in USD at TRUE-MARKET rates,
not the broker's internal spread-loaded MT5 prices. We scrape USD<->CCY from
liveexchanges.com (the same source the MT5-CRO backend already uses for
`external_rates`) and snapshot it daily into fees.db (`fx_external_daily`), so a
given month converts at that month's average rate.

Rate convention: `mid_to_usd` = USD per 1 unit of CCY (USD itself = 1.0).
e.g. KES mid_to_usd ~= 0.0077  (1 KES = 0.0077 USD).

Ported/condensed from MT5-CRO-Backend/app/external_rates.py. Runs inside the
recon container (FEES_MODE=demo -> sqlite fees.db).
"""

import datetime
import logging
import re

import requests

import db

log = logging.getLogger(__name__)

# Non-USD group currencies present on the book (see mt5_groups.Currency distribution).
CURRENCIES = ("EUR", "GBP", "KES", "MXN", "NGN", "ZAR", "AED")
_URL = "https://liveexchanges.com/convert-USD-{ccy}.html"
_TIMEOUT = 20
_UA = "Mozilla/5.0 (compatible; cmtaccounting-recon/1.0)"
_NUM = r"([\d]{1,3}(?:,[\d]{3})*(?:\.[\d]+)?|[\d]+\.[\d]+|[\d]+)"


def ensure_table():
    """Create the daily-rate history table if missing (sqlite fees.db)."""
    with db.fees_db() as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS fx_external_daily ("
            " d           TEXT NOT NULL,"        # ISO date 'YYYY-MM-DD'
            " currency    TEXT NOT NULL,"
            " mid_to_usd  REAL NOT NULL,"        # USD per 1 unit CCY
            " fetched_at  TEXT NOT NULL,"
            " PRIMARY KEY (d, currency))"
        )


def _parse_usd_to_ccy(html: str, ccy: str) -> float:
    """Return X where 1 USD = X CCY (or 0.0 if not parseable).

    liveexchanges.com renders rates as e.g. <span id="USDKES_RATE">129.15</span>
    (direct) or <span id="KESUSD_RATE">0.0077</span> (inverse — we flip it).
    """
    if not html:
        return 0.0
    c = re.escape(ccy)
    m = re.search(rf'id\s*=\s*["\']USD{c}_RATE["\'][^>]*>\s*{_NUM}', html, re.I)
    if m:
        try:
            return float(m.group(1).replace(",", ""))
        except ValueError:
            pass
    m = re.search(rf'id\s*=\s*["\']{c}USD_RATE["\'][^>]*>\s*{_NUM}', html, re.I)
    if m:
        try:
            inv = float(m.group(1).replace(",", ""))
            return 1.0 / inv if inv else 0.0
        except ValueError:
            pass
    return 0.0


def fetch_live() -> dict:
    """{ccy: mid_to_usd} for all CURRENCIES (+ USD=1.0). Currencies that fail are skipped."""
    sess = requests.Session()
    headers = {"User-Agent": _UA, "Accept": "text/html"}
    out = {"USD": 1.0}
    for ccy in CURRENCIES:
        try:
            r = sess.get(_URL.format(ccy=ccy), headers=headers, timeout=_TIMEOUT)
            r.raise_for_status()
            usd_to_ccy = _parse_usd_to_ccy(r.text, ccy)
            if usd_to_ccy > 0:
                out[ccy] = 1.0 / usd_to_ccy
            else:
                log.warning("fx: could not parse rate for %s", ccy)
        except Exception as e:  # network/HTTP/parse — skip this ccy, keep the rest
            log.warning("fx: fetch %s failed: %s", ccy, e)
    return out


def store_today(today: str = None) -> int:
    """Fetch live rates and upsert today's row per currency. Returns rows stored.
    Intended to run once daily (scheduled) so fx_external_daily builds month history."""
    ensure_table()
    d = today or datetime.date.today().isoformat()
    now = datetime.datetime.utcnow().isoformat(timespec="seconds")
    rates = fetch_live()
    n = 0
    with db.fees_db() as conn:
        for ccy, mid in rates.items():
            conn.execute("DELETE FROM fx_external_daily WHERE d = ? AND currency = ?", (d, ccy))
            conn.execute(
                "INSERT INTO fx_external_daily (d, currency, mid_to_usd, fetched_at) "
                "VALUES (?, ?, ?, ?)",
                (d, ccy, float(mid), now),
            )
            n += 1
    return n


def monthly_rate(ccy: str, year: int, month: int) -> float:
    """USD per 1 unit of `ccy`, averaged over the given month.

    Order of preference: that month's recorded average -> latest stored rate ->
    a live fetch -> 1.0 (last-resort, treat as USD-equivalent rather than crash).
    USD always returns 1.0.
    """
    ccy = (ccy or "USD").upper()
    if ccy == "USD":
        return 1.0
    ensure_table()
    start = datetime.date(year, month, 1).isoformat()
    end = (datetime.date(year + 1, 1, 1) if month == 12
           else datetime.date(year, month + 1, 1)).isoformat()
    with db.fees_db() as conn:
        row = conn.execute(
            "SELECT AVG(mid_to_usd) AS r FROM fx_external_daily "
            "WHERE currency = ? AND d >= ? AND d < ?",
            (ccy, start, end),
        ).fetchone()
        if row and row["r"] is not None:
            return float(row["r"])
        row = conn.execute(
            "SELECT mid_to_usd AS r FROM fx_external_daily WHERE currency = ? "
            "ORDER BY d DESC LIMIT 1",
            (ccy,),
        ).fetchone()
        if row and row["r"] is not None:
            return float(row["r"])
    try:
        live = fetch_live()
        if ccy in live:
            return live[ccy]
    except Exception:
        pass
    log.warning("fx: no rate for %s %04d-%02d; defaulting to 1.0", ccy, year, month)
    return 1.0


def live_rates() -> dict:
    """{ccy: mid_to_usd} from the most recent stored snapshot (falls back to a live fetch)."""
    ensure_table()
    with db.fees_db() as conn:
        rows = conn.execute(
            "SELECT currency, mid_to_usd FROM fx_external_daily "
            "WHERE d = (SELECT MAX(d) FROM fx_external_daily)"
        ).fetchall()
    if rows:
        out = {r["currency"]: float(r["mid_to_usd"]) for r in rows}
        out.setdefault("USD", 1.0)
        return out
    return fetch_live()
