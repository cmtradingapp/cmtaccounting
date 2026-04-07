"""All reconciliation SQL lives here."""

import time
from db import dealio, backoffice, fees_db

# ── Simple TTL cache ───────────────────────────────────────────────────────
# Keyed by arbitrary string; value is (stored_at_timestamp, data).
# reconcile() results are expensive (two remote DBs) — cache for 5 minutes.
# available_months() is cheap but called on every page load — cache for 2 min.

_CACHE: dict = {}
_TTL_RECONCILE = 300   # 5 minutes
_TTL_MONTHS    = 120   # 2 minutes


def _cache_get(key: str, ttl: int):
    entry = _CACHE.get(key)
    if entry and (time.time() - entry[0]) < ttl:
        return entry[1]
    return None


def _cache_set(key: str, value):
    _CACHE[key] = (time.time(), value)


def cache_invalidate(year: int = None, month: int = None):
    """Clear cached reconciliation data.
    If year+month given, clears only that period; otherwise clears all recon cache.
    Always clears the months list too.
    """
    if year and month:
        _CACHE.pop(f"reconcile:{year}:{month}", None)
    else:
        for k in [k for k in _CACHE if k.startswith("reconcile:")]:
            _CACHE.pop(k, None)
    _CACHE.pop("available_months", None)


def cache_age(year: int, month: int) -> int | None:
    """Return how many seconds ago this month was cached, or None if not cached."""
    entry = _CACHE.get(f"reconcile:{year}:{month}")
    return int(time.time() - entry[0]) if entry else None


def cache_age_key(key: str) -> int | None:
    """Return how many seconds ago an arbitrary cache key was stored, or None."""
    entry = _CACHE.get(key)
    return int(time.time() - entry[0]) if entry else None

# Payment methods that represent real cash movements
CASH_METHODS = {
    'Wire transfer', 'Wire', 'External', 'Credit card', 'CreditCard',
    'Electronic payment', 'ElectronicPayment', 'CryptoWallet', 'Crypto',
    'Cash', 'CashDeposit',
}

# Transaction types that are internal / non-cash
NON_CASH_TYPES = {
    'Credit in', 'Credit out', 'TransferIn', 'TransferOut', 'Fee',
}

# Payment methods that are non-cash regardless of transactiontype
NON_CASH_METHODS = {
    'Bonus', 'BonusProtectedPositionCashback', 'BonusInsuredPositionCashback',
    'BonusProtectedPositionCredit', 'BonusSpreadCashback', 'BonusSpreadCredit',
    'BonusInsuredPositionCredit', 'Commission', 'FRF commission', 'IB commission',
    'FRFCommission', 'IBCommission', 'Processing fees', 'ProcessingFee',
    'Adjustment', 'Chargeback', 'Migration', 'InternalTransfer',
}


def _is_cash(payment_method, transactiontype):
    if transactiontype in NON_CASH_TYPES:
        return False
    if payment_method in NON_CASH_METHODS:
        return False
    return True


def available_months():
    """Months that have data in backoffice, most recent first."""
    cached = _cache_get("available_months", _TTL_MONTHS)
    if cached is not None:
        return cached
    with backoffice() as cur:
        cur.execute("""
            SELECT DISTINCT TO_CHAR(DATE_TRUNC('month', confirmation_time), 'YYYY-MM') AS month
            FROM vtiger_mttransactions
            WHERE transactionapproval = 'Approved'
              AND confirmation_time IS NOT NULL
            ORDER BY month DESC
            LIMIT 36
        """)
        result = [r["month"] for r in cur.fetchall()]
    _cache_set("available_months", result)
    return result


def crm_summary(year: int, month: int):
    """Per-login totals from CRM, split into cash vs non-cash."""
    with backoffice() as cur:
        cur.execute("""
            SELECT
                login,
                transactiontype,
                payment_method,
                transactionapproval,
                COUNT(*)          AS tx_count,
                SUM(usdamount)    AS total_usd
            FROM vtiger_mttransactions
            WHERE EXTRACT(YEAR  FROM confirmation_time) = %s
              AND EXTRACT(MONTH FROM confirmation_time) = %s
              AND transactionapproval = 'Approved'
            GROUP BY login, transactiontype, payment_method, transactionapproval
        """, (year, month))
        rows = cur.fetchall()

    summary = {}
    for r in rows:
        login = r["login"]
        if login not in summary:
            summary[login] = {
                "cash_deposits": 0, "cash_withdrawals": 0,
                "noncash_in": 0, "noncash_out": 0,
                "tx_count": 0, "payment_methods": set(),
            }
        s = summary[login]
        amt = float(r["total_usd"] or 0)
        is_cash = _is_cash(r["payment_method"] or "", r["transactiontype"] or "")
        is_deposit = r["transactiontype"] in ("Deposit", "TransferIn", "Credit in")
        is_withdraw = r["transactiontype"] in ("Withdrawal", "Withdraw", "TransferOut", "Credit out")

        if is_cash and is_deposit:
            s["cash_deposits"] += amt
        elif is_cash and is_withdraw:
            s["cash_withdrawals"] += amt
        elif not is_cash and is_deposit:
            s["noncash_in"] += amt
        elif not is_cash and is_withdraw:
            s["noncash_out"] += amt

        s["tx_count"] += r["tx_count"]
        if r["payment_method"]:
            s["payment_methods"].add(r["payment_method"])

    for login, s in summary.items():
        s["cash_net"] = round(s["cash_deposits"] - s["cash_withdrawals"], 2)
        s["payment_methods"] = ", ".join(sorted(s["payment_methods"]))

    return summary


def mt4_summary(year: int, month: int):
    """Per-login net deposit from dealio daily_profits."""
    with dealio() as cur:
        cur.execute("""
            SELECT
                login,
                SUM(convertednetdeposit) AS net_usd,
                groupcurrency,
                AVG(conversionratio)     AS avg_fx
            FROM dealio.daily_profits
            WHERE EXTRACT(YEAR  FROM date) = %s
              AND EXTRACT(MONTH FROM date) = %s
              AND netdeposit != 0
            GROUP BY login, groupcurrency
        """, (year, month))
        return {r["login"]: dict(r) for r in cur.fetchall()}


def reconcile(year: int, month: int):
    """Join MT4 netdeposit vs CRM cash transactions per login (cached 5 min)."""
    key = f"reconcile:{year}:{month}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached

    crm = crm_summary(year, month)
    mt4 = mt4_summary(year, month)

    rows = []
    for login in set(crm) | set(mt4):
        c = crm.get(login, {})
        m = mt4.get(login, {})

        mt4_net  = round(float(m.get("net_usd") or 0), 2)
        crm_cash = round(c.get("cash_net", 0), 2)
        diff     = round(mt4_net - crm_cash, 2)

        if login not in crm:
            status = "mt4_only"
        elif login not in mt4:
            status = "crm_only"
        elif abs(diff) < 1.0:
            status = "matched"
        else:
            status = "discrepancy"

        rows.append({
            "login":            login,
            "mt4_net":          mt4_net,
            "crm_cash_net":     crm_cash,
            "crm_cash_dep":     round(c.get("cash_deposits", 0), 2),
            "crm_cash_with":    round(c.get("cash_withdrawals", 0), 2),
            "crm_noncash_in":   round(c.get("noncash_in", 0), 2),
            "crm_noncash_out":  round(c.get("noncash_out", 0), 2),
            "difference":       diff,
            "abs_diff":         abs(diff),
            "status":           status,
            "payment_methods":  c.get("payment_methods", ""),
            "tx_count":         c.get("tx_count", 0),
            "currency":         m.get("groupcurrency", "USD"),
        })

    rows.sort(key=lambda r: r["abs_diff"], reverse=True)

    # Add cash_methods: only the subset of payment_methods that are cash-relevant
    for r in rows:
        raw = [m.strip() for m in (r.get("payment_methods") or "").split(",") if m.strip()]
        cash_only = [m for m in raw if m in CASH_METHODS]
        r["cash_methods"] = ", ".join(cash_only) if cash_only else None

    _cache_set(key, rows)
    return rows


# ── FX Rate queries (dealio.ticks) ────────────────────────────────────────

_TTL_FX_LIVE      = 5    # 5 seconds  — live rate display (uses live_ticks, small hot table)
_TTL_FX_REFERENCE = 60   # 60 seconds — reference price for % calculation
_TTL_FX_HISTORY   = 300  # 5 minutes  — historical chart data
_TTL_FX_MONTHLY   = 300  # 5 minutes  — monthly averages for fee calculator

# Curated symbol groups shown on the FX Rates page
FX_GROUPS = {
    "Major":              ["EURUSD", "GBPUSD", "USDJPY", "USDCAD", "USDCHF", "AUDUSD", "NZDUSD"],
    "African & Emerging": ["USDNGN", "USDKES", "USDZAR", "USDMXN", "USDTRY", "USDINR", "USDAED"],
    "Crypto":             ["BTCUSD", "ETHUSD", "XAUUSD", "XAGUSD"],
}
FX_ALL_SYMBOLS = [s for g in FX_GROUPS.values() for s in g]


def get_live_fx_rates(symbols: list = None) -> list:
    """Latest bid/ask/mid per symbol from dealio.live_ticks (hot table, current state only).
    Falls back to dealio.ticks if live_ticks has no data for a symbol.
    Cached 5 s — DB hit is rare; all client polls within the window are served from cache.
    """
    syms = symbols or FX_ALL_SYMBOLS
    key = "fx_live:" + ",".join(sorted(syms))
    cached = _cache_get(key, _TTL_FX_LIVE)
    if cached is not None:
        return cached

    placeholders = ",".join(["%s"] * len(syms))
    with dealio() as cur:
        # live_ticks has one row per symbol — very fast, no time-range scan needed
        cur.execute(f"""
            SELECT DISTINCT ON (symbol)
                symbol,
                bid,
                ask,
                ROUND(((bid + ask) / 2.0)::numeric, 5) AS mid,
                lastmodified
            FROM dealio.live_ticks
            WHERE symbol IN ({placeholders})
            ORDER BY symbol, lastmodified DESC
        """, syms)
        rows = [dict(r) for r in cur.fetchall()]

    # Any symbols missing from live_ticks? Fill from ticks as fallback
    found = {r["symbol"] for r in rows}
    missing = [s for s in syms if s not in found]
    if missing:
        mp = ",".join(["%s"] * len(missing))
        with dealio() as cur:
            cur.execute(f"""
                SELECT DISTINCT ON (symbol)
                    symbol, bid, ask,
                    ROUND(((bid + ask) / 2.0)::numeric, 5) AS mid,
                    lastmodified
                FROM dealio.ticks
                WHERE symbol IN ({mp})
                ORDER BY symbol, lastmodified DESC
            """, missing)
            rows += [dict(r) for r in cur.fetchall()]

    # Preserve group order
    order = {s: i for i, s in enumerate(syms)}
    rows.sort(key=lambda r: order.get(r["symbol"], 999))
    # Serialize datetime for JSON
    for r in rows:
        if r.get("lastmodified"):
            r["lastmodified"] = r["lastmodified"].isoformat()
        for col in ("bid", "ask", "mid"):
            if r.get(col) is not None:
                r[col] = float(r[col])

    _cache_set(key, rows)
    return rows


def get_reference_fx_rates(minutes_ago: int, symbols: list = None) -> dict:
    """Mid-rate for each symbol at approximately N minutes ago. Used for % change calculation.
    Finds the most recent tick at or before (NOW - interval). Cached 60 s.
    Returns {symbol: mid_float}.
    """
    syms = symbols or FX_ALL_SYMBOLS
    key  = f"fx_ref:{minutes_ago}:" + ",".join(sorted(syms))
    cached = _cache_get(key, _TTL_FX_REFERENCE)
    if cached is not None:
        return cached

    placeholders = ",".join(["%s"] * len(syms))
    with dealio() as cur:
        cur.execute(f"""
            SELECT DISTINCT ON (symbol)
                symbol,
                ROUND(((bid + ask) / 2.0)::numeric, 6) AS mid
            FROM dealio.ticks
            WHERE symbol IN ({placeholders})
              AND lastmodified <= NOW() - ({minutes_ago}::int * INTERVAL '1 minute')
            ORDER BY symbol, lastmodified DESC
        """, syms)
        result = {r["symbol"]: float(r["mid"]) for r in cur.fetchall()}

    _cache_set(key, result)
    return result


def get_fx_history(symbol: str, hours: int = 168) -> list:
    """Hourly average mid-rate for a symbol over the last N hours. Cached 5 min."""
    key = f"fx_history:{symbol}:{hours}"
    cached = _cache_get(key, _TTL_FX_HISTORY)
    if cached is not None:
        return cached

    with dealio() as cur:
        cur.execute("""
            SELECT
                date_trunc('hour', lastmodified) AS ts,
                ROUND(AVG((bid + ask) / 2.0)::numeric, 5) AS mid
            FROM dealio.ticks
            WHERE symbol = %s
              AND lastmodified >= NOW() - (%s || ' hours')::interval
            GROUP BY 1
            ORDER BY 1
        """, (symbol, str(hours)))
        rows = [{"ts": r["ts"].isoformat(), "mid": float(r["mid"])} for r in cur.fetchall()]

    _cache_set(key, rows)
    return rows


def get_monthly_fx_rate(symbol: str, year: int, month: int) -> float | None:
    """Monthly average mid-rate for fee calculations. Cached 5 min.
    Falls back to None if no ticks data for that period (pre-Sept 2025).
    """
    key = f"fx_monthly:{symbol}:{year}:{month}"
    cached = _cache_get(key, _TTL_FX_MONTHLY)
    if cached is not None:
        return cached

    with dealio() as cur:
        cur.execute("""
            SELECT ROUND(AVG((bid + ask) / 2.0)::numeric, 6) AS mid
            FROM dealio.ticks
            WHERE symbol = %s
              AND EXTRACT(YEAR  FROM lastmodified) = %s
              AND EXTRACT(MONTH FROM lastmodified) = %s
        """, (symbol, year, month))
        row = cur.fetchone()
        result = float(row["mid"]) if row and row["mid"] is not None else None

    _cache_set(key, result)
    return result


def summary_stats(rows):
    matched     = sum(1 for r in rows if r["status"] == "matched")
    discrepancy = sum(1 for r in rows if r["status"] == "discrepancy")
    mt4_only    = sum(1 for r in rows if r["status"] == "mt4_only")
    crm_only    = sum(1 for r in rows if r["status"] == "crm_only")
    total_diff  = sum(r["difference"] for r in rows if r["status"] == "discrepancy")
    return {
        "total":       len(rows),
        "matched":     matched,
        "discrepancy": discrepancy,
        "mt4_only":    mt4_only,
        "crm_only":    crm_only,
        "total_diff":  round(total_diff, 2),
        "match_rate":  round(matched / len(rows) * 100, 1) if rows else 0,
    }


def login_detail(year: int, month: int, login: int):
    """All CRM transactions for a specific login in a given month."""
    with backoffice() as cur:
        cur.execute("""
            SELECT
                transactiontype,
                payment_method,
                transactionapproval,
                COUNT(*)       AS tx_count,
                SUM(usdamount) AS total_usd,
                MIN(confirmation_time::date) AS first_date,
                MAX(confirmation_time::date) AS last_date
            FROM vtiger_mttransactions
            WHERE login = %s
              AND EXTRACT(YEAR  FROM confirmation_time) = %s
              AND EXTRACT(MONTH FROM confirmation_time) = %s
            GROUP BY transactiontype, payment_method, transactionapproval
            ORDER BY transactionapproval, transactiontype, payment_method
        """, (login, year, month))
        rows = cur.fetchall()

    result = []
    for r in rows:
        is_cash = _is_cash(r["payment_method"] or "", r["transactiontype"] or "")
        result.append({
            **dict(r),
            "total_usd": round(float(r["total_usd"] or 0), 2),
            "is_cash":   is_cash,
        })
    return result


def login_mt4_detail(year: int, month: int, login: int):
    """Daily MT4 net deposit entries for a specific login."""
    with dealio() as cur:
        cur.execute("""
            SELECT date, netdeposit, convertednetdeposit, balance, equity,
                   closedpnl, floatingpnl, groupcurrency, conversionratio
            FROM dealio.daily_profits
            WHERE login = %s
              AND EXTRACT(YEAR  FROM date) = %s
              AND EXTRACT(MONTH FROM date) = %s
              AND netdeposit != 0
            ORDER BY date
        """, (login, year, month))
        return [dict(r) for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# PSP Fee Management
# ---------------------------------------------------------------------------

def ensure_fee_tables():
    _ensure_fee_tables_core()
    _ensure_prompt_tables()
    _ensure_context_notes_table()
    _ensure_amendment_tables()


def _ensure_prompt_tables():
    """Create and seed the prompt_templates table."""
    import ai_parse
    with fees_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS prompt_templates (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT NOT NULL,
                system_prompt TEXT NOT NULL,
                is_default  INTEGER DEFAULT 0,
                created_at  TEXT DEFAULT (datetime('now')),
                updated_at  TEXT DEFAULT (datetime('now'))
            );
        """)
        # Migration: add prompt_type column if it doesn't exist yet
        try:
            conn.execute(
                "ALTER TABLE prompt_templates ADD COLUMN prompt_type TEXT DEFAULT 'agreement'"
            )
        except Exception:
            pass  # column already exists

        # Migration: add is_builtin column (built-in templates are read-only)
        try:
            conn.execute(
                "ALTER TABLE prompt_templates ADD COLUMN is_builtin INTEGER DEFAULT 0"
            )
        except Exception:
            pass  # column already exists

        existing = conn.execute("SELECT COUNT(*) FROM prompt_templates").fetchone()[0]
        if existing == 0:
            conn.execute(
                "INSERT INTO prompt_templates (name, system_prompt, is_default, prompt_type, is_builtin) VALUES (?, ?, 1, 'agreement', 1)",
                ("Default — Standard Fee Agreement Parser", ai_parse.SYSTEM_PROMPT),
            )

        # Ensure every type has at least one default
        for ptype in ("agreement", "amendment"):
            has_default = conn.execute(
                "SELECT COUNT(*) FROM prompt_templates WHERE prompt_type=? AND is_default=1",
                (ptype,)
            ).fetchone()[0]
            if not has_default:
                # Promote the first template of that type to default
                conn.execute("""
                    UPDATE prompt_templates SET is_default = 1
                    WHERE id = (SELECT id FROM prompt_templates WHERE prompt_type=? ORDER BY id LIMIT 1)
                """, (ptype,))

        # Seed default amendment template if none exists
        amend_count = conn.execute(
            "SELECT COUNT(*) FROM prompt_templates WHERE prompt_type = 'amendment'"
        ).fetchone()[0]
        if amend_count == 0:
            conn.execute(
                "INSERT INTO prompt_templates (name, system_prompt, is_default, prompt_type, is_builtin) VALUES (?, ?, 1, 'amendment', 1)",
                ("Default — Amendment Parser", ai_parse.AMENDMENT_SYSTEM_PROMPT),
            )

        # Ensure existing seeded templates are marked is_builtin if they were created before the column existed
        for builtin_name in ("Default — Standard Fee Agreement Parser", "Default — Amendment Parser"):
            conn.execute(
                "UPDATE prompt_templates SET is_builtin=1 WHERE name=? AND is_builtin=0",
                (builtin_name,)
            )


# --- Prompt templates ---

def get_prompt_templates():
    with fees_db() as conn:
        rows = conn.execute(
            "SELECT id, name, is_default, prompt_type, is_builtin, created_at, updated_at "
            "FROM prompt_templates ORDER BY is_builtin DESC, prompt_type, is_default DESC, name"
        ).fetchall()
        return [dict(r) for r in rows]


def get_prompt_template(template_id: int):
    with fees_db() as conn:
        row = conn.execute(
            "SELECT * FROM prompt_templates WHERE id = ?", (template_id,)
        ).fetchone()
        return dict(row) if row else None


def get_default_prompt_template():
    with fees_db() as conn:
        row = conn.execute(
            "SELECT * FROM prompt_templates WHERE is_default = 1 ORDER BY id LIMIT 1"
        ).fetchone()
        return dict(row) if row else None


def create_prompt_template(name: str, system_prompt: str, prompt_type: str = "agreement") -> int:
    with fees_db() as conn:
        cur = conn.execute(
            "INSERT INTO prompt_templates (name, system_prompt, prompt_type) VALUES (?, ?, ?)",
            (name, system_prompt, prompt_type),
        )
        return cur.lastrowid


def update_prompt_template(template_id: int, name: str, system_prompt: str, prompt_type: str = "agreement"):
    with fees_db() as conn:
        conn.execute(
            "UPDATE prompt_templates SET name=?, system_prompt=?, prompt_type=?, updated_at=datetime('now') WHERE id=?",
            (name, system_prompt, prompt_type, template_id),
        )


def set_default_prompt_template(template_id: int):
    with fees_db() as conn:
        # Determine the type of the template being set as default
        row = conn.execute(
            "SELECT prompt_type FROM prompt_templates WHERE id = ?", (template_id,)
        ).fetchone()
        if row:
            # Only clear the default flag within the same type
            conn.execute(
                "UPDATE prompt_templates SET is_default = 0 WHERE prompt_type = ?",
                (row["prompt_type"],)
            )
        conn.execute("UPDATE prompt_templates SET is_default = 1 WHERE id = ?", (template_id,))


def delete_prompt_template(template_id: int):
    with fees_db() as conn:
        conn.execute("DELETE FROM prompt_templates WHERE id = ?", (template_id,))


# --- Context Notes ---

def _ensure_context_notes_table():
    with fees_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS context_notes (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                label        TEXT NOT NULL,
                text         TEXT NOT NULL,
                use_count    INTEGER DEFAULT 0,
                created_at   TEXT DEFAULT (datetime('now')),
                last_used_at TEXT
            );
        """)
        # Migration: add note_type column if missing
        try:
            conn.execute(
                "ALTER TABLE context_notes ADD COLUMN note_type TEXT DEFAULT 'agreement'"
            )
        except Exception:
            pass  # column already exists


def _ensure_amendment_tables():
    """Create amendment history and upload-cache tables; migrate agreements to store files."""
    with fees_db() as conn:
        conn.executescript("""
            -- Temporary cache: holds uploaded file between /fees/upload POST and confirm POST
            CREATE TABLE IF NOT EXISTS amendment_upload_cache (
                token       TEXT PRIMARY KEY,
                filename    TEXT,
                file_data   BLOB,
                created_at  TEXT DEFAULT (datetime('now'))
            );

            -- Permanent amendment log
            CREATE TABLE IF NOT EXISTS psp_amendments (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                agreement_id    INTEGER NOT NULL REFERENCES psp_agreements(id),
                addendum_date   TEXT,
                applied_at      TEXT DEFAULT (datetime('now')),
                filename        TEXT,
                file_data       BLOB,
                notes           TEXT,
                changes_applied INTEGER DEFAULT 0
            );

            -- Per-rule change snapshot within an amendment
            CREATE TABLE IF NOT EXISTS psp_amendment_changes (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                amendment_id    INTEGER NOT NULL REFERENCES psp_amendments(id),
                action          TEXT NOT NULL,
                fee_rule_id     INTEGER,
                old_payment_method  TEXT, old_fee_type TEXT, old_country TEXT,
                old_fee_kind        TEXT, old_pct_rate REAL,
                old_fixed_amount    REAL, old_fixed_currency TEXT, old_description TEXT,
                new_payment_method  TEXT, new_fee_type TEXT, new_country TEXT,
                new_fee_kind        TEXT, new_pct_rate REAL,
                new_fixed_amount    REAL, new_fixed_currency TEXT, new_description TEXT
            );
        """)
        # Migrate psp_agreements to store initial agreement file
        for col in ("agr_filename TEXT", "agr_file_data BLOB"):
            try:
                conn.execute(f"ALTER TABLE psp_agreements ADD COLUMN {col}")
            except Exception:
                pass
        # Expire old cache entries (> 24 h)
        conn.execute(
            "DELETE FROM amendment_upload_cache "
            "WHERE created_at < datetime('now', '-1 day')"
        )


# --- Amendment history ---

def cache_upload(token: str, filename: str, file_data: bytes):
    with fees_db() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO amendment_upload_cache (token, filename, file_data) VALUES (?, ?, ?)",
            (token, filename, file_data)
        )


def pop_upload_cache(token: str):
    """Return (filename, file_data) and delete the cache entry."""
    with fees_db() as conn:
        row = conn.execute(
            "SELECT filename, file_data FROM amendment_upload_cache WHERE token=?", (token,)
        ).fetchone()
        if row:
            conn.execute("DELETE FROM amendment_upload_cache WHERE token=?", (token,))
            return row["filename"], bytes(row["file_data"])
    return None, None


def create_amendment_record(agreement_id, addendum_date, filename, file_data, notes, changes_applied):
    with fees_db() as conn:
        cur = conn.execute("""
            INSERT INTO psp_amendments
              (agreement_id, addendum_date, filename, file_data, notes, changes_applied)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (agreement_id, addendum_date, filename, file_data, notes, changes_applied))
        return cur.lastrowid


def add_amendment_change(amendment_id, action, fee_rule_id, old_rule, new_rule):
    def _r(d, k): return (d or {}).get(k)
    with fees_db() as conn:
        conn.execute("""
            INSERT INTO psp_amendment_changes
              (amendment_id, action, fee_rule_id,
               old_payment_method, old_fee_type, old_country, old_fee_kind,
               old_pct_rate, old_fixed_amount, old_fixed_currency, old_description,
               new_payment_method, new_fee_type, new_country, new_fee_kind,
               new_pct_rate, new_fixed_amount, new_fixed_currency, new_description)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            amendment_id, action, fee_rule_id,
            _r(old_rule,"payment_method"), _r(old_rule,"fee_type"), _r(old_rule,"country"),
            _r(old_rule,"fee_kind"), _r(old_rule,"pct_rate"),
            _r(old_rule,"fixed_amount"), _r(old_rule,"fixed_currency"), _r(old_rule,"description"),
            _r(new_rule,"payment_method"), _r(new_rule,"fee_type"), _r(new_rule,"country"),
            _r(new_rule,"fee_kind"), _r(new_rule,"pct_rate"),
            _r(new_rule,"fixed_amount"), _r(new_rule,"fixed_currency"), _r(new_rule,"description"),
        ))


def get_amendments(agreement_id):
    with fees_db() as conn:
        rows = conn.execute("""
            SELECT id, addendum_date, applied_at, filename, notes, changes_applied
            FROM psp_amendments WHERE agreement_id=? ORDER BY applied_at DESC
        """, (agreement_id,)).fetchall()
        return [dict(r) for r in rows]


def get_amendment(amendment_id):
    with fees_db() as conn:
        row = conn.execute(
            "SELECT * FROM psp_amendments WHERE id=?", (amendment_id,)
        ).fetchone()
        if not row:
            return None
        amend = dict(row)
        amend.pop("file_data", None)  # don't return blob in normal fetch
        changes = conn.execute(
            "SELECT * FROM psp_amendment_changes WHERE amendment_id=? ORDER BY id",
            (amendment_id,)
        ).fetchall()
        amend["changes"] = [dict(c) for c in changes]
        return amend


def get_amendment_file(amendment_id):
    with fees_db() as conn:
        row = conn.execute(
            "SELECT filename, file_data FROM psp_amendments WHERE id=?", (amendment_id,)
        ).fetchone()
        if row and row["file_data"]:
            return row["filename"], bytes(row["file_data"])
    return None, None


def save_agreement_file(agreement_id, filename, file_data):
    with fees_db() as conn:
        conn.execute(
            "UPDATE psp_agreements SET agr_filename=?, agr_file_data=? WHERE id=?",
            (filename, file_data, agreement_id)
        )


def get_agreement_file(agreement_id):
    with fees_db() as conn:
        row = conn.execute(
            "SELECT agr_filename, agr_file_data FROM psp_agreements WHERE id=?",
            (agreement_id,)
        ).fetchone()
        if row and row["agr_file_data"]:
            return row["agr_filename"], bytes(row["agr_file_data"])
    return None, None


def get_context_notes():
    """Return all notes sorted by type then use_count desc, with usage_pct calculated."""
    with fees_db() as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM context_notes ORDER BY note_type, use_count DESC, label"
        ).fetchall()]
    total = sum(r["use_count"] for r in rows) or 1
    for r in rows:
        r["usage_pct"] = round(r["use_count"] / total * 100, 1)
        r.setdefault("note_type", "agreement")
    return rows


def save_context_note(label: str, text: str, note_type: str = "agreement") -> int:
    with fees_db() as conn:
        cur = conn.execute(
            "INSERT INTO context_notes (label, text, note_type) VALUES (?, ?, ?)",
            (label, text, note_type)
        )
        return cur.lastrowid


def delete_context_note(note_id: int):
    with fees_db() as conn:
        conn.execute("DELETE FROM context_notes WHERE id = ?", (note_id,))


def increment_context_note_usage(note_ids: list):
    if not note_ids:
        return
    with fees_db() as conn:
        placeholders = ",".join("?" * len(note_ids))
        conn.execute(
            f"UPDATE context_notes SET use_count = use_count + 1, "
            f"last_used_at = datetime('now') WHERE id IN ({placeholders})",
            note_ids,
        )


def _ensure_fee_tables_core():
    """Create fee tables in local SQLite if they don't exist."""
    with fees_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS psp_agreements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                psp_name TEXT NOT NULL,
                provider_name TEXT,
                agreement_entity TEXT,
                agreement_date TEXT,
                addendum_date TEXT,
                auto_settlement INTEGER DEFAULT 0,
                settlement_bank TEXT,
                active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS psp_fee_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agreement_id INTEGER NOT NULL REFERENCES psp_agreements(id) ON DELETE CASCADE,
                payment_method TEXT,
                fee_type TEXT NOT NULL,
                country TEXT DEFAULT 'GLOBAL',
                sub_provider TEXT,
                fee_kind TEXT NOT NULL CHECK (fee_kind IN ('percentage','fixed','fixed_plus_pct','tiered')),
                pct_rate REAL,
                fixed_amount REAL,
                fixed_currency TEXT,
                description TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS psp_fee_tiers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fee_rule_id INTEGER NOT NULL REFERENCES psp_fee_rules(id) ON DELETE CASCADE,
                volume_from REAL NOT NULL DEFAULT 0,
                volume_to REAL,
                pct_rate REAL NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_fee_rules_agreement ON psp_fee_rules(agreement_id);
            CREATE INDEX IF NOT EXISTS idx_fee_tiers_rule ON psp_fee_tiers(fee_rule_id);
            CREATE TABLE IF NOT EXISTS agreement_entities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            );
        """)
        # Seed default entities if table is empty
        existing = conn.execute("SELECT COUNT(*) FROM agreement_entities").fetchone()[0]
        if existing == 0:
            conn.executemany("INSERT OR IGNORE INTO agreement_entities (name) VALUES (?)",
                             [("CMT PROCESSING LTD",), ("GCMT GROUP LTD",)])


# --- Agreements ---

def get_entities():
    with fees_db() as conn:
        return [r["name"] for r in conn.execute("SELECT name FROM agreement_entities ORDER BY name").fetchall()]

def add_entity(name):
    with fees_db() as conn:
        conn.execute("INSERT OR IGNORE INTO agreement_entities (name) VALUES (?)", (name,))

def delete_entity(name):
    with fees_db() as conn:
        conn.execute("DELETE FROM agreement_entities WHERE name = ?", (name,))


def get_all_agreements():
    with fees_db() as conn:
        rows = conn.execute("""
            SELECT a.*, COUNT(r.id) AS rule_count
            FROM psp_agreements a
            LEFT JOIN psp_fee_rules r ON r.agreement_id = a.id
            WHERE a.active = 1
            GROUP BY a.id
            ORDER BY a.psp_name
        """).fetchall()
        return [dict(r) for r in rows]


def get_terminated_agreements():
    with fees_db() as conn:
        rows = conn.execute("""
            SELECT a.*, COUNT(r.id) AS rule_count
            FROM psp_agreements a
            LEFT JOIN psp_fee_rules r ON r.agreement_id = a.id
            WHERE a.active = 0
            GROUP BY a.id
            ORDER BY a.updated_at DESC
        """).fetchall()
        return [dict(r) for r in rows]


def get_agreement(psp_id):
    with fees_db() as conn:
        row = conn.execute(
            "SELECT id, psp_name, provider_name, agreement_entity, agreement_date, "
            "addendum_date, auto_settlement, settlement_bank, active, "
            "agr_filename, created_at, updated_at "
            "FROM psp_agreements WHERE id = ?", (psp_id,)
        ).fetchone()
        return dict(row) if row else None


def create_agreement(data):
    with fees_db() as conn:
        cur = conn.execute("""
            INSERT INTO psp_agreements (psp_name, provider_name, agreement_entity,
                agreement_date, addendum_date, auto_settlement, settlement_bank)
            VALUES (:psp_name, :provider_name, :agreement_entity,
                :agreement_date, :addendum_date, :auto_settlement, :settlement_bank)
        """, data)
        return cur.lastrowid


def update_agreement(psp_id, data):
    data["id"] = psp_id
    with fees_db() as conn:
        conn.execute("""
            UPDATE psp_agreements SET
                psp_name = :psp_name, provider_name = :provider_name,
                agreement_entity = :agreement_entity,
                agreement_date = :agreement_date, addendum_date = :addendum_date,
                auto_settlement = :auto_settlement, settlement_bank = :settlement_bank,
                updated_at = datetime('now')
            WHERE id = :id
        """, data)


def delete_agreement(psp_id):
    with fees_db() as conn:
        conn.execute("UPDATE psp_agreements SET active = 0 WHERE id = ?", (psp_id,))


def update_addendum_date(psp_id, addendum_date):
    with fees_db() as conn:
        conn.execute(
            "UPDATE psp_agreements SET addendum_date=?, updated_at=datetime('now') WHERE id=?",
            (addendum_date, psp_id)
        )


# --- Fee Rules ---

def get_fee_rules(agreement_id):
    with fees_db() as conn:
        rules = [dict(r) for r in conn.execute("""
            SELECT * FROM psp_fee_rules
            WHERE agreement_id = ?
            ORDER BY payment_method, country, fee_type
        """, (agreement_id,)).fetchall()]

        tiers_by_rule = {}
        if rules:
            rule_ids = [r["id"] for r in rules]
            placeholders = ",".join("?" * len(rule_ids))
            for t in conn.execute(f"""
                SELECT * FROM psp_fee_tiers
                WHERE fee_rule_id IN ({placeholders})
                ORDER BY fee_rule_id, volume_from
            """, rule_ids).fetchall():
                t = dict(t)
                tiers_by_rule.setdefault(t["fee_rule_id"], []).append(t)

        for r in rules:
            r["tiers"] = tiers_by_rule.get(r["id"], [])
        return rules


def get_fee_rule(rule_id):
    with fees_db() as conn:
        row = conn.execute("SELECT * FROM psp_fee_rules WHERE id=?", (rule_id,)).fetchone()
        return dict(row) if row else None


def create_fee_rule(agreement_id, data):
    with fees_db() as conn:
        cur = conn.execute("""
            INSERT INTO psp_fee_rules (agreement_id, payment_method, fee_type, country,
                sub_provider, fee_kind, pct_rate, fixed_amount, fixed_currency, description)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (agreement_id, data["payment_method"], data["fee_type"], data["country"],
              data["sub_provider"], data["fee_kind"], data["pct_rate"],
              data["fixed_amount"], data["fixed_currency"], data["description"]))
        rule_id = cur.lastrowid

        if data["fee_kind"] == "tiered" and data.get("tiers"):
            for t in data["tiers"]:
                conn.execute("""
                    INSERT INTO psp_fee_tiers (fee_rule_id, volume_from, volume_to, pct_rate)
                    VALUES (?, ?, ?, ?)
                """, (rule_id, t["volume_from"], t["volume_to"], t["pct_rate"]))
        return rule_id


def update_fee_rule(rule_id, data):
    with fees_db() as conn:
        conn.execute("""
            UPDATE psp_fee_rules
            SET payment_method=?, fee_type=?, country=?, sub_provider=?,
                fee_kind=?, pct_rate=?, fixed_amount=?, fixed_currency=?, description=?
            WHERE id=?
        """, (data["payment_method"], data["fee_type"], data["country"],
              data["sub_provider"], data["fee_kind"], data["pct_rate"],
              data["fixed_amount"], data["fixed_currency"], data["description"], rule_id))
        conn.execute("DELETE FROM psp_fee_tiers WHERE fee_rule_id=?", (rule_id,))
        if data["fee_kind"] == "tiered" and data.get("tiers"):
            for t in data["tiers"]:
                conn.execute("""
                    INSERT INTO psp_fee_tiers (fee_rule_id, volume_from, volume_to, pct_rate)
                    VALUES (?, ?, ?, ?)
                """, (rule_id, t["volume_from"], t["volume_to"], t["pct_rate"]))


def delete_fee_rule(rule_id):
    with fees_db() as conn:
        conn.execute("DELETE FROM psp_fee_rules WHERE id = ?", (rule_id,))
