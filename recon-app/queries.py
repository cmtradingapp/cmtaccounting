"""All reconciliation SQL lives here."""

import os
import time
import bisect
import threading
from datetime import datetime, timedelta, timezone
import psycopg2
from db import dealio, crm, fees_db, FEES_MODE, signals

# Per-cache-key locks prevent thundering herd on cold start:
# only ONE thread runs each expensive query while others wait for it to finish.
_CACHE_LOCKS: dict = {}
_CACHE_LOCKS_LOCK = threading.Lock()

def _get_cache_lock(key: str) -> threading.Lock:
    with _CACHE_LOCKS_LOCK:
        if key not in _CACHE_LOCKS:
            _CACHE_LOCKS[key] = threading.Lock()
        return _CACHE_LOCKS[key]

# True when the fees database is PostgreSQL (FEES_MODE=live)
_PG = FEES_MODE == "live"


def _db_retry(fn, max_attempts: int = 4, base_delay: float = 1.0):
    """
    Execute fn() and retry on PostgreSQL hot-standby conflict errors.

    'canceling statement due to conflict with recovery' is a transient error
    on read replicas: the primary runs VACUUM and the replica cancels our query
    to allow WAL replay. Retrying after a short back-off almost always succeeds.
    """
    last_exc = None
    for attempt in range(max_attempts):
        try:
            return fn()
        except psycopg2.Error as exc:
            msg = str(exc)
            code = getattr(exc, 'pgcode', '') or ''
            is_conflict = (
                'conflict with recovery' in msg
                or 'server closed the connection unexpectedly' in msg
                or 'connection to server' in msg and 'failed' in msg
                or 'SSL connection has been closed unexpectedly' in msg
                or code in ('57014', '40001', '08006', '08001', '08004')
            )
            if is_conflict and attempt < max_attempts - 1:
                last_exc = exc
                time.sleep(base_delay * (attempt + 1))   # 1s, 2s, 3s
                continue
            raise
    raise last_exc

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
    """Months that have CRM data, most recent first. Source: Antelope CRM (Azure SQL)."""
    cached = _cache_get("available_months", _TTL_MONTHS)
    if cached is not None:
        return cached

    with _get_cache_lock("available_months"):
        cached = _cache_get("available_months", _TTL_MONTHS)
        if cached is not None:
            return cached
        def _fetch():
            with crm() as cur:
                cur.execute("""
                    SELECT DISTINCT TOP 36 FORMAT(confirmation_time, 'yyyy-MM') AS month
                    FROM report.vtiger_mttransactions
                    WHERE transactionapproval = 'Approved'
                      AND confirmation_time IS NOT NULL
                    ORDER BY month DESC
                """)
                return [r["month"] for r in cur.fetchall()]
        result = _db_retry(_fetch)
        _cache_set("available_months", result)
    return result


def crm_summary(year: int, month: int):
    """Per-login totals from CRM, split into cash vs non-cash."""
    import datetime
    month_start = datetime.date(year, month, 1)
    month_end   = datetime.date(year + 1, 1, 1) if month == 12 else datetime.date(year, month + 1, 1)

    def _fetch_crm():
        with crm() as cur:
            cur.execute("""
                SELECT
                    login,
                    transactiontype,
                    payment_method,
                    transactionapproval,
                    COUNT(*)       AS tx_count,
                    SUM(usdamount) AS total_usd
                FROM report.vtiger_mttransactions
                WHERE confirmation_time >= %s
                  AND confirmation_time <  %s
                  AND transactionapproval = 'Approved'
                  AND (deleted IS NULL OR deleted = 0)
                GROUP BY login, transactiontype, payment_method, transactionapproval
            """, (month_start, month_end))
            return cur.fetchall()

    rows = _db_retry(_fetch_crm)

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


def mt5_summary(year: int, month: int):
    """Per-login net deposit, dispatched by the PLATFORM_SOURCE env flag.

    PLATFORM_SOURCE=dealio (default) -> legacy Dealio replica (dealio.daily_profits).
    PLATFORM_SOURCE=dw               -> MT5 datawarehouse (mt5_deals, via mt5_dw).
    Both return the identical shape {login: {net_usd, groupcurrency, avg_fx}}, so
    reconcile() and every other caller stays source-agnostic.
    """
    if os.environ.get("PLATFORM_SOURCE", "dealio").lower() == "dw":
        import mt5_dw
        return mt5_dw.mt5_summary(year, month)
    return _mt5_summary_dealio(year, month)


def _mt5_summary_dealio(year: int, month: int):
    """Per-login net deposit from dealio daily_profits.
    Uses an explicit date range so TimescaleDB can exclude irrelevant hypertable chunks.
    EXTRACT()-based filtering scans ALL chunks and kills the replica at peak hours.
    """
    import datetime
    month_start = datetime.date(year, month, 1)
    if month == 12:
        month_end = datetime.date(year + 1, 1, 1)
    else:
        month_end = datetime.date(year, month + 1, 1)

    def _fetch_mt5():
        with dealio() as cur:
            cur.execute("""
                SELECT
                    login,
                    SUM(convertednetdeposit) AS net_usd,
                    groupcurrency,
                    AVG(conversionratio)     AS avg_fx
                FROM dealio.daily_profits
                WHERE date >= %s
                  AND date <  %s
                  AND netdeposit != 0
                GROUP BY login, groupcurrency
            """, (month_start, month_end))
            return {r["login"]: dict(r) for r in cur.fetchall()}

    return _db_retry(_fetch_mt5)


def _load_praxis_account_map() -> dict:
    """
    Build {session_cid (str) -> [mt5_login (int), ...]} lookup from Antelope CRM.

    Source: vtiger_mttransactions.vtigeraccountid — this column stores the CRM
    account ID which equals the Praxis session_cid (26xxx/27xxx range).
    vtiger_trading_accounts.vtigeraccountid is a legacy vtiger record ID (~1000xxx)
    that does NOT match Praxis session_cid.
    Cached 30 minutes — the mapping rarely changes.
    """
    key = "praxis_account_map"
    cached = _cache_get(key, 1800)
    if cached is not None:
        return cached

    with _get_cache_lock(key):
        cached = _cache_get(key, 1800)
        if cached is not None:
            return cached
        try:
            def _fetch():
                with crm() as cur:
                    cur.execute("""
                        SELECT DISTINCT login, vtigeraccountid
                        FROM report.vtiger_mttransactions
                        WHERE login IS NOT NULL
                          AND vtigeraccountid IS NOT NULL
                          AND vtigeraccountid > 1000000
                          AND (deleted IS NULL OR deleted = 0)
                    """)
                    m: dict = {}
                    for r in cur.fetchall():
                        cid = str(r["vtigeraccountid"]).strip()
                        lg  = r["login"]
                        if cid and lg:
                            if int(lg) not in m.get(cid, []):
                                m.setdefault(cid, []).append(int(lg))
                    return m
            mapping = _db_retry(_fetch)
        except Exception:
            mapping = {}
        fallback_cids: set = set()
        _cache_set(key, (mapping, fallback_cids))
    return _cache_get(key, 1800) or (mapping, fallback_cids)


_praxis_last_error = None   # surface Praxis connection errors in the UI

def _set_praxis_error(msg):
    global _praxis_last_error
    _praxis_last_error = msg

def get_praxis_error():
    return _praxis_last_error

def praxis_summary(year: int, month: int) -> dict:
    """
    Per-MT5-login Praxis deposit/withdrawal totals for a month. Cached 5 min.

    Join: praxis_transactions.session_cid = vtiger_trading_accounts.vtigeraccountid
          vtiger_trading_accounts.login = MT5 login
    """
    global _praxis_last_error
    import datetime
    key = f"praxis:{year}:{month}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached

    month_start = datetime.date(year, month, 1)
    month_end   = datetime.date(year + 1, 1, 1) if month == 12 else datetime.date(year, month + 1, 1)

    # Load CRM account mapping (session_cid -> [login, ...])
    account_map, _fallback_cids = _load_praxis_account_map()

    try:
        from db import praxis as praxis_ctx
        def _fetch():
            with praxis_ctx() as cur:
                cur.execute("""
                    SELECT
                        session_cid,
                        SUM(CASE WHEN session_intent = 'payment'
                                 THEN usd_amount ELSE 0 END)       AS praxis_deposits,
                        SUM(CASE WHEN session_intent IN ('withdrawal','payout')
                                 THEN usd_amount ELSE 0 END)       AS praxis_withdrawals,
                        COUNT(*)                                    AS praxis_tx_count
                    FROM praxis_transactions
                    WHERE created_timestamp >= EXTRACT(EPOCH FROM %s::timestamp)
                      AND created_timestamp <  EXTRACT(EPOCH FROM %s::timestamp)
                      AND session_cid IS NOT NULL AND session_cid != ''
                    GROUP BY session_cid
                """, (month_start, month_end))
                return cur.fetchall()
        rows = _db_retry(_fetch)
        _set_praxis_error(None)
    except Exception as e:
        _set_praxis_error(str(e))
        print(f"[PRAXIS ERROR] praxis_summary({year}-{month:02d}): {e}")
        rows = []

    # Map each session_cid to MT5 login(s).
    # Only assign when the mapping is unambiguous (exactly one MT5 login per Praxis customer).
    # When a Praxis customer has multiple MT5 accounts we cannot reliably attribute the
    # deposit to the correct account without a transaction-level reference — skip those.
    result: dict = {}
    for r in rows:
        cid    = str(r["session_cid"]).strip()
        logins = account_map.get(cid, [])
        if len(logins) != 1:
            continue   # 0 = no match, >1 = ambiguous — skip both
        login = logins[0]
        if login not in result:
            result[login] = {"praxis_deposits": 0.0, "praxis_withdrawals": 0.0, "praxis_tx_count": 0}
        result[login]["praxis_deposits"]    += float(r["praxis_deposits"] or 0)
        result[login]["praxis_withdrawals"] += float(r["praxis_withdrawals"] or 0)
        result[login]["praxis_tx_count"]    += int(r["praxis_tx_count"] or 0)

    _cache_set(key, result)
    return result


def _load_fee_calc_context():
    """
    Build the four data structures needed to compute expected PSP fees.
    Shared by fee_calculator() and crm_expected_fees().
    All underlying queries are cached, so this is cheap to call repeatedly.
    Returns (rules_by_id, fee_rules_by_psp, saved_mappings, method_mappings_db).
    """
    all_agreements = get_all_agreements()
    rules_by_id: dict = {}
    fee_rules_by_psp: dict = {}
    for agr in all_agreements:
        rules = get_fee_rules(agr["id"])
        rules_by_id[agr["id"]] = {"rules": rules, "psp_name": agr["psp_name"]}
        fee_rules_by_psp[agr["psp_name"].lower()] = {"rules": rules, "psp_name": agr["psp_name"]}
    saved_mappings      = get_processor_mappings()
    method_mappings_db  = get_method_mappings()
    return rules_by_id, fee_rules_by_psp, saved_mappings, method_mappings_db


_PM_DEFAULTS_GLOBAL = {
    "mobileafrica": "Mobile Money", "mobilemoney": "Mobile Money",
    "mobilemoney_checkout": "Mobile Money", "altmobilemoney": "Mobile Money",
    "tingg": "Mobile Money", "payunit": "Mobile Money",
    "altbankonline": "Electronic Payment", "altcrypto": "Crypto", "crypto": "Crypto",
    "ozow": "Electronic Payment", "zpay": "Electronic Payment", "dusupay": "Mobile Money",
    "credit card": "Credit Cards", "creditcard": "Credit Cards",
    "virtualpay": "Electronic Payment", "paywall": "Electronic Payment",
    "bank transfer": "Bank Wire", "wire": "Bank Wire",
    # CRM-specific method names
    "external": "Electronic Payment", "wire": "Bank Wire", "creditcard": "Credit Cards",
    "electronicpayment": "Electronic Payment", "cryptowallet": "Crypto",
}


def _compute_tx_fee(usd, processor, payment_method, fee_type,
                    rules_by_id, fee_rules_by_psp, saved_mappings, method_mappings_db):
    """
    Return expected fee for one transaction given processor + payment_method.

    Lookup priority:
    1. Exact processor name  → processor_mappings → agreement → fee rules
    2. Base processor name   → strip ' - suffix' and retry
    3. Fuzzy PSP name match  → fee_rules_by_psp keys vs processor base name
    """
    if not usd:
        return 0.0

    usd = float(usd)
    pm_raw  = (payment_method or "").lower().strip()
    saved_m = method_mappings_db.get(pm_raw)
    pm_norm = (saved_m["canonical"] if saved_m else _PM_DEFAULTS_GLOBAL.get(pm_raw, pm_raw)).lower()

    # 1 + 2: processor name lookup
    matched_psp = None
    proc = (processor or "").strip()
    for candidate in [proc, proc.split(' - ')[0].strip()]:
        saved = saved_mappings.get(candidate)
        if saved and saved.get("agreement_id"):
            matched_psp = rules_by_id.get(saved["agreement_id"])
            if matched_psp:
                break

    # 3: fuzzy PSP name match against processor base name
    if not matched_psp:
        base = proc.split(' - ')[0].strip().lower()
        for psp_key, psp_data in fee_rules_by_psp.items():
            if base and (psp_key in base or base in psp_key):
                matched_psp = psp_data
                break

    if not matched_psp:
        return 0.0

    # Find best rule for this payment method + fee type
    best = None
    for rule in matched_psp["rules"]:
        if rule.get("fee_type") != fee_type:
            continue
        rule_pm = (rule.get("payment_method") or "").lower()
        if rule_pm and rule_pm not in pm_norm and pm_norm not in rule_pm:
            continue
        if best is None or rule_pm:   # prefer more specific match
            best = rule
    if not best:
        return 0.0

    kind = best.get("fee_kind", "percentage")
    pct  = float(best.get("pct_rate") or 0)
    fix  = float(best.get("fixed_amount") or 0)
    if kind == "percentage":      return round(usd * pct, 4)
    if kind == "fixed":           return round(fix, 4)
    if kind == "fixed_plus_pct":  return round(fix + usd * pct, 4)
    if kind == "tiered":
        for tier in sorted(best.get("tiers", []), key=lambda t: t.get("volume_from", 0), reverse=True):
            if usd >= tier.get("volume_from", 0):
                return round(usd * float(tier.get("pct_rate", 0)), 4)
    return 0.0


def crm_expected_fees(year: int, month: int) -> dict:
    """
    For each login, compute the total expected PSP fee for approved deposits
    in the given month using fee rules from the fees database.

    Returns {login: expected_fee_usd}.

    This bridges the gap between MT5 net deposit (post-fee) and CRM gross deposit
    (pre-fee), allowing the reconciliation to compute a fee-adjusted difference.
    """
    import datetime
    month_start = datetime.date(year, month, 1)
    month_end   = datetime.date(year + 1, 1, 1) if month == 12 else datetime.date(year, month + 1, 1)

    key = f"crm_fees:{year}:{month}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached

    # Load fee calc context (all cached)
    rules_by_id, fee_rules_by_psp, saved_mappings, method_mappings_db = _load_fee_calc_context()

    # Fetch individual deposit transactions from CRM (need per-tx amounts + processor)
    def _fetch_txns():
        with crm() as cur:
            cur.execute("""
                SELECT login, payment_method, payment_processor, usdamount
                FROM report.vtiger_mttransactions
                WHERE confirmation_time >= %s AND confirmation_time < %s
                  AND transactionapproval = 'Approved'
                  AND transactiontype IN ('Deposit', 'TransferIn')
                  AND (deleted IS NULL OR deleted = 0)
                  AND login IS NOT NULL
            """, (month_start, month_end))
            return cur.fetchall()

    try:
        txns = _db_retry(_fetch_txns)
    except Exception:
        txns = []

    fees_by_login: dict = {}
    for r in txns:
        login = r["login"]
        fee   = _compute_tx_fee(
            r["usdamount"], r["payment_processor"], r["payment_method"], "Deposit",
            rules_by_id, fee_rules_by_psp, saved_mappings, method_mappings_db,
        )
        if fee > 0:
            fees_by_login[login] = round(fees_by_login.get(login, 0.0) + fee, 4)

    _cache_set(key, fees_by_login)
    return fees_by_login


def reconcile(year: int, month: int):
    """Join MT5 netdeposit vs CRM cash transactions per login (cached 5 min)."""
    key = f"reconcile:{year}:{month}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached

    crm    = crm_summary(year, month)
    mt5    = mt5_summary(year, month)
    praxis = praxis_summary(year, month)
    fees   = crm_expected_fees(year, month)   # {login: expected_fee_usd}
    bank   = bank_recon_summary(year, month)  # {login: {bank_deposits, bank_withdrawals, ...}}

    rows = []
    for login in set(crm) | set(mt5):
        c = crm.get(login, {})
        m = mt5.get(login, {})

        mt5_net      = round(float(m.get("net_usd") or 0), 2)
        crm_cash     = round(c.get("cash_net", 0), 2)
        expected_fee = round(fees.get(login, 0.0), 2)
        diff         = round(mt5_net - crm_cash, 2)
        # Fee adj is informational only — company pays fees so they don't affect matching
        fee_adj_diff = round(diff - expected_fee, 2)
        # Full adj: MT5 vs (cash + internal credits/debits from bonuses etc.)
        # Bonuses are credited to MT5 by the company; they show as noncash in CRM
        noncash_net = round(c.get("noncash_in", 0) - c.get("noncash_out", 0), 2)
        adj_diff    = round(diff - noncash_net, 2)

        if login not in crm:
            status = "mt5_only"
        elif login not in mt5:
            status = "crm_only"
        elif abs(adj_diff) < 1.0:
            status = "matched"
        else:
            status = "discrepancy"

        p = praxis.get(login, {})
        praxis_net = round(
            p.get("praxis_deposits", 0) - p.get("praxis_withdrawals", 0), 2
        )
        bk = bank.get(login, {})
        rows.append({
            "login":              login,
            "mt5_net":            mt5_net,
            "crm_cash_net":       crm_cash,
            "crm_cash_dep":       round(c.get("cash_deposits", 0), 2),
            "crm_cash_with":      round(c.get("cash_withdrawals", 0), 2),
            "crm_noncash_in":     round(c.get("noncash_in", 0), 2),
            "crm_noncash_out":    round(c.get("noncash_out", 0), 2),
            "difference":         diff,
            "abs_diff":           abs(diff),
            "expected_fees":      expected_fee,
            "fee_adj_diff":       fee_adj_diff,
            "status":             status,
            "payment_methods":    c.get("payment_methods", ""),
            "tx_count":           c.get("tx_count", 0),
            "currency":           m.get("groupcurrency", "USD"),
            "praxis_net":         praxis_net,
            "praxis_deposits":    round(p.get("praxis_deposits", 0), 2),
            "praxis_withdrawals": round(p.get("praxis_withdrawals", 0), 2),
            "praxis_tx_count":    p.get("praxis_tx_count", 0),
            "bank_net":           round(bk.get("bank_net", 0), 2),
            "bank_deposits":      round(bk.get("bank_deposits", 0), 2),
            "bank_withdrawals":   round(bk.get("bank_withdrawals", 0), 2),
            "bank_matched":       bk.get("bank_matched", 0),
        })

    rows.sort(key=lambda r: r["abs_diff"], reverse=True)

    # Add cash_methods: only the subset of payment_methods that are cash-relevant
    for r in rows:
        raw = [m.strip() for m in (r.get("payment_methods") or "").split(",") if m.strip()]
        cash_only = [m for m in raw if m in CASH_METHODS]
        r["cash_methods"] = ", ".join(cash_only) if cash_only else None

    _cache_set(key, rows)
    return rows


def _load_trading_account_fallback() -> tuple:
    """
    Secondary CID mapping from vtiger_trading_accounts.
    Cached independently (30 min) so slowness here never blocks other hot paths.
    Returns (mapping: {cid: [login, ...]}, fallback_cids: set).
    Only used in reconcile_grouped() for display grouping — NOT for Praxis matching.
    """
    key = "trading_account_fallback"
    cached = _cache_get(key, 1800)
    if cached is not None:
        return cached

    with _get_cache_lock(key):
        cached = _cache_get(key, 1800)
        if cached is not None:
            return cached
        try:
            def _fetch():
                with crm() as cur:
                    cur.execute("""
                        SELECT login, CAST(vtigeraccountid AS VARCHAR(20)) AS cid
                        FROM report.vtiger_trading_accounts
                        WHERE login IS NOT NULL
                          AND vtigeraccountid IS NOT NULL
                          AND (deleted IS NULL OR deleted = 0)
                    """)
                    m: dict = {}
                    cids: set = set()
                    for r in cur.fetchall():
                        cid = (r["cid"] or "").strip()
                        lg  = r["login"]
                        if cid and lg:
                            if int(lg) not in m.get(cid, []):
                                m.setdefault(cid, []).append(int(lg))
                            cids.add(cid)
                    return m, cids
            result = _db_retry(_fetch)
        except Exception:
            result = ({}, set())
        _cache_set(key, result)
    return _cache_get(key, 1800) or result


def reconcile_grouped(year: int, month: int) -> list:
    """
    Group reconcile() rows by Praxis CID (vtigeraccountid).

    Returns a list of group dicts, sorted by aggregate abs_diff descending:
    {
        "cid":      str,   # Praxis CID, or "" if unmapped
        "logins":   [...], # list of individual reconcile row dicts
        "agg":      {...}, # aggregated totals across all logins in the group
        "expanded": bool,  # True when any login has a non-matched status
    }

    Single-login groups are flagged with multi=False so the template
    can render them as plain rows without a toggle.
    """
    rows        = reconcile(year, month)   # cached
    account_map, fallback_cids = _load_praxis_account_map()   # primary: vtiger_mttransactions
    fb_map, fb_cids = _load_trading_account_fallback()        # secondary: vtiger_trading_accounts

    # Build login→cid: primary wins, fallback fills gaps
    login_to_cid: dict = {}
    for cid, logins in account_map.items():
        for login in logins:
            if login not in login_to_cid:
                login_to_cid[login] = cid
    primary_logins = set(login_to_cid)
    for cid, logins in fb_map.items():
        for login in logins:
            if login not in primary_logins:
                login_to_cid[login] = cid
    fallback_cids = fb_cids  # cids that came from trading_accounts fallback

    STATUS_PRIORITY = {"discrepancy": 3, "crm_only": 2, "mt5_only": 2, "matched": 1}

    groups_dict: dict = {}
    for r in rows:
        cid = login_to_cid.get(r["login"], "")
        key = cid if cid else f"__login_{r['login']}"
        if key not in groups_dict:
            groups_dict[key] = {"cid": cid, "logins": []}
        groups_dict[key]["logins"].append({**r, "cid": cid})

    groups = []
    for g in groups_dict.values():
        logins = g["logins"]
        agg = {
            "mt5_net":        round(sum(r["mt5_net"]        for r in logins), 2),
            "crm_cash_dep":   round(sum(r["crm_cash_dep"]   for r in logins), 2),
            "crm_cash_with":  round(sum(r["crm_cash_with"]  for r in logins), 2),
            "crm_cash_net":   round(sum(r["crm_cash_net"]   for r in logins), 2),
            "crm_noncash_in": round(sum(r["crm_noncash_in"] for r in logins), 2),
            "crm_noncash_out":round(sum(r["crm_noncash_out"]for r in logins), 2),
            "difference":     round(sum(r["difference"]     for r in logins), 2),
            "expected_fees":  round(sum(r["expected_fees"]  for r in logins), 2),
            "praxis_net":     round(sum(r["praxis_net"]     for r in logins), 2),
            "praxis_deposits":round(sum(r["praxis_deposits"]for r in logins), 2),
            "praxis_withdrawals":round(sum(r["praxis_withdrawals"]for r in logins),2),
            "praxis_tx_count":sum(r["praxis_tx_count"]      for r in logins),
            "bank_net":       round(sum(r.get("bank_net",0) for r in logins), 2),
            "bank_deposits":  round(sum(r.get("bank_deposits",0) for r in logins), 2),
            "bank_withdrawals":round(sum(r.get("bank_withdrawals",0) for r in logins), 2),
            "bank_matched":   sum(r.get("bank_matched",0)   for r in logins),
            "tx_count":       sum(r["tx_count"]             for r in logins),
            "currency":       logins[0]["currency"],
        }
        agg["abs_diff"] = abs(agg["difference"])
        agg["fee_adj_diff"] = round(agg["difference"] - agg["expected_fees"], 2)
        agg["status"] = max(
            (r["status"] for r in logins),
            key=lambda s: STATUS_PRIORITY.get(s, 0)
        )
        # Collect unique cash payment methods across all logins
        methods = set()
        for r in logins:
            for m in (r.get("cash_methods") or "").split(","):
                m = m.strip()
                if m:
                    methods.add(m)
        agg["cash_methods"] = ", ".join(sorted(methods)) or None

        groups.append({
            "cid":      g["cid"],
            "logins":   logins,
            "agg":      agg,
            "multi":    len(logins) > 1,
            "expanded": agg["status"] != "matched",
            "cid_fallback": g["cid"] in fallback_cids,
        })

    # Batch-fetch client names for all known CIDs (single round-trip to CRM)
    cid_names: dict = {}
    numeric_cids = [g["cid"] for g in groups if g["cid"]]
    if numeric_cids:
        try:
            def _fetch_names():
                with crm() as cur:
                    placeholders = ",".join(["%s"] * len(numeric_cids))
                    cur.execute(
                        f"SELECT CAST(accountid AS VARCHAR(20)) AS cid,"
                        f" RTRIM(first_name + ' ' + last_name) AS name"
                        f" FROM report.vtiger_account"
                        f" WHERE accountid IN ({placeholders})",
                        [int(c) for c in numeric_cids]
                    )
                    return {(r["cid"] or "").strip(): (r["name"] or "").strip()
                            for r in cur.fetchall()}
            cid_names = _db_retry(_fetch_names)
        except Exception:
            cid_names = {}

    for g in groups:
        g["name"] = cid_names.get(g["cid"], "")

    groups.sort(key=lambda g: g["agg"]["abs_diff"], reverse=True)
    return groups


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


# (bucket_seconds, lookback_interval) for each chart period.
# Uses epoch-division bucketing — pure PostgreSQL, no TimescaleDB extension needed.
_FX_OHLC_PARAMS = {
    '5m':  (300,    '8 hours'),
    '15m': (900,    '24 hours'),
    '1h':  (3600,   '5 days'),
    '4h':  (14400,  '3 weeks'),
    '1d':  (86400,  '90 days'),
    '1w':  (86400,  '180 days'),   # daily candles — data only from Sept 2025
    '1mo': (86400,  '365 days'),
}


def get_fx_ohlc(symbol: str, period: str = '1d') -> list:
    """OHLC candlestick data using epoch-division bucketing. Cached 5 min."""
    bucket_secs, lookback = _FX_OHLC_PARAMS.get(period, (3600, '5 days'))
    key = f"fx_ohlc:{symbol}:{period}"
    cached = _cache_get(key, _TTL_FX_HISTORY)
    if cached is not None:
        return cached

    with dealio() as cur:
        cur.execute("""
            SELECT
                TO_TIMESTAMP(
                    FLOOR(EXTRACT(EPOCH FROM lastmodified) / %(b)s) * %(b)s
                ) AS ts,
                ROUND(((array_agg((bid+ask) ORDER BY lastmodified))[1] / 2.0)::numeric, 5)       AS o,
                ROUND((MAX(bid+ask) / 2.0)::numeric, 5)                                           AS h,
                ROUND((MIN(bid+ask) / 2.0)::numeric, 5)                                           AS l,
                ROUND(((array_agg((bid+ask) ORDER BY lastmodified DESC))[1] / 2.0)::numeric, 5)  AS c
            FROM dealio.ticks
            WHERE symbol = %(sym)s
              AND lastmodified >= NOW() - %(lb)s::interval
            GROUP BY 1
            ORDER BY 1
        """, {"b": bucket_secs, "sym": symbol, "lb": lookback})
        rows = [
            {"ts": r["ts"].isoformat(),
             "o": float(r["o"]), "h": float(r["h"]),
             "l": float(r["l"]), "c": float(r["c"])}
            for r in cur.fetchall()
        ]

    _cache_set(key, rows)
    return rows


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


_TTL_CLIENT_LIST       = 3600    # 1 hour — expensive cross-DB aggregate (most spans)
_TTL_CLIENT_LIST_WIDE  = 21600   # 6 hours — "all" / 2Y spans (slow, changes slowly)

# Stale-while-revalidate store — holds the last successful result even after TTL expires.
# Eliminates 524 timeouts: stale data is returned instantly while background refreshes.
_CLIENT_LIST_STALE: dict = {}
_CLIENT_LIST_REFRESHING: set = set()
_CLIENT_LIST_PROGRESS: dict = {}   # {key: {"stage": str, "started": float}}


_TTL_FEE_CALC = 300   # 5 minutes


def fee_calculator(date_from=None, date_to=None) -> dict:
    """
    Compare actual Praxis fees vs expected fees from psp_fee_rules.

    Returns:
      {
        "by_psp": [
          {
            "psp_name":        str,    # matched agreement psp_name
            "payment_processor": str,  # raw Praxis processor string
            "tx_count":        int,
            "gross_volume":    float,  # sum usd_amount
            "actual_fees":     float,  # sum fee/100
            "expected_fees":   float,  # computed from fee rules
            "variance":        float,  # actual - expected
            "match_rate":      float,  # % of txs with a matching rule
          }
        ],
        "by_method": [ same shape, grouped by payment_method ],
        "totals": { gross_volume, actual_fees, expected_fees, variance },
        "unmatched_processors": [ list of processor strings with no fee rule ],
        "date_from": str,
        "date_to":   str,
      }
    """
    import datetime as _dt
    if date_from is None: date_from = _dt.date.today() - _dt.timedelta(days=31)
    if date_to   is None: date_to   = _dt.date.today() + _dt.timedelta(days=1)

    key = f"fee_calc:{date_from}:{date_to}"
    cached = _cache_get(key, _TTL_FEE_CALC)
    if cached is not None:
        return cached

    # Load fee rules and mappings (shared helper, all results cached)
    rules_by_id, fee_rules_by_psp, saved_mappings, _ = _load_fee_calc_context()

    # Pull Praxis transactions for the period
    try:
        from db import praxis as praxis_ctx
        def _fetch():
            with praxis_ctx() as cur:
                cur.execute("""
                    SELECT
                        payment_processor,
                        payment_method,
                        session_intent   AS direction,
                        usd_amount,
                        fee / 100.0      AS fee_actual,
                        currency
                    FROM praxis_transactions
                    WHERE created_timestamp >= EXTRACT(EPOCH FROM %s::timestamp)
                      AND created_timestamp <  EXTRACT(EPOCH FROM %s::timestamp)
                      AND session_intent IN ('payment','withdrawal','payout')
                    ORDER BY payment_processor, payment_method
                """, (date_from, date_to))
                return cur.fetchall()
        rows = _db_retry(_fetch)
    except Exception:
        rows = []

    # Load method mappings from DB (user-managed), with sensible defaults as fallback
    _method_mappings_db = get_method_mappings()   # {praxis_method: {canonical, confirmed}}
    _PM_DEFAULTS = {
        "mobileafrica":        "Mobile Money",
        "mobilemoney":         "Mobile Money",
        "mobilemoney_checkout":"Mobile Money",
        "altmobilemoney":      "Mobile Money",
        "tingg":               "Mobile Money",
        "payunit":             "Mobile Money",
        "altbankonline":       "Electronic Payment",
        "altcrypto":           "Crypto",
        "crypto":              "Crypto",
        "ozow":                "Electronic Payment",
        "zpay":                "Electronic Payment",
        "dusupay":             "Mobile Money",
        "credit card":         "Credit Cards",
        "creditcard":          "Credit Cards",
        "virtualpay":          "Electronic Payment",
        "paywall":             "Electronic Payment",
        "bank transfer":       "Bank Wire",
        "wire":                "Bank Wire",
    }
    def _translate_method(pm_raw):
        # DB mapping takes priority over defaults
        saved = _method_mappings_db.get(pm_raw)
        if saved:
            return saved["canonical"]
        return _PM_DEFAULTS.get(pm_raw, pm_raw)   # fall back to raw name

    # Helper — look up expected fee using saved mappings first, fuzzy fallback
    def _calc_expected(usd_amount, payment_method, direction, processor):
        if not usd_amount:
            return 0.0, False
        fee_type = "Deposit" if direction == "payment" else "Withdrawal"
        # Translate Praxis method name to canonical form used in rules
        pm_raw   = (payment_method or "").lower().strip()
        pm_canon = _translate_method(pm_raw)
        pm_norm  = pm_canon.lower()
        proc_key = (processor or "")

        # 1. Check saved manual/AI mapping first (highest priority)
        matched_psp = None
        saved = saved_mappings.get(proc_key)
        if saved and saved.get("agreement_id"):
            matched_psp = rules_by_id.get(saved["agreement_id"])

        # 2. Fall back to fuzzy name match
        if not matched_psp:
            proc_l = proc_key.lower()
            for psp_key, psp_data in fee_rules_by_psp.items():
                if psp_key in proc_l or proc_l in psp_key:
                    matched_psp = psp_data
                    break

        if not matched_psp:
            return 0.0, False

        # Match rules: try exact canonical match first, then "any method" (null) rules
        best = None
        for rule in matched_psp["rules"]:
            if rule.get("fee_type") != fee_type:
                continue
            rule_pm = (rule.get("payment_method") or "").lower()
            if rule_pm and rule_pm not in pm_norm and pm_norm not in rule_pm:
                continue   # method specified but doesn't match
            if best is None or rule_pm:  # prefer more specific match
                best = rule

        if not best:
            return 0.0, False

        kind = best.get("fee_kind", "percentage")
        pct  = float(best.get("pct_rate") or 0)
        fix  = float(best.get("fixed_amount") or 0)
        usd  = float(usd_amount or 0)

        if kind == "percentage":      fee = round(usd * pct, 4)
        elif kind == "fixed":         fee = round(fix, 4)
        elif kind == "fixed_plus_pct":fee = round(fix + usd * pct, 4)
        elif kind == "tiered":
            tiers = sorted(best.get("tiers", []), key=lambda t: t.get("volume_from", 0))
            fee = 0.0
            for tier in reversed(tiers):
                if usd >= tier.get("volume_from", 0):
                    fee = round(usd * float(tier.get("pct_rate", 0)), 4)
                    break
        else:
            fee = 0.0
        return fee, True

    # Aggregate by processor
    from collections import defaultdict
    by_psp: dict     = defaultdict(lambda: {"tx_count":0,"gross":0.0,"actual":0.0,"expected":0.0,"matched":0})
    by_method: dict  = defaultdict(lambda: {"tx_count":0,"gross":0.0,"actual":0.0,"expected":0.0,"matched":0})
    unmatched: set   = set()

    for r in rows:
        proc  = (r["payment_processor"] or "unknown")
        meth  = (r["payment_method"]    or "unknown")
        usd   = float(r["usd_amount"]   or 0)
        act   = float(r["fee_actual"]   or 0)
        dirn  = r["direction"]

        exp, hit = _calc_expected(usd, meth, dirn, proc)
        if not hit and proc not in saved_mappings:
            unmatched.add(proc)

        for bucket, key2 in [(by_psp, proc), (by_method, meth)]:
            bucket[key2]["tx_count"]  += 1
            bucket[key2]["gross"]     += usd
            bucket[key2]["actual"]    += act
            bucket[key2]["expected"]  += exp
            if hit:
                bucket[key2]["matched"] += 1

    def _to_rows(d):
        result = []
        for label, v in d.items():
            tc = v["tx_count"]
            result.append({
                "label":        label,
                "tx_count":     tc,
                "gross_volume": round(v["gross"],    2),
                "actual_fees":  round(v["actual"],   2),
                "expected_fees":round(v["expected"], 2),
                "variance":     round(v["actual"] - v["expected"], 2),
                "match_rate":   round(v["matched"] / tc * 100, 1) if tc else 0.0,
            })
        result.sort(key=lambda r: abs(r["variance"]), reverse=True)
        return result

    by_psp_rows    = _to_rows(by_psp)
    by_method_rows = _to_rows(by_method)

    total_gross  = sum(r["gross_volume"]  for r in by_psp_rows)
    total_actual = sum(r["actual_fees"]   for r in by_psp_rows)
    total_exp    = sum(r["expected_fees"] for r in by_psp_rows)

    result = {
        "by_psp":               by_psp_rows,
        "by_method":            by_method_rows,
        "totals": {
            "gross_volume":  round(total_gross,  2),
            "actual_fees":   round(total_actual, 2),
            "expected_fees": round(total_exp,    2),
            "variance":      round(total_actual - total_exp, 2),
        },
        "unmatched_processors": sorted(unmatched),
        "date_from":  str(date_from),
        "date_to":    str(date_to),
    }
    _cache_set(key, result)
    return result


def fee_uncovered_transactions(processor: str, date_from=None, date_to=None) -> list:
    """
    Return individual Praxis transactions for `processor` that have no matching fee rule.
    Each item: {tid, ts, direction, payment_method, canonical_method, usd_amount,
                fee_actual, why}  where why is 'no_mapping' or 'no_rule'.
    Grouped by canonical_method + direction for easy display.
    """
    import datetime as _dt
    if date_from is None: date_from = _dt.date.today() - _dt.timedelta(days=31)
    if date_to   is None: date_to   = _dt.date.today() + _dt.timedelta(days=1)

    # ── Same setup as fee_calculator (all cached) ─────────────────────────
    all_agreements = get_all_agreements()
    rules_by_id: dict = {}
    fee_rules_by_psp: dict = {}
    for agr in all_agreements:
        rules = get_fee_rules(agr["id"])
        rules_by_id[agr["id"]] = {"rules": rules, "psp_name": agr["psp_name"]}
        fee_rules_by_psp[agr["psp_name"].lower()] = {"rules": rules, "psp_name": agr["psp_name"]}

    saved_mappings      = get_processor_mappings()
    _method_mappings_db = get_method_mappings()
    _PM_DEFAULTS = {
        "mobileafrica":"Mobile Money","mobilemoney":"Mobile Money",
        "mobilemoney_checkout":"Mobile Money","altmobilemoney":"Mobile Money",
        "tingg":"Mobile Money","payunit":"Mobile Money",
        "altbankonline":"Electronic Payment","altcrypto":"Crypto","crypto":"Crypto",
        "ozow":"Electronic Payment","zpay":"Electronic Payment","dusupay":"Mobile Money",
        "credit card":"Credit Cards","creditcard":"Credit Cards",
        "virtualpay":"Electronic Payment","paywall":"Electronic Payment",
        "bank transfer":"Bank Wire","wire":"Bank Wire",
    }
    def _translate(pm_raw):
        s = _method_mappings_db.get(pm_raw)
        if s: return s["canonical"]
        return _PM_DEFAULTS.get(pm_raw, pm_raw)

    def _resolve_psp(proc_key):
        """Return (matched_psp_data, why_prefix) for a processor."""
        saved = saved_mappings.get(proc_key)
        if saved and saved.get("agreement_id"):
            return rules_by_id.get(saved["agreement_id"]), "no_rule"
        proc_l = proc_key.lower()
        for psp_key, psp_data in fee_rules_by_psp.items():
            if psp_key in proc_l or proc_l in psp_key:
                return psp_data, "no_rule"
        return None, "no_mapping"

    # ── Fetch individual transactions for this processor ──────────────────
    try:
        from db import praxis as praxis_ctx
        def _fetch():
            with praxis_ctx() as cur:
                cur.execute("""
                    SELECT
                        tid,
                        TO_TIMESTAMP(created_timestamp) AS ts,
                        payment_method,
                        session_intent   AS direction,
                        usd_amount,
                        fee / 100.0      AS fee_actual
                    FROM praxis_transactions
                    WHERE created_timestamp >= EXTRACT(EPOCH FROM %s::timestamp)
                      AND created_timestamp <  EXTRACT(EPOCH FROM %s::timestamp)
                      AND payment_processor  = %s
                      AND session_intent IN ('payment','withdrawal','payout')
                    ORDER BY created_timestamp DESC
                """, (date_from, date_to, processor))
                return cur.fetchall()
        rows = _db_retry(_fetch)
    except Exception:
        rows = []

    matched_psp, default_why = _resolve_psp(processor)

    uncovered = []
    for r in rows:
        pm_raw   = (r["payment_method"] or "").lower().strip()
        pm_canon = _translate(pm_raw)
        pm_norm  = pm_canon.lower()
        dirn     = r["direction"]
        fee_type = "Deposit" if dirn == "payment" else "Withdrawal"
        usd      = float(r["usd_amount"] or 0)

        if matched_psp is None:
            why = "no_mapping"
        else:
            # Check if any rule matches this method + direction
            hit = False
            for rule in matched_psp["rules"]:
                if rule.get("fee_type") != fee_type:
                    continue
                rule_pm = (rule.get("payment_method") or "").lower()
                if rule_pm and rule_pm not in pm_norm and pm_norm not in rule_pm:
                    continue
                hit = True
                break
            if hit:
                continue   # covered — skip
            why = "no_rule"

        uncovered.append({
            "tid":              r["tid"],
            "ts":               str(r["ts"])[:16] if r["ts"] else None,
            "direction":        fee_type,
            "payment_method":   r["payment_method"] or "unknown",
            "canonical_method": pm_canon,
            "usd_amount":       round(usd, 2),
            "fee_actual":       round(float(r["fee_actual"] or 0), 2),
            "why":              why,
        })

    return uncovered


def equity_report(date_from=None, date_to=None) -> list:
    """
    Per-CID equity statement for a date range. Cached 1 hour.
    Returns one row per CID (or login) with:
      open_balance, open_equity, deposits, withdrawals,
      realised_pnl, close_balance, close_equity — all USD.
    """
    import datetime as _dt
    if date_from is None: date_from = _dt.date(2021, 1, 1)
    if date_to   is None: date_to   = _dt.date.today() + _dt.timedelta(days=1)

    key = f"equity_report:{date_from}:{date_to}"
    cached = _cache_get(key, _TTL_CLIENT_LIST)
    if cached is not None:
        return cached

    import concurrent.futures as _cf
    span_days = (date_to - date_from).days
    stmt_timeout = 300000 if span_days > 365 else 120000

    # Opening snapshot (first day per login in range)
    def _fetch_open():
        with dealio() as cur:
            cur.execute(f"SET statement_timeout = {stmt_timeout}")
            cur.execute("""
                SELECT DISTINCT ON (login)
                    login,
                    balance   AS open_balance,
                    equity    AS open_equity,
                    MAX(groupcurrency) OVER (PARTITION BY login) AS currency
                FROM dealio.daily_profits
                WHERE date >= %s AND date < %s
                ORDER BY login, date ASC
            """, (date_from, date_to))
            return {r["login"]: dict(r) for r in cur.fetchall()}

    # Closing snapshot (last day per login in range)
    def _fetch_close():
        with dealio() as cur:
            cur.execute(f"SET statement_timeout = {stmt_timeout}")
            cur.execute("""
                SELECT DISTINCT ON (login)
                    login,
                    balance   AS close_balance,
                    equity    AS close_equity,
                    groupcurrency AS currency,
                    date      AS last_date
                FROM dealio.daily_profits
                WHERE date >= %s AND date < %s
                ORDER BY login, date DESC
            """, (date_from, date_to))
            return {r["login"]: dict(r) for r in cur.fetchall()}

    # Aggregate deposits / withdrawals / P&L
    def _fetch_agg():
        with dealio() as cur:
            cur.execute(f"SET statement_timeout = {stmt_timeout}")
            cur.execute("""
                SELECT
                    login,
                    SUM(CASE WHEN convertednetdeposit > 0 THEN convertednetdeposit ELSE 0 END) AS deposits,
                    SUM(CASE WHEN convertednetdeposit < 0 THEN -convertednetdeposit ELSE 0 END) AS withdrawals,
                    SUM(convertedclosedpnl) AS realised_pnl,
                    MAX(groupcurrency) AS currency,
                    MAX(date) AS last_active
                FROM dealio.daily_profits
                WHERE date >= %s AND date < %s
                GROUP BY login
            """, (date_from, date_to))
            return {r["login"]: dict(r) for r in cur.fetchall()}

    # Run all three dealio queries in parallel (each hits a separate connection)
    with _cf.ThreadPoolExecutor(max_workers=3) as _ex:
        _f_open  = _ex.submit(_db_retry, _fetch_open)
        _f_close = _ex.submit(_db_retry, _fetch_close)
        _f_agg   = _ex.submit(_db_retry, _fetch_agg)
        open_snap  = _f_open.result()
        close_snap = _f_close.result()
        agg        = _f_agg.result()

    # CID + name mapping
    account_map, _fallback_cids = _load_praxis_account_map()  # {cid: [login, ...]}
    login_to_cid = {login: cid for cid, logins in account_map.items() for login in logins}

    # Praxis names
    try:
        from db import praxis as praxis_ctx
        def _fetch_names():
            with praxis_ctx() as cur:
                cur.execute("""
                    SELECT DISTINCT ON (session_cid)
                        session_cid, customer_first_name || ' ' || customer_last_name AS name
                    FROM praxis_transactions WHERE customer_first_name IS NOT NULL
                    ORDER BY session_cid, created_timestamp DESC
                """)
                return {r["session_cid"]: (r["name"] or "").strip() for r in cur.fetchall()}
        praxis_names = _db_retry(_fetch_names)
    except Exception:
        praxis_names = {}

    # CRM name fallback (vtiger_account) — covers clients with no Praxis activity
    try:
        def _fetch_du():
            with crm() as cur:
                cur.execute("""
                    SELECT ta.login,
                           RTRIM(a.first_name + ' ' + a.last_name) AS name,
                           a.email
                    FROM report.vtiger_trading_accounts ta
                    JOIN report.vtiger_account a ON a.accountid = ta.vtigeraccountid
                    WHERE (ta.deleted IS NULL OR ta.deleted = 0)
                      AND ta.login IS NOT NULL
                """)
                return {r["login"]: (r["name"] or "").strip() for r in cur.fetchall()}
        crm_names = _db_retry(_fetch_du)
    except Exception:
        crm_names = {}

    rows = []
    for login, a in agg.items():
        cid   = login_to_cid.get(login, "")
        name  = praxis_names.get(cid) or crm_names.get(login) or ""
        o     = open_snap.get(login,  {})
        c     = close_snap.get(login, {})
        rows.append({
            "login":         login,
            "cid":           cid,
            "name":          name,
            "currency":      a.get("currency") or "USD",
            "last_active":   str(a.get("last_active") or ""),
            "open_balance":  round(float(o.get("open_balance")  or 0), 2),
            "open_equity":   round(float(o.get("open_equity")   or 0), 2),
            "deposits":      round(float(a.get("deposits")      or 0), 2),
            "withdrawals":   round(float(a.get("withdrawals")   or 0), 2),
            "realised_pnl":  round(float(a.get("realised_pnl") or 0), 2),
            "close_balance": round(float(c.get("close_balance") or 0), 2),
            "close_equity":  round(float(c.get("close_equity")  or 0), 2),
        })

    rows.sort(key=lambda r: r["close_equity"], reverse=True)
    _cache_set(key, rows)
    return rows


def is_client_list_computing():
    return bool(_CLIENT_LIST_REFRESHING)


def get_client_list_progress(date_from=None, date_to=None):
    """Return the real computation stage for the given key, or None."""
    if date_from and date_to:
        key = f"client_list:{date_from}:{date_to}"
        return _CLIENT_LIST_PROGRESS.get(key)
    # Return any active progress if no specific key given
    for v in _CLIENT_LIST_PROGRESS.values():
        return v
    return None


def _compute_client_list(date_from, date_to) -> list:
    """Inner worker: runs all DB queries and builds the client list rows.
    Called by client_list() — either synchronously (first load) or from a
    background refresh thread (stale-while-revalidate).
    """
    import datetime as _dt2, concurrent.futures as _cf, time as _time
    span_days = (_dt2.date.today() - date_from).days
    wide_span = span_days >= 270
    _prog_key = f"client_list:{date_from}:{date_to}"
    _stage_n = [0]
    def _set_stage(s, n):
        _stage_n[0] = n
        _CLIENT_LIST_PROGRESS[_prog_key] = {
            "stage": s, "num": n,
            "started": _CLIENT_LIST_PROGRESS.get(_prog_key, {}).get("started", _time.time())
        }
    # Floating P&L: always use a narrow 90-day window — we only want the most
    # recent value, no need to scan years of history (16× speedup vs full range)
    float_from = max(date_from, date_to - _dt2.timedelta(days=90))
    stmt_timeout = 300000 if wide_span else 120000

    _set_stage("Querying MT5 deposits & P&L\u2026", 1)

    def _fetch_mt5():
        with dealio() as cur:
            cur.execute(f"SET statement_timeout = {stmt_timeout}")
            cur.execute("""
                SELECT
                    login,
                    SUM(convertednetdeposit)  AS net_deposit,
                    SUM(convertedclosedpnl)   AS client_realised_pnl,
                    MAX(groupcurrency)        AS currency,
                    MAX(CASE
                        WHEN ABS(COALESCE(netdeposit, 0))   > 0.001
                          OR ABS(COALESCE(closedpnl, 0))    > 0.001
                          OR ABS(COALESCE(floatingpnl, 0))  > 0.001
                        THEN date ELSE NULL
                    END) AS last_active
                FROM dealio.daily_profits
                WHERE date >= %s AND date < %s
                GROUP BY login
            """, (date_from, date_to))
            rows = {r["login"]: dict(r) for r in cur.fetchall()}

            cur.execute(f"SET statement_timeout = {stmt_timeout}")
            cur.execute("""
                SELECT DISTINCT ON (login)
                    login,
                    convertedfloatingpnl AS client_floating_eod
                FROM dealio.daily_profits
                WHERE date >= %s AND date < %s
                ORDER BY login, date DESC
            """, (float_from, date_to))
            for r in cur.fetchall():
                if r["login"] in rows:
                    rows[r["login"]]["client_floating_eod"] = r["client_floating_eod"]
            return rows

    _set_stage("Loading CRM account mappings\u2026", 2)
    # CID mapping (cached — fast)
    account_map, _fallback_cids = _load_praxis_account_map()
    login_to_cid = {}
    all_cids = []
    for cid_str, logins in account_map.items():
        all_cids.append(cid_str)
        for login in logins:
            if login not in login_to_cid:
                login_to_cid[login] = cid_str

    _BATCH = 2000
    def _fetch_crm_names_for(cids):
        result = {}
        if not cids:
            return result
        try:
            with crm() as cur:
                for i in range(0, len(cids), _BATCH):
                    batch = cids[i:i + _BATCH]
                    ph = ",".join(["%s"] * len(batch))
                    cur.execute(
                        f"SELECT CAST(accountid AS VARCHAR(20)) AS cid,"
                        f" RTRIM(first_name + ' ' + last_name) AS name, email"
                        f" FROM report.vtiger_account WHERE accountid IN ({ph})",
                        [int(c) for c in batch]
                    )
                    for r in cur.fetchall():
                        result[(r["cid"] or "").strip()] = {
                            "name": (r["name"] or "").strip(),
                            "email": r["email"] or ""
                        }
        except Exception:
            pass
        return result

    _set_stage("Fetching client names\u2026", 3)
    # Run MT5 and CRM names in parallel (different DBs, both I/O-bound)
    cids_for_names = all_cids if wide_span else []
    with _cf.ThreadPoolExecutor(max_workers=2) as _ex:
        _f_mt5   = _ex.submit(_db_retry, _fetch_mt5)
        _f_names = _ex.submit(_fetch_crm_names_for, cids_for_names)
        try:
            mt5 = _f_mt5.result()
        except Exception as _mt5_err:
            print(f"[client_list] MT5 query FAILED ({date_from}→{date_to}): {_mt5_err}")
            raise   # propagate so caller gets a real error, not silent empty
        crm_names_wide = _f_names.result()

    if wide_span:
        crm_names = crm_names_wide
    else:
        cids_needed = list({login_to_cid[l] for l in mt5 if l in login_to_cid and login_to_cid[l]})
        crm_names = _fetch_crm_names_for(cids_needed)

    praxis_names: dict = {}
    try:
        from db import praxis as praxis_ctx
        def _fetch_praxis_names():
            with praxis_ctx() as cur:
                cur.execute("""
                    SELECT DISTINCT ON (session_cid)
                        session_cid,
                        customer_first_name || ' ' || customer_last_name AS name,
                        wallet_data_email AS email
                    FROM praxis_transactions
                    WHERE customer_first_name IS NOT NULL
                    ORDER BY session_cid, created_timestamp DESC
                """)
                return {r["session_cid"]: {"name": (r["name"] or "").strip(),
                                            "email": r["email"] or ""}
                        for r in cur.fetchall()}
        praxis_names = _db_retry(_fetch_praxis_names)
    except Exception:
        praxis_names = {}

    _set_stage("Building result rows\u2026", 4)
    rows = []
    for login, m in mt5.items():
        net_dep    = float(m["net_deposit"] or 0)
        client_pnl = float(m["client_realised_pnl"] or 0)
        floating   = float(m.get("client_floating_eod") or 0)
        co_trading = round(-client_pnl, 2)
        co_total   = round(co_trading + net_dep - floating, 2)
        cid        = login_to_cid.get(login, "")
        praxis_info= praxis_names.get(cid, {})
        crm_info   = crm_names.get(cid, {})
        rows.append({
            "login":               login,
            "cid":                 cid,
            "name":                praxis_info.get("name") or crm_info.get("name") or "",
            "email":               praxis_info.get("email") or crm_info.get("email") or "",
            "net_deposit":         round(net_dep, 2),
            "client_realised_pnl": round(client_pnl, 2),
            "client_floating_eod": round(floating, 2),
            "company_trading_pnl": co_trading,
            "company_total":       co_total,
            "currency":            m.get("currency") or "USD",
            "last_active":         str(m.get("last_active") or ""),
        })

    rows.sort(key=lambda r: r["company_total"], reverse=True)
    _CLIENT_LIST_PROGRESS.pop(_prog_key, None)   # clear progress on completion
    return rows


def client_list(date_from=None, date_to=None) -> list:
    """Stale-while-revalidate wrapper around _compute_client_list.

    1. Fresh cache hit  → return immediately (normal path)
    2. Stale cache hit  → return old data instantly, refresh in background thread
    3. No cache at all  → compute synchronously (first ever request for this key)

    This eliminates 524 timeouts after the first successful load: every subsequent
    request returns within milliseconds regardless of how long recomputation takes.
    """
    import datetime as _dt
    if date_from is None:
        date_from = _dt.date(2021, 1, 1)
    if date_to is None:
        date_to = _dt.date.today() + _dt.timedelta(days=1)

    key = f"client_list:{date_from}:{date_to}"
    span_days = (_dt.date.today() - date_from).days
    ttl = _TTL_CLIENT_LIST_WIDE if span_days >= 270 else _TTL_CLIENT_LIST

    # 1. Fresh cache
    cached = _cache_get(key, ttl)
    if cached is not None:
        return cached

    # 2. Stale cache — return old data immediately, refresh in background
    stale = _CLIENT_LIST_STALE.get(key)
    if stale is not None:
        if key not in _CLIENT_LIST_REFRESHING:
            _CLIENT_LIST_REFRESHING.add(key)
            def _bg_refresh():
                try:
                    rows = _compute_client_list(date_from, date_to)
                    if rows:
                        _cache_set(key, rows)
                        _CLIENT_LIST_STALE[key] = rows
                        print(f"[client_list] background refresh done: {key} ({len(rows)} rows)")
                except Exception as e:
                    print(f"[client_list] background refresh failed: {key}: {e}")
                finally:
                    _CLIENT_LIST_REFRESHING.discard(key)
            import threading as _thr
            _thr.Thread(target=_bg_refresh, daemon=True).start()
        return stale  # instant response with stale data

    # 3. No cache — for wide spans, compute in background and return empty
    #    (avoids Cloudflare 524 — first load served by startup warmup or retry)
    if span_days >= 270:
        if key not in _CLIENT_LIST_REFRESHING:
            _CLIENT_LIST_REFRESHING.add(key)
            def _bg_first():
                try:
                    rows = _compute_client_list(date_from, date_to)
                    if rows:
                        _cache_set(key, rows)
                        _CLIENT_LIST_STALE[key] = rows
                        print(f"[client_list] first compute done: {key} ({len(rows)} rows)")
                except Exception as e:
                    print(f"[client_list] first compute failed: {key}: {e}")
                finally:
                    _CLIENT_LIST_REFRESHING.discard(key)
            import threading as _thr
            _thr.Thread(target=_bg_first, daemon=True).start()
        return []  # frontend will detect empty + computing flag and auto-retry

    # For narrow spans (≤1 year): compute synchronously — fast enough (<30s)
    rows = _compute_client_list(date_from, date_to)
    if rows:
        _cache_set(key, rows)
        _CLIENT_LIST_STALE[key] = rows
    return rows


def summary_stats(rows):
    matched     = sum(1 for r in rows if r["status"] == "matched")
    discrepancy = sum(1 for r in rows if r["status"] == "discrepancy")
    mt5_only    = sum(1 for r in rows if r["status"] == "mt5_only")
    crm_only    = sum(1 for r in rows if r["status"] == "crm_only")
    total_diff  = sum(r["difference"] for r in rows if r["status"] == "discrepancy")
    return {
        "total":       len(rows),
        "matched":     matched,
        "discrepancy": discrepancy,
        "mt5_only":    mt5_only,
        "crm_only":    crm_only,
        "total_diff":  round(total_diff, 2),
        "match_rate":  round(matched / len(rows) * 100, 1) if rows else 0,
    }


def praxis_client_tree(year: int, month: int,
                       date_from=None, date_to=None) -> list:
    """
    Build a per-Praxis-customer tree.

    By default covers the single month (year, month).
    Pass explicit date_from / date_to (datetime.date) for wider ranges
    such as 3-month, 6-month, 1-year, or all-time views.

    Cached 5 minutes per unique date range.
    """
    import datetime
    if date_from is None:
        date_from = datetime.date(year, month, 1)
    if date_to is None:
        date_to = datetime.date(year + 1, 1, 1) if month == 12 else datetime.date(year, month + 1, 1)

    month_start = date_from
    month_end   = date_to

    key = f"praxis_tree:{date_from}:{date_to}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached

    # 1. Load Praxis transactions for the month
    try:
        from db import praxis as praxis_ctx
        def _fetch_praxis():
            with praxis_ctx() as cur:
                cur.execute("""
                    SELECT
                        session_cid,
                        tid,
                        session_intent      AS direction,
                        usd_amount,
                        amount / 100.0      AS amount_local,
                        currency,
                        payment_method,
                        payment_processor,
                        fee / 100.0         AS fee_actual,
                        wallet_data_email   AS email,
                        customer_first_name AS first_name,
                        customer_last_name  AS last_name,
                        TO_TIMESTAMP(created_timestamp) AS ts
                    FROM praxis_transactions
                    WHERE created_timestamp >= EXTRACT(EPOCH FROM %s::timestamp)
                      AND created_timestamp <  EXTRACT(EPOCH FROM %s::timestamp)
                      AND session_cid IS NOT NULL AND session_cid != ''
                    ORDER BY session_cid, created_timestamp
                """, (month_start, month_end))
                return cur.fetchall()
        praxis_rows = _db_retry(_fetch_praxis)
        _set_praxis_error(None)
    except Exception as e:
        _set_praxis_error(str(e))
        print(f"[PRAXIS ERROR] praxis_client_tree({year}-{month:02d}): {e}")
        praxis_rows = []

    # 2. Group Praxis transactions by session_cid
    from collections import defaultdict
    by_cid: dict = defaultdict(list)
    for r in praxis_rows:
        by_cid[str(r["session_cid"]).strip()].append(dict(r))

    # 3. Account map cid → [login]
    account_map, _fallback_cids = _load_praxis_account_map()

    # 4. CRM cash summary keyed by login
    # For spans >1 month, merge multiple months' summaries
    import calendar as _cal
    crm: dict = {}
    d = datetime.date(month_start.year, month_start.month, 1)
    while d < month_end:
        month_data = crm_summary(d.year, d.month)
        for login, s in month_data.items():
            if login not in crm:
                crm[login] = {"cash_deposits":0,"cash_withdrawals":0,
                              "noncash_in":0,"noncash_out":0,"tx_count":0,"payment_methods":set()}
            crm[login]["cash_deposits"]    += s.get("cash_deposits", 0)
            crm[login]["cash_withdrawals"] += s.get("cash_withdrawals", 0)
            crm[login]["noncash_in"]       += s.get("noncash_in", 0)
            crm[login]["noncash_out"]      += s.get("noncash_out", 0)
            crm[login]["tx_count"]         += s.get("tx_count", 0)
        # advance to next month
        last_day = _cal.monthrange(d.year, d.month)[1]
        d = datetime.date(d.year, d.month, last_day) + datetime.timedelta(days=1)
        d = datetime.date(d.year, d.month, 1)
    # compute cash_net for each login
    for login, s in crm.items():
        s["cash_net"] = round(s["cash_deposits"] - s["cash_withdrawals"], 2)

    # 5. Build tree
    tree = []
    for cid, txs in sorted(by_cid.items()):
        logins     = account_map.get(cid, [])
        ambiguous  = len(logins) > 1
        # Customer info from first tx
        sample     = txs[0]
        name_parts = [sample.get("first_name") or "", sample.get("last_name") or ""]
        name       = " ".join(p for p in name_parts if p).strip() or "—"
        email      = sample.get("email") or "—"

        total_praxis = sum(float(t["usd_amount"] or 0) for t in txs
                          if t["direction"] == "payment")
        total_praxis -= sum(float(t["usd_amount"] or 0) for t in txs
                           if t["direction"] in ("withdrawal", "payout"))

        mt5_accounts = []
        for login in (logins if logins else [None]):
            acct_txs = txs   # when ambiguous, all txs shown under each login
            p_dep  = sum(float(t["usd_amount"] or 0) for t in acct_txs if t["direction"] == "payment")
            p_with = sum(float(t["usd_amount"] or 0) for t in acct_txs
                        if t["direction"] in ("withdrawal", "payout"))
            p_net  = round(p_dep - p_with, 2)

            c_net  = round(crm.get(login or 0, {}).get("cash_net", 0), 2) if login else 0
            diff   = round(p_net - c_net, 2)

            if login is None:
                match = "no_mt5"
            elif abs(diff) < 1.0:
                match = "matched"
            elif c_net == 0 and p_net != 0:
                match = "unmatched_praxis"
            elif p_net == 0 and c_net != 0:
                match = "unmatched_crm"
            else:
                match = "discrepancy"

            tx_list = []
            for t in acct_txs:
                tx_list.append({
                    "tid":       t["tid"],
                    "direction": t["direction"],
                    "usd":       round(float(t["usd_amount"] or 0), 2),
                    "amount_local": round(float(t["amount_local"] or 0), 2),
                    "currency":  t["currency"],
                    "method":    t["payment_method"],
                    "processor": t["payment_processor"],
                    "fee":       round(float(t["fee_actual"] or 0), 2),
                    "ts":        str(t["ts"])[:16] if t["ts"] else "",
                })

            mt5_accounts.append({
                "login":          login,
                "praxis_net":     p_net,
                "praxis_dep":     round(p_dep, 2),
                "praxis_with":    round(p_with, 2),
                "crm_net":        c_net,
                "diff":           diff,
                "match":          match,
                "transactions":   tx_list,
            })

        tree.append({
            "session_cid":   cid,
            "name":          name,
            "email":         email,
            "ambiguous":     ambiguous,
            "total_praxis":  round(total_praxis, 2),
            "mt5_accounts":  mt5_accounts,
            "has_issue":     ambiguous or any(a["match"] not in ("matched",) for a in mt5_accounts),
        })

    # Sort: issues first, then by total amount desc
    tree.sort(key=lambda n: (not n["has_issue"], -abs(n["total_praxis"])))

    _cache_set(key, tree)
    return tree


def equity_by_client(year: int, month: int) -> list:
    """Last balance & equity per login for the month (MRS Export #1). Cached 5 min."""
    import datetime
    key = f"equity:{year}:{month}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached

    if os.environ.get("PLATFORM_SOURCE", "dealio").lower() == "dw":
        import mt5_dw
        result = mt5_dw.equity_by_client(year, month)
        _cache_set(key, result)
        return result

    month_start = datetime.date(year, month, 1)
    month_end   = datetime.date(year + 1, 1, 1) if month == 12 else datetime.date(year, month + 1, 1)

    def _fetch():
        with dealio() as cur:
            cur.execute("""
                SELECT DISTINCT ON (login)
                    login, groupcurrency AS currency,
                    balance, equity, date
                FROM dealio.daily_profits
                WHERE date >= %s AND date < %s
                ORDER BY login, date DESC
            """, (month_start, month_end))
            return [dict(r) for r in cur.fetchall()]

    result = _db_retry(_fetch)
    _cache_set(key, result)
    return result


def crm_transaction_list(year: int, month: int) -> list:
    """Individual CRM transactions for the month (MRS Export #2, CRM side). Cached 5 min."""
    import datetime
    key = f"crm_txlist:{year}:{month}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached

    month_start = datetime.date(year, month, 1)
    month_end   = datetime.date(year + 1, 1, 1) if month == 12 else datetime.date(year, month + 1, 1)

    def _fetch():
        with crm() as cur:
            cur.execute("""
                SELECT
                    login, transactiontype, transactionapproval,
                    payment_method, usdamount,
                    confirmation_time, transactionid
                FROM report.vtiger_mttransactions
                WHERE confirmation_time >= %s
                  AND confirmation_time <  %s
                  AND (deleted IS NULL OR deleted = 0)
                ORDER BY login, confirmation_time
            """, (month_start, month_end))
            return [dict(r) for r in cur.fetchall()]

    result = _db_retry(_fetch)
    _cache_set(key, result)
    return result


def praxis_transaction_list(year: int, month: int) -> list:
    """Individual Praxis transactions for the month (MRS Export #2, Praxis side). Cached 5 min."""
    import datetime
    key = f"praxis_txlist:{year}:{month}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached

    month_start = datetime.date(year, month, 1)
    month_end   = datetime.date(year + 1, 1, 1) if month == 12 else datetime.date(year, month + 1, 1)

    try:
        from db import praxis as praxis_ctx
        def _fetch():
            with praxis_ctx() as cur:
                cur.execute("""
                    SELECT
                        session_cid         AS login,
                        tid,
                        transaction_id,
                        session_order_id,
                        session_intent      AS direction,
                        amount / 100.0      AS amount_local,
                        currency,
                        usd_amount,
                        payment_method,
                        payment_processor,
                        conversion_rate,
                        fee / 100.0         AS fee_actual,
                        wallet_data_email   AS email,
                        customer_first_name,
                        customer_last_name,
                        TO_TIMESTAMP(created_timestamp) AS inserted_at
                    FROM praxis_transactions
                    WHERE created_timestamp >= EXTRACT(EPOCH FROM %s::timestamp)
                      AND created_timestamp <  EXTRACT(EPOCH FROM %s::timestamp)
                      AND session_cid IS NOT NULL AND session_cid != ''
                    ORDER BY session_cid, created_timestamp
                """, (month_start, month_end))
                account_map, _fallback_cids = _load_praxis_account_map()
                rows = []
                for r in cur.fetchall():
                    row = dict(r)
                    row["usd_amount"] = float(row["usd_amount"] or 0)
                    row["fee_actual"] = float(row["fee_actual"] or 0)
                    cid = str(row.get("login") or "").strip()
                    logins = account_map.get(cid, [])
                    row["mt5_login"] = logins[0] if logins else None
                    rows.append(row)
                return rows
        result = _db_retry(_fetch)
    except Exception:
        result = []

    _cache_set(key, result)
    return result


def profitability_by_day(year: int, month: int) -> list:
    """Daily realised + unrealised P&L per login (MRS Export #3). Cached 5 min."""
    import datetime
    key = f"pnl:{year}:{month}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached

    if os.environ.get("PLATFORM_SOURCE", "dealio").lower() == "dw":
        import mt5_dw
        result = mt5_dw.profitability_by_day(year, month)
        _cache_set(key, result)
        return result

    month_start = datetime.date(year, month, 1)
    month_end   = datetime.date(year + 1, 1, 1) if month == 12 else datetime.date(year, month + 1, 1)

    def _fetch():
        with dealio() as cur:
            cur.execute("""
                SELECT
                    login, date,
                    groupcurrency       AS currency,
                    closedpnl           AS realised_pnl,
                    floatingpnl         AS unrealised_pnl_eod,
                    balance, equity
                FROM dealio.daily_profits
                WHERE date >= %s AND date < %s
                ORDER BY login, date
            """, (month_start, month_end))
            return [dict(r) for r in cur.fetchall()]

    result = _db_retry(_fetch)
    _cache_set(key, result)
    return result


def psp_balance_at_month_end(year: int, month: int) -> list:
    """Net position + fee summary per PSP up to month-end (MRS Export #4). Cached 5 min."""
    import datetime
    key = f"pspbal:{year}:{month}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached

    month_end = datetime.date(year + 1, 1, 1) if month == 12 else datetime.date(year, month + 1, 1)

    try:
        from db import praxis as praxis_ctx
        def _fetch():
            with praxis_ctx() as cur:
                cur.execute("""
                    SELECT
                        payment_processor                                         AS psp,
                        currency,
                        SUM(usd_amount)                                           AS gross_volume_usd,
                        SUM(CASE WHEN session_intent = 'payment'
                                 THEN usd_amount ELSE 0 END)                     AS deposits_usd,
                        SUM(CASE WHEN session_intent IN ('withdrawal','payout')
                                 THEN usd_amount ELSE 0 END)                     AS withdrawals_usd,
                        SUM(CASE WHEN session_intent = 'payment'
                                 THEN usd_amount ELSE -usd_amount END)           AS net_usd,
                        SUM(fee / 100.0)                                         AS actual_fees_usd,
                        COUNT(*)                                                  AS tx_count
                    FROM praxis_transactions
                    WHERE created_timestamp < EXTRACT(EPOCH FROM %s::timestamp)
                    GROUP BY payment_processor, currency
                    ORDER BY payment_processor, currency
                """, (month_end,))
                return [dict(r) for r in cur.fetchall()]
        result = _db_retry(_fetch)
    except Exception:
        result = []

    _cache_set(key, result)
    return result


# ═══════════════════════════════════════════════════════════════════════════
# PSPs tab — arbitrary date-range analytics over praxis_transactions
# (unlike the month-scoped functions above, these take any (date_from, date_to)
# window so the /psps pages can offer rolling 7d/30d/90d/custom ranges).
# ═══════════════════════════════════════════════════════════════════════════

def _num_row(row: dict, float_keys) -> dict:
    """Coerce the given keys to plain float (psycopg2 returns Decimal for NUMERIC
    columns, which Flask's `|tojson` filter can't serialize) — 0.0 for NULL."""
    for k in float_keys:
        row[k] = float(row[k] or 0)
    return row


# ── Transaction-outcome classifier ─────────────────────────────────────────
# The Praxis reporting table has NO clean status: transaction_status/session_status
# are 100% NULL and outcome only exists as a per-processor status_code plus a
# free-text status_details whose meaning varies by processor (status_code='0' means
# APPROVED for SafeCharge but is a meaningless default for KorapayAPM, whose outcome
# lives entirely in status_details). We derive a BEST-EFFORT outcome class from the
# status_details keywords (the most portable signal) with per-processor overrides for
# the NULL/ambiguous cases. The share we cannot classify is bucketed as 'unknown' and
# surfaced honestly in the UI.
#
# Precedence (first match wins): chargeback > refunded > declined > pending > approved.
# declined before pending so a terminal "... New status declined" isn't mislabelled by
# the earlier "pending" word; pending before approved so 3DS/OTP interstitials
# ("Successfully Authenticated", "enter the PIN") aren't counted as approvals.

OUTCOME_CLASSES = ("approved", "declined", "pending", "refunded", "chargeback", "unknown")

_KW_CHARGEBACK = ["chargeback", "charge back", "dispute"]
_KW_REFUND     = ["refund", "reversal", "reversed"]
_KW_DECLINED   = ["insuff", "not sufficient", "declin", "do not honor", "do not honour",
                  "not honor", "not honour", "fraud", "invalid", "not permitted",
                  "not allowed", "expired", "timed out", "timeout", "timedout",
                  "unable to", "fail", "reject", "cancel", "exceed", "not active",
                  "not supported", "blocked", "suspect", "policy", "limit reached",
                  "restricted", "error", "no response", "not enough", "abandon",
                  "abort", "denied", "violation", "not available", "unavailable",
                  "retry", "missing", "unpaid", "not support", "should be",
                  "no payment method", "msisdn"]
_KW_PENDING    = ["pending", "3ds", "3-ds", "authentication required", "authenticated",
                  "enrolled", "pin", "awaiting", "initiated", "in progress", "requires",
                  "otp", "waiting", "please wait", "being processed", "please provide",
                  "processing"]
_KW_APPROVED   = ["approved", "success", "succeed", "complete", "captured", "settled",
                  "accepted", "accept", "paid"]

# Precedence order shared by the SQL CASE and the Python mirror below.
_OUTCOME_RULES = [
    ("chargeback", _KW_CHARGEBACK),
    ("refunded",   _KW_REFUND),
    ("declined",   _KW_DECLINED),
    ("pending",    _KW_PENDING),
    ("approved",   _KW_APPROVED),
]


def classify_outcome(status_details, status_code=None, payment_processor=None) -> str:
    """Best-effort outcome class for one Praxis transaction — Python mirror of
    outcome_case_sql(). Returns one of OUTCOME_CLASSES."""
    d = (status_details or "").lower()
    if d:
        for cls, kws in _OUTCOME_RULES:
            if any(k in d for k in kws):
                return cls
    # NULL / blank details → per-processor overrides on status_code
    proc = (payment_processor or "").lower()
    code = (status_code or "").strip()
    if proc.startswith("safecharge") and code == "0":
        return "approved"
    return "unknown"


def outcome_case_sql(details_col="status_details", code_col="status_code",
                     proc_col="payment_processor") -> str:
    """SQL CASE expression matching classify_outcome(), so approval metrics aggregate
    SQL-side over the full table instead of pulling rows into Python.
    NB: literal % are doubled for psycopg2 params-mode execution."""
    def _ilike_any(col, kws):
        arr = ", ".join("'%%" + k.replace("'", "''") + "%%'" for k in kws)
        return f"{col} ILIKE ANY (ARRAY[{arr}])"
    whens = [f"WHEN {_ilike_any(details_col, kws)} THEN '{cls}'" for cls, kws in _OUTCOME_RULES]
    whens.append(
        f"WHEN ({details_col} IS NULL OR {details_col} = '') "
        f"AND {proc_col} ILIKE 'safecharge%%' AND {code_col} = '0' THEN 'approved'"
    )
    return "CASE\n  " + "\n  ".join(whens) + "\n  ELSE 'unknown' END"


def psp_data_span():
    """(min_date, max_date) of praxis_transactions by created_timestamp. Cached 30 min.

    Used to anchor the dashboard date-range on *available* data: the Praxis feed can
    lag behind 'today', so "last N days" should mean the last N days of data we have,
    not a window that runs past the end of the data and shows nothing."""
    key = "psp_data_span"
    cached = _cache_get(key, 1800)
    if cached is not None:
        return cached
    import datetime as _dt
    span = (None, None)
    try:
        from db import praxis as praxis_ctx
        def _fetch():
            with praxis_ctx() as cur:
                cur.execute("SELECT MIN(created_timestamp) AS mn, MAX(created_timestamp) AS mx "
                            "FROM praxis_transactions")
                return cur.fetchone()
        row = _db_retry(_fetch)
        if row and row.get("mn") and row.get("mx"):
            span = (_dt.datetime.utcfromtimestamp(int(row["mn"])).date(),
                    _dt.datetime.utcfromtimestamp(int(row["mx"])).date())
    except Exception:
        span = (None, None)
    _cache_set(key, span)
    return span


def psp_data_max_date():
    """Latest transaction date present in praxis_transactions (by created_timestamp)."""
    return psp_data_span()[1]


def psp_distinct_processors(date_from=None, date_to=None) -> list:
    """Distinct Praxis processors with volume/tx_count/fees in the given window.
    With no window, covers all-time — used to populate the processor filter dropdown.
    Cached 5 min."""
    key = f"psp_processors:{date_from}:{date_to}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached

    oc = outcome_case_sql()
    try:
        from db import praxis as praxis_ctx
        def _fetch():
            with praxis_ctx() as cur:
                where = ["payment_processor IS NOT NULL", "payment_processor != ''"]
                params = []
                if date_from and date_to:
                    where.append("created_timestamp >= EXTRACT(EPOCH FROM %s::timestamp)")
                    where.append("created_timestamp <  EXTRACT(EPOCH FROM %s::timestamp)")
                    params = [date_from, date_to]
                cur.execute(f"""
                    SELECT payment_processor                          AS psp,
                           COUNT(*)                                    AS tx_count,
                           SUM(usd_amount)                             AS volume,
                           SUM(fee / 100.0)                            AS actual_fees,
                           COUNT(*) FILTER (WHERE outcome='approved')  AS approved,
                           COUNT(*) FILTER (WHERE outcome='declined')  AS declined
                    FROM (SELECT payment_processor, usd_amount, fee, {oc} AS outcome
                          FROM praxis_transactions
                          WHERE {' AND '.join(where)}) t
                    GROUP BY payment_processor
                    ORDER BY volume DESC NULLS LAST
                """, params)
                rows = []
                for r in cur.fetchall():
                    row = _num_row(dict(r), ("volume", "actual_fees"))
                    decided = (row.get("approved") or 0) + (row.get("declined") or 0)
                    row["decided"] = decided
                    row["success_rate"] = round(100.0 * row["approved"] / decided, 1) if decided else None
                    rows.append(row)
                return rows
        result = _db_retry(_fetch)
    except Exception:
        result = []

    _cache_set(key, result)
    return result


def psp_dashboard_stats(date_from, date_to, processor: str = None) -> dict:
    """Headline stats for the PSPs dashboard over a date range, incl. derived outcome
    counts (approved/declined/pending/refunded/chargeback/unknown), a success/approval
    rate (approved of *decided* = approved+declined) and a chargeback rate. Cached 5 min."""
    key = f"psp_stats:{date_from}:{date_to}:{processor}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached

    oc = outcome_case_sql()
    try:
        from db import praxis as praxis_ctx
        def _fetch():
            with praxis_ctx() as cur:
                where = [
                    "created_timestamp >= EXTRACT(EPOCH FROM %s::timestamp)",
                    "created_timestamp <  EXTRACT(EPOCH FROM %s::timestamp)",
                ]
                params = [date_from, date_to]
                if processor:
                    where.append("payment_processor = %s")
                    params.append(processor)
                cur.execute(f"""
                    SELECT
                        COUNT(*)                                              AS tx_count,
                        COALESCE(SUM(usd_amount), 0)                          AS total_volume_usd,
                        COALESCE(SUM(fee / 100.0), 0)                         AS total_fees_actual_usd,
                        COALESCE(SUM(CASE WHEN session_intent='payment'
                                 THEN usd_amount ELSE 0 END), 0)              AS deposits_usd,
                        COALESCE(SUM(CASE WHEN session_intent IN ('withdrawal','payout')
                                 THEN usd_amount ELSE 0 END), 0)              AS withdrawals_usd,
                        COALESCE(SUM(CASE WHEN session_intent='payment'
                                 THEN usd_amount ELSE -usd_amount END), 0)    AS net_usd,
                        COUNT(DISTINCT session_cid)                           AS distinct_customers,
                        COUNT(*) FILTER (WHERE outcome='approved')            AS approved_count,
                        COUNT(*) FILTER (WHERE outcome='declined')            AS declined_count,
                        COUNT(*) FILTER (WHERE outcome='pending')             AS pending_count,
                        COUNT(*) FILTER (WHERE outcome='refunded')            AS refunded_count,
                        COUNT(*) FILTER (WHERE outcome='chargeback')          AS chargeback_count,
                        COUNT(*) FILTER (WHERE outcome='unknown')             AS unknown_count
                    FROM (
                        SELECT usd_amount, session_intent, session_cid, fee,
                               {oc} AS outcome
                        FROM praxis_transactions
                        WHERE {' AND '.join(where)}
                    ) t
                """, params)
                row = dict(cur.fetchone())
                for k in ("total_volume_usd", "total_fees_actual_usd", "deposits_usd",
                          "withdrawals_usd", "net_usd"):
                    row[k] = float(row[k] or 0)
                tc      = row["tx_count"] or 0
                decided = (row["approved_count"] or 0) + (row["declined_count"] or 0)
                row["decided_count"]   = decided
                row["avg_ticket_usd"]  = (row["total_volume_usd"] / tc) if tc else 0.0
                row["success_rate"]    = (100.0 * row["approved_count"] / decided) if decided else 0.0
                row["chargeback_rate"] = (100.0 * row["chargeback_count"] / tc) if tc else 0.0
                # Backwards-compat alias (Praxis-reported actual fee, near-zero — the
                # dashboard shows expected fees from the calculator instead).
                row["total_fees_usd"]  = row["total_fees_actual_usd"]
                return row
        result = _db_retry(_fetch)
    except Exception:
        result = {"tx_count": 0, "total_volume_usd": 0, "total_fees_usd": 0,
                  "total_fees_actual_usd": 0, "deposits_usd": 0, "withdrawals_usd": 0,
                  "net_usd": 0, "distinct_customers": 0, "avg_ticket_usd": 0,
                  "approved_count": 0, "declined_count": 0, "pending_count": 0,
                  "refunded_count": 0, "chargeback_count": 0, "unknown_count": 0,
                  "decided_count": 0, "success_rate": 0, "chargeback_rate": 0}

    _cache_set(key, result)
    return result


def psp_status_distribution(date_from, date_to, processor: str = None) -> list:
    """Tx counts + volume per derived outcome class, for the status-distribution
    doughnut. Ordered by OUTCOME_CLASSES; only non-empty classes returned. Cached 5 min."""
    key = f"psp_statusdist:{date_from}:{date_to}:{processor}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached
    oc = outcome_case_sql()
    try:
        from db import praxis as praxis_ctx
        def _fetch():
            with praxis_ctx() as cur:
                where = ["created_timestamp >= EXTRACT(EPOCH FROM %s::timestamp)",
                         "created_timestamp <  EXTRACT(EPOCH FROM %s::timestamp)"]
                params = [date_from, date_to]
                if processor:
                    where.append("payment_processor = %s"); params.append(processor)
                cur.execute(f"""
                    SELECT outcome, COUNT(*) AS tx_count, COALESCE(SUM(usd_amount),0) AS volume
                    FROM (SELECT usd_amount, {oc} AS outcome FROM praxis_transactions
                          WHERE {' AND '.join(where)}) t
                    GROUP BY outcome
                """, params)
                by = {r["outcome"]: {"outcome": r["outcome"], "tx_count": r["tx_count"],
                                     "volume": float(r["volume"] or 0)} for r in cur.fetchall()}
                return [by[c] for c in OUTCOME_CLASSES if c in by]
        result = _db_retry(_fetch)
    except Exception:
        result = []
    _cache_set(key, result)
    return result


def psp_approval_ratio(date_from, date_to, processor: str = None, group_by: str = "country",
                       limit: int = 200, min_decided: int = 0) -> list:
    """Approval rate (approved / (approved+declined)) grouped by customer country or
    processor, for the dashboard approval-ratio panel (country/PSP toggle). Returns
    EVERY group so the panel matches the Providers list — rated groups first (rate desc),
    then rate-less groups (0 decided, ratio=None → rendered as '—') by activity. Cached 5 min."""
    group_by = "psp" if group_by == "psp" else "country"
    dim = "payment_processor" if group_by == "psp" else "customer_country"
    key = f"psp_appratio:{date_from}:{date_to}:{processor}:{group_by}:{limit}:{min_decided}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached
    oc = outcome_case_sql()
    try:
        from db import praxis as praxis_ctx
        def _fetch():
            with praxis_ctx() as cur:
                where = ["created_timestamp >= EXTRACT(EPOCH FROM %s::timestamp)",
                         "created_timestamp <  EXTRACT(EPOCH FROM %s::timestamp)",
                         f"{dim} IS NOT NULL", f"{dim} <> ''"]
                params = [date_from, date_to]
                if processor:
                    where.append("payment_processor = %s"); params.append(processor)
                cur.execute(f"""
                    SELECT name,
                           COUNT(*) FILTER (WHERE outcome='approved') AS approved,
                           COUNT(*) FILTER (WHERE outcome='declined') AS declined,
                           COUNT(*) AS total
                    FROM (SELECT {dim} AS name, {oc} AS outcome
                          FROM praxis_transactions WHERE {' AND '.join(where)}) t
                    GROUP BY name
                """, params)
                rated, unrated = [], []
                for r in cur.fetchall():
                    decided = (r["approved"] or 0) + (r["declined"] or 0)
                    if decided < min_decided:
                        continue
                    row = {"name": r["name"], "approved": r["approved"] or 0,
                           "declined": r["declined"] or 0, "total": r["total"], "decided": decided,
                           "ratio": round(100.0 * (r["approved"] or 0) / decided, 1) if decided else None}
                    (rated if decided else unrated).append(row)
                # Rank by activity (decided count) so the highest-signal groups lead and
                # tiny 1/1 = 100% samples don't dominate the top; the colour-coded bar still
                # conveys each rate. Rate-less (0 decided) groups trail, by total activity.
                rated.sort(key=lambda x: x["decided"], reverse=True)
                unrated.sort(key=lambda x: x["total"], reverse=True)
                return (rated + unrated)[:limit]
        result = _db_retry(_fetch)
    except Exception:
        result = []
    _cache_set(key, result)
    return result


def psp_approval_tree(date_from, date_to, processor: str = None, group_by: str = "country") -> list:
    """Two-level approval drill-down for the dashboard: root = country|psp (per group_by),
    children = the other dimension. Each node carries approved/declined/decided/total and a
    ratio (approved/(approved+declined); None if 0 decided). Groups both dimensions in one
    cached query, then nests. Nodes sorted by activity (decided) desc. Cached 5 min."""
    group_by = "psp" if group_by == "psp" else "country"
    key = f"psp_apptree:{date_from}:{date_to}:{processor}:{group_by}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached
    oc = outcome_case_sql()

    def _ratio(app, dec):
        d = (app or 0) + (dec or 0)
        return round(100.0 * (app or 0) / d, 1) if d else None

    try:
        from db import praxis as praxis_ctx
        def _fetch():
            with praxis_ctx() as cur:
                where = ["created_timestamp >= EXTRACT(EPOCH FROM %s::timestamp)",
                         "created_timestamp <  EXTRACT(EPOCH FROM %s::timestamp)",
                         "customer_country IS NOT NULL AND customer_country <> ''",
                         "payment_processor IS NOT NULL AND payment_processor <> ''"]
                params = [date_from, date_to]
                if processor:
                    where.append("payment_processor = %s"); params.append(processor)
                cur.execute(f"""
                    SELECT country, psp,
                           COUNT(*) FILTER (WHERE outcome='approved') AS approved,
                           COUNT(*) FILTER (WHERE outcome='declined') AS declined,
                           COUNT(*) AS total
                    FROM (SELECT customer_country AS country, payment_processor AS psp,
                                 {oc} AS outcome
                          FROM praxis_transactions WHERE {' AND '.join(where)}) t
                    GROUP BY country, psp
                """, params)
                return cur.fetchall()
        rows = _db_retry(_fetch)

        roots: dict = {}
        for r in rows:
            root_name  = r["country"] if group_by == "country" else r["psp"]
            child_name = r["psp"]     if group_by == "country" else r["country"]
            app, dec, tot = (r["approved"] or 0), (r["declined"] or 0), (r["total"] or 0)
            node = roots.setdefault(root_name, {"name": root_name, "approved": 0, "declined": 0,
                                                "total": 0, "children": []})
            node["approved"] += app; node["declined"] += dec; node["total"] += tot
            node["children"].append({"name": child_name, "approved": app, "declined": dec,
                                     "total": tot, "decided": app + dec, "ratio": _ratio(app, dec)})
        out = []
        for node in roots.values():
            node["decided"] = node["approved"] + node["declined"]
            node["ratio"] = _ratio(node["approved"], node["declined"])
            node["children"].sort(key=lambda c: c["decided"], reverse=True)
            out.append(node)
        out.sort(key=lambda n: n["decided"], reverse=True)
        result = out
    except Exception:
        result = []
    _cache_set(key, result)
    return result


def psp_expected_fees(date_from, date_to, processor: str = None) -> dict:
    """Expected PSP fees over a date range from the fee-agreement engine — the Praxis
    `fee` column is unpopulated (~0), so this is the real fee figure. Aggregates
    APPROVED transactions by (processor, method, direction) and applies fee rules per
    group: exact for percentage / fixed / fixed+pct, approximate for tiered (fee on the
    group's average ticket × count). Cached 5 min.
    Returns {'total', 'matched_vol', 'unmatched_vol'} (all USD)."""
    key = f"psp_expfees:{date_from}:{date_to}:{processor}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached
    oc = outcome_case_sql()
    total = matched = unmatched = 0.0
    try:
        rules_by_id, fee_rules_by_psp, saved_mappings, method_mappings_db = _load_fee_calc_context()
        from db import praxis as praxis_ctx
        def _fetch():
            with praxis_ctx() as cur:
                where = ["created_timestamp >= EXTRACT(EPOCH FROM %s::timestamp)",
                         "created_timestamp <  EXTRACT(EPOCH FROM %s::timestamp)"]
                params = [date_from, date_to]
                if processor:
                    where.append("payment_processor = %s"); params.append(processor)
                cur.execute(f"""
                    SELECT payment_processor, payment_method, session_intent,
                           COALESCE(SUM(usd_amount),0) AS vol, COUNT(*) AS n
                    FROM (SELECT payment_processor, payment_method, session_intent, usd_amount,
                                 {oc} AS outcome FROM praxis_transactions
                          WHERE {' AND '.join(where)}) t
                    WHERE outcome = 'approved'
                    GROUP BY payment_processor, payment_method, session_intent
                """, params)
                return cur.fetchall()
        for g in _db_retry(_fetch):
            vol = float(g["vol"] or 0); n = g["n"] or 0
            if not n or vol == 0:
                continue
            fee_type = "Deposit" if (g["session_intent"] or "") == "payment" else "Withdrawal"
            fee = _compute_tx_fee(vol / n, g["payment_processor"], g["payment_method"], fee_type,
                                  rules_by_id, fee_rules_by_psp, saved_mappings, method_mappings_db) * n
            if fee > 0:
                total += fee; matched += vol
            else:
                unmatched += vol
    except Exception:
        pass
    result = {"total": round(total, 2), "matched_vol": round(matched, 2),
              "unmatched_vol": round(unmatched, 2)}
    _cache_set(key, result)
    return result


def psp_volume_by_day(date_from, date_to, processor: str = None) -> list:
    """Daily volume + tx count in the range, for the dashboard bar chart. Cached 5 min."""
    key = f"psp_volday:{date_from}:{date_to}:{processor}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached

    try:
        from db import praxis as praxis_ctx
        def _fetch():
            with praxis_ctx() as cur:
                where = [
                    "created_timestamp >= EXTRACT(EPOCH FROM %s::timestamp)",
                    "created_timestamp <  EXTRACT(EPOCH FROM %s::timestamp)",
                ]
                params = [date_from, date_to]
                if processor:
                    where.append("payment_processor = %s")
                    params.append(processor)
                cur.execute(f"""
                    SELECT
                        DATE(TO_TIMESTAMP(created_timestamp)) AS day,
                        SUM(usd_amount)                       AS volume,
                        COUNT(*)                              AS tx_count
                    FROM praxis_transactions
                    WHERE {' AND '.join(where)}
                    GROUP BY day
                    ORDER BY day
                """, params)
                return [_num_row(dict(r), ("volume",)) for r in cur.fetchall()]
        result = _db_retry(_fetch)
    except Exception:
        result = []

    _cache_set(key, result)
    return result


def psp_breakdown_by_direction(date_from, date_to, processor: str = None) -> list:
    """Volume/tx count grouped by session_intent (payment/withdrawal/payout). Cached 5 min."""
    key = f"psp_dir:{date_from}:{date_to}:{processor}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached

    try:
        from db import praxis as praxis_ctx
        def _fetch():
            with praxis_ctx() as cur:
                where = [
                    "created_timestamp >= EXTRACT(EPOCH FROM %s::timestamp)",
                    "created_timestamp <  EXTRACT(EPOCH FROM %s::timestamp)",
                ]
                params = [date_from, date_to]
                if processor:
                    where.append("payment_processor = %s")
                    params.append(processor)
                cur.execute(f"""
                    SELECT
                        COALESCE(NULLIF(session_intent, ''), 'unknown') AS direction,
                        SUM(usd_amount)                                 AS volume,
                        COUNT(*)                                        AS tx_count
                    FROM praxis_transactions
                    WHERE {' AND '.join(where)}
                    GROUP BY direction
                    ORDER BY volume DESC NULLS LAST
                """, params)
                return [_num_row(dict(r), ("volume",)) for r in cur.fetchall()]
        result = _db_retry(_fetch)
    except Exception:
        result = []

    _cache_set(key, result)
    return result


def psp_breakdown_by_currency(date_from, date_to, processor: str = None, top_n: int = 8) -> list:
    """Top-N currencies by volume in the range. Cached 5 min."""
    key = f"psp_ccy:{date_from}:{date_to}:{processor}:{top_n}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached

    try:
        from db import praxis as praxis_ctx
        def _fetch():
            with praxis_ctx() as cur:
                where = [
                    "created_timestamp >= EXTRACT(EPOCH FROM %s::timestamp)",
                    "created_timestamp <  EXTRACT(EPOCH FROM %s::timestamp)",
                ]
                params = [date_from, date_to]
                if processor:
                    where.append("payment_processor = %s")
                    params.append(processor)
                params.append(top_n)
                cur.execute(f"""
                    SELECT currency, SUM(usd_amount) AS volume, COUNT(*) AS tx_count
                    FROM praxis_transactions
                    WHERE {' AND '.join(where)}
                    GROUP BY currency
                    ORDER BY volume DESC NULLS LAST
                    LIMIT %s
                """, params)
                return [_num_row(dict(r), ("volume",)) for r in cur.fetchall()]
        result = _db_retry(_fetch)
    except Exception:
        result = []

    _cache_set(key, result)
    return result


def psp_recent_transactions(date_from, date_to, processor: str = None, limit: int = 10) -> list:
    """Most recent transactions in range, for the dashboard's recent-activity table."""
    try:
        from db import praxis as praxis_ctx
        def _fetch():
            with praxis_ctx() as cur:
                where = [
                    "created_timestamp >= EXTRACT(EPOCH FROM %s::timestamp)",
                    "created_timestamp <  EXTRACT(EPOCH FROM %s::timestamp)",
                ]
                params = [date_from, date_to]
                if processor:
                    where.append("payment_processor = %s")
                    params.append(processor)
                params.append(limit)
                cur.execute(f"""
                    SELECT
                        session_cid AS login, tid, session_order_id,
                        session_intent AS direction, usd_amount, currency,
                        payment_method, payment_processor, customer_country,
                        fee / 100.0 AS fee_actual,
                        customer_first_name, customer_last_name,
                        wallet_data_email AS email,
                        status_details, status_code,
                        TO_TIMESTAMP(created_timestamp) AS inserted_at
                    FROM praxis_transactions
                    WHERE {' AND '.join(where)}
                    ORDER BY created_timestamp DESC
                    LIMIT %s
                """, params)
                rows = []
                for r in cur.fetchall():
                    row = dict(r)
                    row["outcome"] = classify_outcome(row.get("status_details"),
                                                       row.get("status_code"),
                                                       row.get("payment_processor"))
                    rows.append(row)
                return rows
        return _db_retry(_fetch)
    except Exception:
        return []


def psp_transactions_search(date_from, date_to, processor: str = None, direction: str = None,
                             search: str = None, page: int = 1, page_size: int = 50) -> dict:
    """Paginated, filterable transaction explorer over praxis_transactions.

    Returns {"rows": [...], "total_count": int, "page": int, "page_size": int}.
    Not cached — filters/pagination change on every request.
    """
    page = max(page, 1)
    offset = (page - 1) * page_size

    try:
        from db import praxis as praxis_ctx
        where = [
            "created_timestamp >= EXTRACT(EPOCH FROM %s::timestamp)",
            "created_timestamp <  EXTRACT(EPOCH FROM %s::timestamp)",
        ]
        params = [date_from, date_to]
        if processor:
            where.append("payment_processor = %s")
            params.append(processor)
        if direction:
            where.append("session_intent = %s")
            params.append(direction)
        if search:
            where.append("""(
                tid::text ILIKE %s OR
                transaction_id::text ILIKE %s OR
                session_order_id::text ILIKE %s OR
                session_cid::text ILIKE %s OR
                wallet_data_email ILIKE %s OR
                (customer_first_name || ' ' || customer_last_name) ILIKE %s
            )""")
            like = f"%{search}%"
            params.extend([like, like, like, like, like, like])
        where_sql = " AND ".join(where)

        def _fetch_count():
            with praxis_ctx() as cur:
                cur.execute(f"SELECT COUNT(*) AS n FROM praxis_transactions WHERE {where_sql}", params)
                return cur.fetchone()["n"]
        total_count = _db_retry(_fetch_count)

        def _fetch_rows():
            with praxis_ctx() as cur:
                cur.execute(f"""
                    SELECT
                        session_cid AS login, tid, transaction_id, session_order_id,
                        session_intent AS direction, amount / 100.0 AS amount_local,
                        currency, usd_amount, payment_method, payment_processor,
                        customer_country, conversion_rate, fee / 100.0 AS fee_actual,
                        wallet_data_email AS email,
                        customer_first_name, customer_last_name,
                        status_details, status_code,
                        TO_TIMESTAMP(created_timestamp) AS inserted_at
                    FROM praxis_transactions
                    WHERE {where_sql}
                    ORDER BY created_timestamp DESC
                    LIMIT %s OFFSET %s
                """, params + [page_size, offset])
                account_map, _ = _load_praxis_account_map()
                rows = []
                for r in cur.fetchall():
                    row = dict(r)
                    row["usd_amount"] = float(row["usd_amount"] or 0)
                    row["fee_actual"] = float(row["fee_actual"] or 0)
                    row["outcome"] = classify_outcome(row.get("status_details"),
                                                      row.get("status_code"),
                                                      row.get("payment_processor"))
                    cid = str(row.get("login") or "").strip()
                    logins = account_map.get(cid, [])
                    row["mt5_login"] = logins[0] if logins else None
                    rows.append(row)
                return rows
        rows = _db_retry(_fetch_rows)
    except Exception:
        total_count, rows = 0, []

    return {"rows": rows, "total_count": total_count, "page": page, "page_size": page_size}


def psp_directory(date_from, date_to) -> list:
    """Per-processor directory joining live Praxis activity with fee-agreement mappings,
    for the PSPs > Providers page."""
    processors = psp_distinct_processors(date_from, date_to)
    proc_mappings = get_processor_mappings()
    agreements_by_id = {a["id"]: a for a in get_all_agreements()}

    directory = []
    for p in processors:
        mapping = proc_mappings.get(p["psp"])
        agreement = agreements_by_id.get(mapping["agreement_id"]) if mapping else None
        directory.append({
            **p,
            "mapped": bool(agreement),
            "agreement_id": agreement["id"] if agreement else None,
            "agreement_psp_name": agreement["psp_name"] if agreement else None,
            "confirmed": mapping["confirmed"] if mapping else False,
        })
    return directory


def cid_full_profile(cid: str, date_from, date_to) -> dict:
    """
    Full profile for a Praxis customer (session_cid) across all their MT5 accounts.
    Returns a unified dict with name, email, all logins, Praxis txs, CRM + MT5 per login.
    Cached 5 minutes.
    """
    key = f"cid_profile:{cid}:{date_from}:{date_to}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached

    account_map, _fallback_cids = _load_praxis_account_map()
    logins = account_map.get(str(cid).strip(), [])

    # 1. Praxis transactions — fetch for ALL CIDs with same name (merged view)
    praxis_txs = []
    name = "—"
    email = "—"

    # First get related CIDs by fetching all CIDs for this name (requires knowing name first)
    # We do a two-pass: primary CID first, then detect name, then fetch all related
    try:
        from db import praxis as praxis_ctx
        def _fetch_praxis_for_cids(cids):
            with praxis_ctx() as cur:
                placeholders = ",".join(["%s"] * len(cids))
                cur.execute(f"""
                    SELECT tid, session_cid, session_intent AS direction,
                           usd_amount, amount/100.0 AS amount_local, currency,
                           payment_method, payment_processor,
                           fee/100.0 AS fee_actual,
                           wallet_data_email AS email,
                           customer_first_name AS first_name,
                           customer_last_name  AS last_name,
                           TO_TIMESTAMP(created_timestamp) AS ts
                    FROM praxis_transactions
                    WHERE session_cid IN ({placeholders})
                      AND created_timestamp >= EXTRACT(EPOCH FROM %s::timestamp)
                      AND created_timestamp <  EXTRACT(EPOCH FROM %s::timestamp)
                    ORDER BY created_timestamp
                """, list(cids) + [date_from, date_to])
                rows = []
                for r in cur.fetchall():
                    row = dict(r)
                    row["usd_amount"] = float(row["usd_amount"] or 0)
                    row["fee_actual"] = float(row["fee_actual"] or 0)
                    row["ts"] = str(row["ts"])[:16] if row["ts"] else ""
                    rows.append(row)
                return rows

        # Pass 1: fetch primary CID to get the name
        primary_txs = _db_retry(lambda: _fetch_praxis_for_cids([cid]))
        if primary_txs:
            s = primary_txs[0]
            parts = [(s.get("first_name") or "").strip(), (s.get("last_name") or "").strip()]
            name  = " ".join(p for p in parts if p) or "—"
            email = s.get("email") or "—"
    except Exception:
        primary_txs = []

    # Name fallback: if Praxis had no transactions for this CID, look up vtiger_account
    if name == "—":
        try:
            def _fetch_crm_name():
                with crm() as cur:
                    cur.execute("""
                        SELECT RTRIM(first_name + ' ' + last_name) AS full_name, email
                        FROM report.vtiger_account
                        WHERE accountid = %s
                    """, (int(cid),))
                    return cur.fetchone()
            row = _db_retry(_fetch_crm_name)
            if row:
                name  = (row["full_name"] or "").strip() or "—"
                email = row["email"] or "—"
        except Exception:
            pass

    # Pass 2: find all related CIDs by name, then fetch all their txs together
    all_cids = [cid]
    first_name = (primary_txs[0].get("first_name") or "").strip() if primary_txs else ""
    last_name  = (primary_txs[0].get("last_name")  or "").strip() if primary_txs else ""
    if first_name and last_name and first_name.lower() not in ("test",""):
        try:
            from db import praxis as praxis_ctx
            def _fetch_all_cids():
                with praxis_ctx() as cur:
                    cur.execute("""
                        SELECT DISTINCT session_cid FROM praxis_transactions
                        WHERE customer_first_name = %s AND customer_last_name = %s
                    """, (first_name, last_name))
                    return [r["session_cid"] for r in cur.fetchall()]
            all_cids = _db_retry(_fetch_all_cids) or [cid]
        except Exception:
            all_cids = [cid]

    # Fetch Praxis txs for ALL cids (primary + related merged)
    try:
        from db import praxis as praxis_ctx
        praxis_txs = _db_retry(lambda: _fetch_praxis_for_cids(all_cids))
    except Exception:
        praxis_txs = primary_txs

    # 2. CRM + MT5 per login
    crm_by_login = {}
    mt5_by_login = {}
    for login in logins:
        crm_by_login[login] = client_crm_detail(login, date_from, date_to)
        mt5_by_login[login] = client_mt5_detail(login, date_from, date_to)

    # 3. Summary totals
    praxis_dep  = sum(t["usd_amount"] for t in praxis_txs if t["direction"] == "payment")
    praxis_with = sum(t["usd_amount"] for t in praxis_txs if t["direction"] in ("withdrawal","payout"))
    crm_dep     = sum(
        r["total_usd"] for rows in crm_by_login.values() for r in rows
        if r["is_cash"] and r.get("transactionapproval") == "Approved"
        and r.get("transactiontype") in ("Deposit","TransferIn")
    )
    crm_with    = sum(
        r["total_usd"] for rows in crm_by_login.values() for r in rows
        if r["is_cash"] and r.get("transactionapproval") == "Approved"
        and r.get("transactiontype") in ("Withdrawal","Withdraw","TransferOut")
    )

    # 3b. Related CIDs = all_cids minus the primary one (already found above)
    related_cids = [c for c in all_cids if c != cid]
    # Get tx counts per related CID
    if related_cids:
        counts = {}
        for t in praxis_txs:
            sc = t.get("session_cid")
            if sc and sc != cid:
                counts[sc] = counts.get(sc, 0) + 1
        related_cids = [{"cid": c, "tx_count": counts.get(c, 0)} for c in related_cids]
    else:
        related_cids = []

    # 4. Trading P&L from MT5 (converted to USD), also compute per-login
    client_realised_pnl = 0.0
    client_unrealised_eod = 0.0
    pnl_by_login = {}
    for login, rows in mt5_by_login.items():
        # Use converted (USD) fields throughout — closedpnl is in native currency
        l_realised = sum(float(r.get("convertedclosedpnl") or r.get("closedpnl") or 0) for r in rows)
        l_unrealised = float(rows[-1].get("convertedfloatingpnl") or rows[-1].get("floatingpnl") or 0) if rows else 0.0
        l_net_dep = sum(float(r.get("convertednetdeposit") or 0) for r in rows)
        pnl_by_login[login] = {
            "client_realised":    round(l_realised, 2),
            "client_unrealised":  round(l_unrealised, 2),
            "company_trading":    round(-l_realised, 2),
            "net_deposit":        round(l_net_dep, 2),
            "company_total":      round(-l_realised + l_net_dep, 2),
        }
        client_realised_pnl   += l_realised
        client_unrealised_eod  = l_unrealised  # use last account's EOD

    company_trading_pnl = round(-client_realised_pnl, 2)   # positive = company profit
    net_deposit         = round(crm_dep - crm_with, 2)

    result = {
        "cid":          cid,
        "name":         name,
        "email":        email,
        "mt5_accounts": logins,
        "praxis_txs":   praxis_txs,
        "crm_by_login": crm_by_login,
        "mt5_by_login": mt5_by_login,
        "pnl_by_login": pnl_by_login,
        "related_cids": related_cids,
        "summary": {
            "praxis_deposits":      round(praxis_dep, 2),
            "praxis_withdrawals":   round(praxis_with, 2),
            "crm_cash_dep":         round(crm_dep, 2),
            "crm_cash_with":        round(crm_with, 2),
            "diff":                 round(praxis_dep - crm_dep, 2),
            "net_deposit":          net_deposit,
            "client_realised_pnl":  round(client_realised_pnl, 2),
            "client_unrealised_eod":round(client_unrealised_eod, 2),
            "company_trading_pnl":  company_trading_pnl,
            # Total company value: net_deposit − client_realised − client_floating
            # (includes unrealised exposure from open positions)
            "company_total_value":  round(company_trading_pnl + net_deposit - client_unrealised_eod, 2),
        }
    }
    _cache_set(key, result)
    return result


def client_crm_detail(login: int, date_from, date_to) -> list:
    """CRM transactions for a login over an arbitrary date range."""
    def _fetch():
        with crm() as cur:
            cur.execute("""
                SELECT
                    transactiontype, payment_method, transactionapproval,
                    COUNT(*)       AS tx_count,
                    SUM(usdamount) AS total_usd,
                    MIN(CAST(confirmation_time AS date)) AS first_date,
                    MAX(CAST(confirmation_time AS date)) AS last_date
                FROM report.vtiger_mttransactions
                WHERE login = %s
                  AND confirmation_time >= %s
                  AND confirmation_time <  %s
                  AND (deleted IS NULL OR deleted = 0)
                GROUP BY transactiontype, payment_method, transactionapproval
                ORDER BY transactionapproval, transactiontype, payment_method
            """, (login, date_from, date_to))
            return cur.fetchall()
    rows = _db_retry(_fetch)
    result = []
    for r in rows:
        is_cash = _is_cash(r["payment_method"] or "", r["transactiontype"] or "")
        result.append({**dict(r), "total_usd": round(float(r["total_usd"] or 0), 2), "is_cash": is_cash})
    return result


def client_mt5_detail(login: int, date_from, date_to) -> list:
    """MT5 daily profits for a login over an arbitrary date range."""
    def _fetch():
        with dealio() as cur:
            cur.execute("""
                SELECT date, netdeposit, convertednetdeposit, balance, equity,
                       closedpnl, convertedclosedpnl,
                       floatingpnl, convertedfloatingpnl,
                       groupcurrency, conversionratio
                FROM dealio.daily_profits
                WHERE login = %s
                  AND date >= %s AND date < %s
                  AND (
                      ABS(COALESCE(netdeposit, 0))   > 0.001
                   OR ABS(COALESCE(closedpnl, 0))    > 0.001
                   OR ABS(COALESCE(floatingpnl, 0))  > 0.001
                  )
                ORDER BY date
            """, (login, date_from, date_to))
            return [dict(r) for r in cur.fetchall()]
    return _db_retry(_fetch)


def client_praxis_detail(login: int, date_from, date_to) -> list:
    """Individual Praxis transactions for a login over an arbitrary date range.
    Resolves MT5 login → Praxis session_cid via vtiger_trading_accounts.
    """
    account_map, _fallback_cids = _load_praxis_account_map()
    # Reverse lookup: login → [cid, ...]
    cids = [cid for cid, logins in account_map.items() if login in logins]
    if not cids:
        return []
    try:
        from db import praxis as praxis_ctx
        def _fetch():
            placeholders = ",".join(["%s"] * len(cids))
            with praxis_ctx() as cur:
                cur.execute(f"""
                    SELECT tid, session_cid, session_intent AS direction,
                           usd_amount, amount/100.0 AS amount_local, currency,
                           payment_method, payment_processor,
                           fee/100.0 AS fee_actual,
                           wallet_data_email AS email,
                           customer_first_name AS first_name,
                           customer_last_name  AS last_name,
                           TO_TIMESTAMP(created_timestamp) AS ts
                    FROM praxis_transactions
                    WHERE session_cid IN ({placeholders})
                      AND created_timestamp >= EXTRACT(EPOCH FROM %s::timestamp)
                      AND created_timestamp <  EXTRACT(EPOCH FROM %s::timestamp)
                    ORDER BY created_timestamp
                """, cids + [date_from, date_to])
                rows = []
                for r in cur.fetchall():
                    row = dict(r)
                    row["usd_amount"] = float(row["usd_amount"] or 0)
                    row["fee_actual"] = float(row["fee_actual"] or 0)
                    row["ts"] = str(row["ts"])[:16] if row["ts"] else ""
                    rows.append(row)
                return rows
        return _db_retry(_fetch)
    except Exception:
        return []


def login_detail(year: int, month: int, login: int):
    """All CRM transactions for a specific login in a given month."""
    import datetime
    month_start = datetime.date(year, month, 1)
    month_end   = datetime.date(year + 1, 1, 1) if month == 12 else datetime.date(year, month + 1, 1)
    with crm() as cur:
        cur.execute("""
            SELECT
                transactiontype,
                payment_method,
                transactionapproval,
                COUNT(*)       AS tx_count,
                SUM(usdamount) AS total_usd,
                MIN(CAST(confirmation_time AS date)) AS first_date,
                MAX(CAST(confirmation_time AS date)) AS last_date
            FROM report.vtiger_mttransactions
            WHERE login = %s
              AND confirmation_time >= %s
              AND confirmation_time <  %s
              AND (deleted IS NULL OR deleted = 0)
            GROUP BY transactiontype, payment_method, transactionapproval
            ORDER BY transactionapproval, transactiontype, payment_method
        """, (login, month_start, month_end))
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



def login_crm_transactions(year: int, month: int, login: int):
    """Individual CRM transactions for a login in a month (for popover detail)."""
    import datetime
    month_start = datetime.date(year, month, 1)
    month_end   = datetime.date(year + 1, 1, 1) if month == 12 else datetime.date(year, month + 1, 1)
    with crm() as cur:
        cur.execute("""
            SELECT transactiontype, payment_method, transactionapproval,
                   usdamount, CAST(confirmation_time AS date) AS tx_date,
                   psp_transaction_id
            FROM report.vtiger_mttransactions
            WHERE login = %s
              AND confirmation_time >= %s AND confirmation_time < %s
              AND (deleted IS NULL OR deleted = 0)
            ORDER BY confirmation_time DESC
        """, (login, month_start, month_end))
        rows = cur.fetchall()
    return [{
        "type":     r["transactiontype"] or "",
        "method":   r["payment_method"] or chr(8212),
        "approval": r["transactionapproval"] or "",
        "usd":      round(float(r["usdamount"] or 0), 2),
        "date":     str(r["tx_date"]) if r["tx_date"] else "",
        "psp_id":   r["psp_transaction_id"] or "",
        "is_cash":  _is_cash(r["payment_method"] or "", r["transactiontype"] or ""),
    } for r in rows]


def login_mt5_detail(year: int, month: int, login: int):
    """Daily MT5 net deposit entries for a specific login."""
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

def _ensure_processor_mappings_table():
    """Maps Praxis payment_processor strings to psp_agreements.id."""
    if _PG:
        ddl = """
            CREATE TABLE IF NOT EXISTS processor_mappings (
                processor_name TEXT PRIMARY KEY,
                agreement_id   INTEGER REFERENCES psp_agreements(id) ON DELETE SET NULL,
                confirmed      INTEGER DEFAULT 0,
                created_at     TIMESTAMPTZ DEFAULT NOW(),
                updated_at     TIMESTAMPTZ DEFAULT NOW()
            );
        """
    else:
        ddl = """
            CREATE TABLE IF NOT EXISTS processor_mappings (
                processor_name TEXT PRIMARY KEY,
                agreement_id   INTEGER REFERENCES psp_agreements(id) ON DELETE SET NULL,
                confirmed      INTEGER DEFAULT 0,
                created_at     TEXT DEFAULT (datetime('now')),
                updated_at     TEXT DEFAULT (datetime('now'))
            );
        """
    with fees_db() as conn:
        conn.executescript(ddl)


def get_processor_mappings() -> dict:
    """Return {processor_name: {agreement_id, confirmed}} for all saved mappings."""
    with fees_db() as conn:
        rows = conn.execute(
            "SELECT processor_name, agreement_id, confirmed FROM processor_mappings"
        ).fetchall()
        return {r["processor_name"]: {"agreement_id": r["agreement_id"],
                                       "confirmed": bool(r["confirmed"])}
                for r in rows}


def save_processor_mapping(processor_name: str, agreement_id, confirmed: bool = True):
    with fees_db() as conn:
        if _PG:
            conn.execute("""
                INSERT INTO processor_mappings (processor_name, agreement_id, confirmed, updated_at)
                VALUES (?, ?, ?, NOW())
                ON CONFLICT (processor_name) DO UPDATE
                  SET agreement_id=EXCLUDED.agreement_id,
                      confirmed=EXCLUDED.confirmed,
                      updated_at=NOW()
            """, (processor_name, agreement_id, 1 if confirmed else 0))
        else:
            conn.execute("""
                INSERT OR REPLACE INTO processor_mappings
                  (processor_name, agreement_id, confirmed, updated_at)
                VALUES (?, ?, ?, datetime('now'))
            """, (processor_name, agreement_id, 1 if confirmed else 0))


def delete_processor_mapping(processor_name: str):
    with fees_db() as conn:
        conn.execute("DELETE FROM processor_mappings WHERE processor_name=?",
                     (processor_name,))


def _ensure_method_mappings_table():
    """Maps Praxis payment_method strings to canonical rule payment_method names."""
    if _PG:
        ddl = """
            CREATE TABLE IF NOT EXISTS method_mappings (
                praxis_method  TEXT PRIMARY KEY,
                canonical      TEXT NOT NULL,
                confirmed      INTEGER DEFAULT 0,
                created_at     TIMESTAMPTZ DEFAULT NOW(),
                updated_at     TIMESTAMPTZ DEFAULT NOW()
            );
        """
    else:
        ddl = """
            CREATE TABLE IF NOT EXISTS method_mappings (
                praxis_method  TEXT PRIMARY KEY,
                canonical      TEXT NOT NULL,
                confirmed      INTEGER DEFAULT 0,
                created_at     TEXT DEFAULT (datetime('now')),
                updated_at     TEXT DEFAULT (datetime('now'))
            );
        """
    with fees_db() as conn:
        conn.executescript(ddl)


def get_method_mappings() -> dict:
    """Return {praxis_method: canonical} for all saved method mappings."""
    with fees_db() as conn:
        rows = conn.execute(
            "SELECT praxis_method, canonical, confirmed FROM method_mappings"
        ).fetchall()
        return {r["praxis_method"]: {"canonical": r["canonical"],
                                      "confirmed": bool(r["confirmed"])}
                for r in rows}


def save_method_mapping(praxis_method: str, canonical: str, confirmed: bool = True):
    with fees_db() as conn:
        if _PG:
            conn.execute("""
                INSERT INTO method_mappings (praxis_method, canonical, confirmed, updated_at)
                VALUES (?, ?, ?, NOW())
                ON CONFLICT (praxis_method) DO UPDATE
                  SET canonical=EXCLUDED.canonical, confirmed=EXCLUDED.confirmed, updated_at=NOW()
            """, (praxis_method, canonical, 1 if confirmed else 0))
        else:
            conn.execute("""
                INSERT OR REPLACE INTO method_mappings
                  (praxis_method, canonical, confirmed, updated_at)
                VALUES (?, ?, ?, datetime('now'))
            """, (praxis_method, canonical, 1 if confirmed else 0))


def delete_method_mapping(praxis_method: str):
    with fees_db() as conn:
        conn.execute("DELETE FROM method_mappings WHERE praxis_method=?", (praxis_method,))


def ensure_fee_tables():
    _ensure_fee_tables_core()
    _ensure_prompt_tables()
    _ensure_context_notes_table()
    _ensure_amendment_tables()
    _ensure_processor_mappings_table()
    _ensure_method_mappings_table()
    _ensure_bank_tables()
    _ensure_operator_watermark_table()


def _ensure_operator_watermark_table():
    """Track max CRM operator ID seen — identifies truly new operators day-over-day."""
    if _PG:
        ddl = """CREATE TABLE IF NOT EXISTS operator_id_watermark (
            key TEXT PRIMARY KEY,
            max_id INTEGER NOT NULL DEFAULT 0,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );"""
        with fees_db() as conn:
            conn.execute(ddl)
    else:
        ddl = """CREATE TABLE IF NOT EXISTS operator_id_watermark (
            key TEXT PRIMARY KEY,
            max_id INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT DEFAULT (datetime('now'))
        );"""
        with fees_db() as conn:
            conn.executescript(ddl)


def get_operator_watermark(key: str = 'default') -> dict:
    with fees_db() as conn:
        row = conn.execute(
            "SELECT max_id, updated_at FROM operator_id_watermark WHERE key = ?", (key,)
        ).fetchone()
        return dict(row) if row else {"max_id": 0, "updated_at": None}


def update_operator_watermark(new_max_id: int, key: str = 'default') -> None:
    with fees_db() as conn:
        if _PG:
            conn.execute("""
                INSERT INTO operator_id_watermark (key, max_id)
                VALUES (%s, %s)
                ON CONFLICT(key) DO UPDATE SET max_id=EXCLUDED.max_id, updated_at=NOW()
            """, (key, new_max_id))
        else:
            conn.execute("""
                INSERT INTO operator_id_watermark (key, max_id, updated_at)
                VALUES (?, ?, datetime('now'))
                ON CONFLICT(key) DO UPDATE SET max_id=excluded.max_id, updated_at=excluded.updated_at
            """, (key, new_max_id))


def _ensure_prompt_tables():
    """Create and seed the prompt_templates table (SQLite or PostgreSQL)."""
    import ai_parse
    if _PG:
        _ddl = """
            CREATE TABLE IF NOT EXISTS prompt_templates (
                id            SERIAL PRIMARY KEY,
                name          TEXT NOT NULL,
                system_prompt TEXT NOT NULL,
                is_default    INTEGER DEFAULT 0,
                prompt_type   TEXT DEFAULT 'agreement',
                is_builtin    INTEGER DEFAULT 0,
                created_at    TIMESTAMPTZ DEFAULT NOW(),
                updated_at    TIMESTAMPTZ DEFAULT NOW()
            );
        """
    else:
        _ddl = """
            CREATE TABLE IF NOT EXISTS prompt_templates (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT NOT NULL,
                system_prompt TEXT NOT NULL,
                is_default  INTEGER DEFAULT 0,
                created_at  TEXT DEFAULT (datetime('now')),
                updated_at  TEXT DEFAULT (datetime('now'))
            );
        """

    with fees_db() as conn:
        conn.executescript(_ddl)

        if not _PG:
            # SQLite column migrations
            for col, default in [("prompt_type TEXT", "'agreement'"), ("is_builtin INTEGER", "0")]:
                try:
                    conn.execute(f"ALTER TABLE prompt_templates ADD COLUMN {col} DEFAULT {default}")
                except Exception:
                    pass
        else:
            # PostgreSQL: ADD COLUMN IF NOT EXISTS
            for col, default in [("prompt_type TEXT", "'agreement'"), ("is_builtin INTEGER", "0")]:
                col_name = col.split()[0]
                try:
                    conn.execute(
                        f"ALTER TABLE prompt_templates ADD COLUMN IF NOT EXISTS {col} DEFAULT {default}"
                    )
                except Exception:
                    pass

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
                conn.execute("""
                    UPDATE prompt_templates SET is_default = 1
                    WHERE id = (SELECT id FROM prompt_templates WHERE prompt_type=? ORDER BY id LIMIT 1)
                """, (ptype,))

        amend_count = conn.execute(
            "SELECT COUNT(*) FROM prompt_templates WHERE prompt_type = 'amendment'"
        ).fetchone()[0]
        if amend_count == 0:
            conn.execute(
                "INSERT INTO prompt_templates (name, system_prompt, is_default, prompt_type, is_builtin) VALUES (?, ?, 1, 'amendment', 1)",
                ("Default — Amendment Parser", ai_parse.AMENDMENT_SYSTEM_PROMPT),
            )

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
    if _PG:
        _ddl = """
            CREATE TABLE IF NOT EXISTS context_notes (
                id           SERIAL PRIMARY KEY,
                label        TEXT NOT NULL,
                text         TEXT NOT NULL,
                use_count    INTEGER DEFAULT 0,
                note_type    TEXT DEFAULT 'agreement',
                created_at   TIMESTAMPTZ DEFAULT NOW(),
                last_used_at TIMESTAMPTZ
            );
        """
    else:
        _ddl = """
            CREATE TABLE IF NOT EXISTS context_notes (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                label        TEXT NOT NULL,
                text         TEXT NOT NULL,
                use_count    INTEGER DEFAULT 0,
                created_at   TEXT DEFAULT (datetime('now')),
                last_used_at TEXT
            );
        """
    with fees_db() as conn:
        conn.executescript(_ddl)
        if not _PG:
            try:
                conn.execute("ALTER TABLE context_notes ADD COLUMN note_type TEXT DEFAULT 'agreement'")
            except Exception:
                pass
        else:
            try:
                conn.execute("ALTER TABLE context_notes ADD COLUMN IF NOT EXISTS note_type TEXT DEFAULT 'agreement'")
            except Exception:
                pass


def _ensure_amendment_tables():
    """Create amendment history and upload-cache tables."""
    if _PG:
        _ddl = """
            CREATE TABLE IF NOT EXISTS amendment_upload_cache (
                token       TEXT PRIMARY KEY,
                filename    TEXT,
                file_data   BYTEA,
                created_at  TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS psp_amendments (
                id              SERIAL PRIMARY KEY,
                agreement_id    INTEGER NOT NULL REFERENCES psp_agreements(id),
                addendum_date   TEXT,
                applied_at      TIMESTAMPTZ DEFAULT NOW(),
                filename        TEXT,
                file_data       BYTEA,
                notes           TEXT,
                changes_applied INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS psp_amendment_changes (
                id              SERIAL PRIMARY KEY,
                amendment_id    INTEGER NOT NULL REFERENCES psp_amendments(id),
                action          TEXT NOT NULL,
                fee_rule_id     INTEGER,
                old_payment_method  TEXT, old_fee_type TEXT, old_country TEXT,
                old_fee_kind        TEXT, old_pct_rate DOUBLE PRECISION,
                old_fixed_amount    DOUBLE PRECISION, old_fixed_currency TEXT, old_description TEXT,
                new_payment_method  TEXT, new_fee_type TEXT, new_country TEXT,
                new_fee_kind        TEXT, new_pct_rate DOUBLE PRECISION,
                new_fixed_amount    DOUBLE PRECISION, new_fixed_currency TEXT, new_description TEXT
            );
        """
    else:
        _ddl = """
            CREATE TABLE IF NOT EXISTS amendment_upload_cache (
                token       TEXT PRIMARY KEY,
                filename    TEXT,
                file_data   BLOB,
                created_at  TEXT DEFAULT (datetime('now'))
            );
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
        """

    with fees_db() as conn:
        conn.executescript(_ddl)
        # SQLite: migrate agr_filename / agr_file_data onto psp_agreements
        if not _PG:
            for col in ("agr_filename TEXT", "agr_file_data BLOB"):
                try:
                    conn.execute(f"ALTER TABLE psp_agreements ADD COLUMN {col}")
                except Exception:
                    pass
        # Expire old cache entries (> 24 h)
        expire_sql = (
            "DELETE FROM amendment_upload_cache WHERE created_at < NOW() - INTERVAL '1 day'"
            if _PG else
            "DELETE FROM amendment_upload_cache WHERE created_at < datetime('now', '-1 day')"
        )
        conn.execute(expire_sql)


# --- Amendment history ---

def cache_upload(token: str, filename: str, file_data: bytes):
    with fees_db() as conn:
        if _PG:
            conn.execute(
                "INSERT INTO amendment_upload_cache (token, filename, file_data) VALUES (?, ?, ?)"
                " ON CONFLICT (token) DO UPDATE SET filename=EXCLUDED.filename, file_data=EXCLUDED.file_data",
                (token, filename, file_data)
            )
        else:
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
    """Create core fee tables (SQLite or PostgreSQL depending on FEES_MODE)."""
    if _PG:
        _ddl = """
            CREATE TABLE IF NOT EXISTS psp_agreements (
                id               SERIAL PRIMARY KEY,
                psp_name         TEXT NOT NULL,
                provider_name    TEXT,
                agreement_entity TEXT,
                agreement_date   TEXT,
                addendum_date    TEXT,
                auto_settlement  INTEGER DEFAULT 0,
                settlement_bank  TEXT,
                active           INTEGER DEFAULT 1,
                agr_filename     TEXT,
                agr_file_data    BYTEA,
                created_at       TIMESTAMPTZ DEFAULT NOW(),
                updated_at       TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS psp_fee_rules (
                id               SERIAL PRIMARY KEY,
                agreement_id     INTEGER NOT NULL REFERENCES psp_agreements(id) ON DELETE CASCADE,
                payment_method   TEXT,
                fee_type         TEXT NOT NULL,
                country          TEXT DEFAULT 'GLOBAL',
                sub_provider     TEXT,
                fee_kind         TEXT NOT NULL CHECK (fee_kind IN ('percentage','fixed','fixed_plus_pct','tiered')),
                pct_rate         DOUBLE PRECISION,
                fixed_amount     DOUBLE PRECISION,
                fixed_currency   TEXT,
                description      TEXT,
                created_at       TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS psp_fee_tiers (
                id           SERIAL PRIMARY KEY,
                fee_rule_id  INTEGER NOT NULL REFERENCES psp_fee_rules(id) ON DELETE CASCADE,
                volume_from  DOUBLE PRECISION NOT NULL DEFAULT 0,
                volume_to    DOUBLE PRECISION,
                pct_rate     DOUBLE PRECISION NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_fee_rules_agreement ON psp_fee_rules(agreement_id);
            CREATE INDEX IF NOT EXISTS idx_fee_tiers_rule ON psp_fee_tiers(fee_rule_id);
            CREATE TABLE IF NOT EXISTS agreement_entities (
                id   SERIAL PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            );
        """
    else:
        _ddl = """
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
        """

    with fees_db() as conn:
        conn.executescript(_ddl)
        # SQLite migrations for agr_filename/agr_file_data (PG DDL already includes them)
        if not _PG:
            for col in ("agr_filename TEXT", "agr_file_data BLOB"):
                try:
                    conn.execute(f"ALTER TABLE psp_agreements ADD COLUMN {col}")
                except Exception:
                    pass
        # Seed default entities if table is empty
        existing = conn.execute("SELECT COUNT(*) FROM agreement_entities").fetchone()[0]
        if existing == 0:
            if _PG:
                for name in ("CMT PROCESSING LTD", "GCMT GROUP LTD"):
                    conn.execute(
                        "INSERT INTO agreement_entities (name) VALUES (%s) ON CONFLICT DO NOTHING",
                        (name,)
                    )
            else:
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


def purge_agreement(psp_id):
    """Permanently delete a terminated agreement and all its fee rules."""
    with fees_db() as conn:
        conn.execute("DELETE FROM psp_fee_rules WHERE agreement_id = ?", (psp_id,))
        conn.execute("DELETE FROM psp_agreements WHERE id = ? AND active = 0", (psp_id,))


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


# ═══════════════════════════════════════════════════════════════════════════════
# BANK STATEMENTS MODULE
# ═══════════════════════════════════════════════════════════════════════════════

def _ensure_bank_tables():
    """Create bank_accounts, bank_statements, bank_transactions tables."""
    if _PG:
        _ddl = """
            CREATE TABLE IF NOT EXISTS bank_accounts (
                id           SERIAL PRIMARY KEY,
                bank_name    TEXT NOT NULL,
                account_number TEXT NOT NULL,
                account_label  TEXT,
                currency     TEXT NOT NULL DEFAULT 'USD',
                entity       TEXT,
                active       INTEGER DEFAULT 1,
                created_at   TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS bank_statements (
                id              SERIAL PRIMARY KEY,
                bank_account_id INTEGER NOT NULL REFERENCES bank_accounts(id),
                period_start    DATE,
                period_end      DATE,
                filename        TEXT,
                file_data       BYTEA,
                opening_balance DOUBLE PRECISION,
                closing_balance DOUBLE PRECISION,
                total_credits   DOUBLE PRECISION,
                total_debits    DOUBLE PRECISION,
                tx_count        INTEGER DEFAULT 0,
                source          TEXT DEFAULT 'upload',
                uploaded_at     TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS bank_transactions (
                id              SERIAL PRIMARY KEY,
                statement_id    INTEGER NOT NULL REFERENCES bank_statements(id) ON DELETE CASCADE,
                bank_account_id INTEGER NOT NULL REFERENCES bank_accounts(id),
                tx_date         DATE NOT NULL,
                value_date      DATE,
                amount          DOUBLE PRECISION NOT NULL,
                balance         DOUBLE PRECISION,
                currency        TEXT,
                reference       TEXT,
                description     TEXT,
                tx_type         TEXT,
                counterparty    TEXT,
                matched_crm_id  INTEGER,
                matched_praxis_tid TEXT,
                match_confidence DOUBLE PRECISION,
                match_status    TEXT DEFAULT 'unmatched'
            );
            CREATE INDEX IF NOT EXISTS idx_bank_tx_date ON bank_transactions(tx_date);
            CREATE INDEX IF NOT EXISTS idx_bank_tx_account ON bank_transactions(bank_account_id);
            CREATE INDEX IF NOT EXISTS idx_bank_tx_match ON bank_transactions(match_status);
        """
    else:
        _ddl = """
            CREATE TABLE IF NOT EXISTS bank_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bank_name TEXT NOT NULL,
                account_number TEXT NOT NULL,
                account_label TEXT,
                currency TEXT NOT NULL DEFAULT 'USD',
                entity TEXT,
                active INTEGER DEFAULT 1,
                created_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS bank_statements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bank_account_id INTEGER NOT NULL REFERENCES bank_accounts(id),
                period_start DATE,
                period_end DATE,
                filename TEXT,
                file_data BLOB,
                opening_balance REAL,
                closing_balance REAL,
                total_credits REAL,
                total_debits REAL,
                tx_count INTEGER DEFAULT 0,
                source TEXT DEFAULT 'upload',
                uploaded_at TEXT DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS bank_transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                statement_id INTEGER NOT NULL REFERENCES bank_statements(id) ON DELETE CASCADE,
                bank_account_id INTEGER NOT NULL REFERENCES bank_accounts(id),
                tx_date DATE NOT NULL,
                value_date DATE,
                amount REAL NOT NULL,
                balance REAL,
                currency TEXT,
                reference TEXT,
                description TEXT,
                tx_type TEXT,
                counterparty TEXT,
                matched_crm_id INTEGER,
                matched_praxis_tid TEXT,
                match_confidence REAL,
                match_status TEXT DEFAULT 'unmatched'
            );
            CREATE INDEX IF NOT EXISTS idx_bank_tx_date ON bank_transactions(tx_date);
            CREATE INDEX IF NOT EXISTS idx_bank_tx_account ON bank_transactions(bank_account_id);
            CREATE INDEX IF NOT EXISTS idx_bank_tx_match ON bank_transactions(match_status);
        """
    with fees_db() as conn:
        conn.executescript(_ddl)
        # Migration: add active column to bank_statements if missing
        if not _PG:
            try:
                conn.execute("ALTER TABLE bank_statements ADD COLUMN active INTEGER DEFAULT 1")
                conn.execute("UPDATE bank_statements SET active = 1 WHERE active IS NULL")
            except Exception:
                pass
            # Migration: add matched_login to bank_transactions (avoids CRM query in recon summary)
            try:
                conn.execute("ALTER TABLE bank_transactions ADD COLUMN matched_login INTEGER")
            except Exception:
                pass
        else:
            try:
                conn.execute("ALTER TABLE bank_statements ADD COLUMN IF NOT EXISTS active INTEGER DEFAULT 1")
                conn.execute("UPDATE bank_statements SET active = 1 WHERE active IS NULL")
            except Exception:
                pass
            try:
                conn.execute("ALTER TABLE bank_transactions ADD COLUMN IF NOT EXISTS matched_login INTEGER")
            except Exception:
                pass


# --- Bank Accounts ---

def get_bank_accounts(active_only=True):
    with fees_db() as conn:
        if active_only:
            rows = conn.execute(
                "SELECT * FROM bank_accounts WHERE active = 1 ORDER BY bank_name, account_number"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM bank_accounts ORDER BY active DESC, bank_name, account_number"
            ).fetchall()
        return [dict(r) for r in rows]


def get_bank_account(account_id):
    with fees_db() as conn:
        row = conn.execute("SELECT * FROM bank_accounts WHERE id = ?", (account_id,)).fetchone()
        return dict(row) if row else None


def create_bank_account(data):
    with fees_db() as conn:
        cur = conn.execute("""
            INSERT INTO bank_accounts (bank_name, account_number, account_label, currency, entity)
            VALUES (?, ?, ?, ?, ?)
        """, (data["bank_name"], data["account_number"],
              data.get("account_label", ""), data.get("currency", "USD"),
              data.get("entity", "")))
        return cur.lastrowid


def update_bank_account(account_id, data):
    with fees_db() as conn:
        conn.execute("""
            UPDATE bank_accounts
            SET bank_name=?, account_number=?, account_label=?, currency=?, entity=?
            WHERE id=?
        """, (data["bank_name"], data["account_number"],
              data.get("account_label", ""), data.get("currency", "USD"),
              data.get("entity", ""), account_id))


def delete_bank_account(account_id):
    """Soft-delete: moves to Historical Accounts."""
    with fees_db() as conn:
        conn.execute("UPDATE bank_accounts SET active = 0 WHERE id = ?", (account_id,))


def restore_bank_account(account_id):
    with fees_db() as conn:
        conn.execute("UPDATE bank_accounts SET active = 1 WHERE id = ?", (account_id,))


def purge_bank_account(account_id):
    """Permanently delete account and all its statements/transactions."""
    with fees_db() as conn:
        conn.execute("""
            DELETE FROM bank_transactions WHERE bank_account_id = ?
        """, (account_id,))
        conn.execute("DELETE FROM bank_statements WHERE bank_account_id = ?", (account_id,))
        conn.execute("DELETE FROM bank_accounts WHERE id = ?", (account_id,))


def get_historical_accounts():
    with fees_db() as conn:
        rows = conn.execute(
            "SELECT * FROM bank_accounts WHERE active = 0 ORDER BY bank_name, account_number"
        ).fetchall()
        return [dict(r) for r in rows]


# --- Bank Statements ---

def get_bank_statements(bank_account_id=None, limit=200):
    with fees_db() as conn:
        if bank_account_id:
            rows = conn.execute("""
                SELECT s.*, a.bank_name, a.account_number, a.account_label, a.currency AS acct_currency
                FROM bank_statements s
                JOIN bank_accounts a ON a.id = s.bank_account_id
                WHERE s.bank_account_id = ? AND (s.active IS NULL OR s.active = 1)
                ORDER BY s.period_end DESC, s.uploaded_at DESC
                LIMIT ?
            """, (bank_account_id, limit)).fetchall()
        else:
            rows = conn.execute("""
                SELECT s.*, a.bank_name, a.account_number, a.account_label, a.currency AS acct_currency
                FROM bank_statements s
                JOIN bank_accounts a ON a.id = s.bank_account_id
                WHERE s.active IS NULL OR s.active = 1
                ORDER BY s.uploaded_at DESC
                LIMIT ?
            """, (limit,)).fetchall()
        return [dict(r) for r in rows]


def get_historical_statements(limit=200):
    with fees_db() as conn:
        rows = conn.execute("""
            SELECT s.*, a.bank_name, a.account_number, a.account_label, a.currency AS acct_currency
            FROM bank_statements s
            JOIN bank_accounts a ON a.id = s.bank_account_id
            WHERE s.active = 0
            ORDER BY s.uploaded_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]


def get_bank_statement(statement_id):
    with fees_db() as conn:
        row = conn.execute("""
            SELECT s.*, a.bank_name, a.account_number, a.account_label, a.currency AS acct_currency
            FROM bank_statements s
            JOIN bank_accounts a ON a.id = s.bank_account_id
            WHERE s.id = ?
        """, (statement_id,)).fetchone()
        return dict(row) if row else None


def create_bank_statement(data, file_data=None):
    """Create a bank statement record and its transactions.
    data = {bank_account_id, period_start, period_end, filename, opening_balance,
            closing_balance, total_credits, total_debits, source, transactions: [...]}
    """
    with fees_db() as conn:
        cur = conn.execute("""
            INSERT INTO bank_statements
                (bank_account_id, period_start, period_end, filename, file_data,
                 opening_balance, closing_balance, total_credits, total_debits,
                 tx_count, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data["bank_account_id"],
            data.get("period_start"),
            data.get("period_end"),
            data.get("filename"),
            file_data,
            data.get("opening_balance"),
            data.get("closing_balance"),
            data.get("total_credits"),
            data.get("total_debits"),
            len(data.get("transactions", [])),
            data.get("source", "upload"),
        ))
        stmt_id = cur.lastrowid

        for tx in data.get("transactions", []):
            conn.execute("""
                INSERT INTO bank_transactions
                    (statement_id, bank_account_id, tx_date, value_date, amount,
                     balance, currency, reference, description, tx_type, counterparty)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                stmt_id,
                data["bank_account_id"],
                tx["date"],
                tx.get("value_date"),
                tx["amount"],
                tx.get("balance"),
                tx.get("currency"),
                tx.get("reference"),
                tx.get("description"),
                tx.get("tx_type", "other"),
                tx.get("counterparty"),
            ))
        return stmt_id


def delete_bank_statement(statement_id):
    """Soft-delete: moves to Historical Statements."""
    with fees_db() as conn:
        conn.execute("UPDATE bank_statements SET active = 0 WHERE id = ?", (statement_id,))


def restore_bank_statement(statement_id):
    with fees_db() as conn:
        conn.execute("UPDATE bank_statements SET active = 1 WHERE id = ?", (statement_id,))


def purge_bank_statement(statement_id):
    """Permanently delete statement and all its transactions."""
    with fees_db() as conn:
        conn.execute("DELETE FROM bank_transactions WHERE statement_id = ?", (statement_id,))
        conn.execute("DELETE FROM bank_statements WHERE id = ?", (statement_id,))


def get_bank_transactions(statement_id):
    with fees_db() as conn:
        rows = conn.execute("""
            SELECT * FROM bank_transactions
            WHERE statement_id = ?
            ORDER BY tx_date, id
        """, (statement_id,)).fetchall()
        return [dict(r) for r in rows]


def get_bank_tx_statement_id(tx_id):
    with fees_db() as conn:
        row = conn.execute(
            "SELECT statement_id FROM bank_transactions WHERE id=?", (tx_id,)
        ).fetchone()
        return row[0] if row else None


def update_bank_transaction(tx_id, data):
    with fees_db() as conn:
        conn.execute("""
            UPDATE bank_transactions
            SET tx_date=?, value_date=?, amount=?, balance=?, currency=?,
                reference=?, description=?, tx_type=?
            WHERE id=?
        """, (data.get("tx_date"), data.get("value_date") or None,
              data["amount"], data.get("balance"), data.get("currency"),
              data.get("reference"), data.get("description"),
              data.get("tx_type", "other"), tx_id))
        stmt_row = conn.execute(
            "SELECT statement_id FROM bank_transactions WHERE id=?", (tx_id,)
        ).fetchone()
        if stmt_row:
            _recompute_statement_totals(conn, stmt_row[0])


def delete_bank_transaction(tx_id):
    with fees_db() as conn:
        stmt_row = conn.execute(
            "SELECT statement_id FROM bank_transactions WHERE id=?", (tx_id,)
        ).fetchone()
        conn.execute("DELETE FROM bank_transactions WHERE id=?", (tx_id,))
        if stmt_row:
            _recompute_statement_totals(conn, stmt_row[0])


def _recompute_statement_totals(conn, statement_id):
    r = conn.execute("""
        SELECT
            COUNT(*) AS cnt,
            SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) AS credits,
            SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END) AS debits
        FROM bank_transactions WHERE statement_id=?
    """, (statement_id,)).fetchone()
    conn.execute("""
        UPDATE bank_statements
        SET tx_count=?, total_credits=?, total_debits=?
        WHERE id=?
    """, (r[0], r[1] or 0, r[2] or 0, statement_id))


# ── Bank ↔ CRM Reconciliation Matching ──────────────────────────────────────

def bank_transactions_for_period(year: int, month: int) -> list:
    """All bank transactions in the given month, across all active bank accounts."""
    from datetime import date as _date
    start = _date(year, month, 1).isoformat()
    end   = _date(year + (month // 12), (month % 12) + 1, 1).isoformat()
    key = f"bank_txns:{year}:{month}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached
    with fees_db() as conn:
        rows = conn.execute("""
            SELECT t.*, a.bank_name, a.account_number, a.currency AS acct_currency
            FROM bank_transactions t
            JOIN bank_statements s ON s.id = t.statement_id
            JOIN bank_accounts a ON a.id = t.bank_account_id
            WHERE t.tx_date >= ? AND t.tx_date < ?
              AND (s.active IS NULL OR s.active = 1)
            ORDER BY t.tx_date, t.id
        """, (start, end)).fetchall()
    result = [dict(r) for r in rows]
    _cache_set(key, result)
    return result


def crm_cash_transactions_individual(year: int, month: int) -> list:
    """Individual CRM cash transactions for matching (not grouped)."""
    from datetime import date as _date
    start = _date(year, month, 1).isoformat()
    end   = _date(year + (month // 12), (month % 12) + 1, 1).isoformat()
    key = f"crm_individual:{year}:{month}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached
    try:
        def _fetch():
            with crm() as cur:
                cur.execute("""
                    SELECT login, usdamount, CAST(confirmation_time AS DATE) AS conf_date,
                           payment_method, payment_processor,
                           psp_transaction_id, transactionid,
                           transactiontype, transactionapproval
                    FROM report.vtiger_mttransactions
                    WHERE confirmation_time >= %s AND confirmation_time < %s
                      AND transactionapproval = 'Approved'
                      AND (deleted IS NULL OR deleted = 0)
                """, (start, end))
                return [dict(r) for r in cur.fetchall()]
        raw = _db_retry(_fetch)
    except Exception:
        raw = []
    # Filter to cash-only
    result = [r for r in raw if _is_cash(r.get("payment_method", ""), r.get("transactiontype", ""))]
    _cache_set(key, result)
    return result


def auto_match_bank_to_crm(year: int, month: int) -> dict:
    """4-pass matching of bank transactions to CRM transactions.

    Updates bank_transactions rows in-place (matched_crm_id, match_confidence,
    match_status). Returns stats dict.
    """
    import re
    from datetime import datetime as _dt, timedelta as _td

    bank_txns = bank_transactions_for_period(year, month)
    crm_txns  = crm_cash_transactions_individual(year, month)

    if not bank_txns or not crm_txns:
        return {"matched": 0, "unmatched": len(bank_txns), "by_pass": {1: 0, 2: 0, 3: 0, 4: 0}}

    # Build CRM lookup structures
    crm_by_psp_ref: dict = {}   # psp_transaction_id → [crm_tx, ...]
    crm_by_login: dict = {}     # login → [crm_tx, ...]
    crm_matched_ids: set = set()

    for c in crm_txns:
        ref = str(c.get("psp_transaction_id") or "").strip()
        if ref:
            crm_by_psp_ref.setdefault(ref, []).append(c)
        login = c.get("login")
        if login:
            crm_by_login.setdefault(int(login), []).append(c)

    def _parse_date(d):
        if isinstance(d, str):
            try:
                return _dt.strptime(d[:10], "%Y-%m-%d")
            except (ValueError, TypeError):
                return None
        if hasattr(d, "year"):
            return _dt(d.year, d.month, d.day)
        return None

    stats = {"matched": 0, "unmatched": 0, "by_pass": {1: 0, 2: 0, 3: 0, 4: 0}}
    matches: list = []   # (bank_tx_id, crm_transactionid, confidence, pass_num)

    # Filter to unmatched deposit bank txns only (credits)
    unmatched = [b for b in bank_txns if (b.get("match_status") or "unmatched") == "unmatched"]

    # ── Pass 1: Reference exact match ──────────────────────────────────────
    still_unmatched = []
    for b in unmatched:
        b_ref = str(b.get("reference") or "").strip()
        if b_ref and b_ref in crm_by_psp_ref:
            candidates = [c for c in crm_by_psp_ref[b_ref]
                          if c["transactionid"] not in crm_matched_ids]
            if len(candidates) == 1:
                matches.append((b["id"], candidates[0]["transactionid"], 0.95, 1))
                crm_matched_ids.add(candidates[0]["transactionid"])
                stats["by_pass"][1] += 1
                continue
        still_unmatched.append(b)

    # ── Pass 2: Reference substring ────────────────────────────────────────
    unmatched2 = []
    for b in still_unmatched:
        b_ref = str(b.get("reference") or "").strip()
        b_desc = str(b.get("description") or "")
        found = None
        for c in crm_txns:
            if c["transactionid"] in crm_matched_ids:
                continue
            c_ref = str(c.get("psp_transaction_id") or "").strip()
            if not c_ref or len(c_ref) < 5:
                continue
            if (b_ref and c_ref in b_ref) or (b_ref and b_ref in c_ref) or (c_ref in b_desc):
                if found is None:
                    found = c
                else:
                    found = None  # ambiguous — skip
                    break
        if found:
            matches.append((b["id"], found["transactionid"], 0.85, 2))
            crm_matched_ids.add(found["transactionid"])
            stats["by_pass"][2] += 1
        else:
            unmatched2.append(b)

    # ── Pass 3: Login number in bank description ───────────────────────────
    unmatched3 = []
    login_re = re.compile(r"\b(1[234]\d{7})\b")   # MT5 logins: 130M–149M range
    for b in unmatched2:
        b_desc = str(b.get("reference") or "") + " " + str(b.get("description") or "")
        login_matches = login_re.findall(b_desc)
        if not login_matches:
            unmatched3.append(b)
            continue
        b_date = _parse_date(b.get("tx_date"))
        b_amt  = abs(b.get("amount") or 0)
        found = None
        for login_str in login_matches:
            login = int(login_str)
            for c in crm_by_login.get(login, []):
                if c["transactionid"] in crm_matched_ids:
                    continue
                c_amt = abs(c.get("usdamount") or 0)
                c_date = _parse_date(c.get("conf_date"))
                if c_date and b_date and abs((b_date - c_date).days) <= 3 and abs(b_amt - c_amt) < 2:
                    if found is None:
                        found = c
                    else:
                        found = None
                        break
            if found is None and login_matches:
                break
        if found:
            matches.append((b["id"], found["transactionid"], 0.80, 3))
            crm_matched_ids.add(found["transactionid"])
            stats["by_pass"][3] += 1
        else:
            unmatched3.append(b)

    # ── Pass 4: Amount + date proximity (wire transfers only) ──────────────
    for b in unmatched3:
        b_amt  = b.get("amount") or 0
        if b_amt <= 50:  # skip small amounts / fees
            continue
        b_date = _parse_date(b.get("tx_date"))
        if not b_date:
            continue
        found = None
        for c in crm_txns:
            if c["transactionid"] in crm_matched_ids:
                continue
            pm = (c.get("payment_method") or "").lower()
            if "wire" not in pm and "external" not in pm:
                continue
            c_amt = abs(c.get("usdamount") or 0)
            c_date = _parse_date(c.get("conf_date"))
            if c_date and abs((b_date - c_date).days) <= 3 and abs(abs(b_amt) - c_amt) < 2:
                if found is None:
                    found = c
                else:
                    found = None  # ambiguous
                    break
        if found:
            matches.append((b["id"], found["transactionid"], 0.70, 4))
            crm_matched_ids.add(found["transactionid"])
            stats["by_pass"][4] += 1

    # ── Persist matches — also store login so bank_recon_summary avoids CRM query ──
    # Build crm_id → login map from the CRM txns we already have in memory
    crm_id_to_login = {}
    for c in crm_txns:
        tid = c.get("transactionid")
        lg  = c.get("login")
        if tid and lg:
            crm_id_to_login[tid] = int(lg)

    if matches:
        with fees_db() as conn:
            for bank_tx_id, crm_tx_id, confidence, _ in matches:
                login_val = crm_id_to_login.get(crm_tx_id)
                conn.execute("""
                    UPDATE bank_transactions
                    SET matched_crm_id = ?, match_confidence = ?,
                        match_status = 'matched', matched_login = ?
                    WHERE id = ?
                """, (crm_tx_id, confidence, login_val, bank_tx_id))

    stats["matched"]   = len(matches)
    stats["unmatched"]  = len(bank_txns) - len(matches)

    # Invalidate caches that depend on bank match state
    _CACHE.pop(f"bank_txns:{year}:{month}", None)
    _CACHE.pop(f"bank_recon:{year}:{month}", None)

    return stats


def bank_recon_summary(year: int, month: int) -> dict:
    """Aggregate matched bank transactions per MT5 login.

    Returns {login: {"bank_deposits": X, "bank_withdrawals": Y,
                     "bank_net": Z, "bank_matched": N}}
    """
    key = f"bank_recon:{year}:{month}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached

    # Get all matched bank txns for the period
    # matched_login is stored during auto_match_bank_to_crm — no CRM query needed here
    bank_txns = bank_transactions_for_period(year, month)
    matched = [b for b in bank_txns if b.get("match_status") == "matched"
               and (b.get("matched_login") or b.get("matched_crm_id"))]

    if not matched:
        result = {}
        _cache_set(key, result)
        return result

    # Aggregate per login using stored matched_login
    result: dict = {}
    for b in matched:
        login = b.get("matched_login")
        if not login:
            continue
        login = int(login)
        if login not in result:
            result[login] = {"bank_deposits": 0, "bank_withdrawals": 0, "bank_net": 0, "bank_matched": 0}
        amt = b.get("amount") or 0
        if amt > 0:
            result[login]["bank_deposits"]  += amt
        else:
            result[login]["bank_withdrawals"] += amt
        result[login]["bank_net"]     += amt
        result[login]["bank_matched"] += 1

    # Round
    for v in result.values():
        v["bank_deposits"]    = round(v["bank_deposits"], 2)
        v["bank_withdrawals"] = round(v["bank_withdrawals"], 2)
        v["bank_net"]         = round(v["bank_net"], 2)

    _cache_set(key, result)
    return result


def get_bank_statement_file(statement_id):
    """Return (filename, file_data) for download."""
    with fees_db() as conn:
        row = conn.execute(
            "SELECT filename, file_data FROM bank_statements WHERE id = ?",
            (statement_id,)
        ).fetchone()
        if row and row["file_data"]:
            return row["filename"], row["file_data"]
        return None, None


# ═══════════════════════════════════════════════════════════════════════════════
# OPERATORS DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════

_TTL_OPERATORS = 1800   # 30 minutes

def operator_list() -> list:
    """All CRM operators from vtiger_users. Cached 30 min."""
    key = "operator_list"
    cached = _cache_get(key, _TTL_OPERATORS)
    if cached is not None:
        return cached
    try:
        def _fetch():
            with crm() as cur:
                cur.execute("""
                    SELECT
                        id, user_name, first_name, last_name, email,
                        department, office, status, position,
                        fax AS role_name,
                        last_login, phone AS desk_label
                    FROM report.vtiger_users
                    ORDER BY
                        CASE WHEN status='Active' THEN 0 ELSE 1 END,
                        department, office, first_name
                """)
                return [dict(r) for r in cur.fetchall()]
        rows = _db_retry(_fetch)
    except Exception:
        rows = []
    if rows:
        _cache_set(key, rows)
    return rows


def operator_client_stats() -> dict:
    """Per-operator client counts + deposit volumes from vtiger_account. Cached 30 min."""
    key = "operator_client_stats"
    cached = _cache_get(key, _TTL_OPERATORS)
    if cached is not None:
        return cached
    try:
        def _fetch():
            with crm() as cur:
                cur.execute("""
                    SELECT
                        assigned_to AS operator_id,
                        COUNT(*) AS total_clients,
                        COUNT(CASE WHEN countdeposits > 0 THEN 1 END) AS funded_clients,
                        SUM(CAST(ISNULL(total_deposit, 0) AS FLOAT)) AS total_deposit_volume,
                        SUM(CAST(ISNULL(net_deposit, 0) AS FLOAT)) AS net_deposit_volume
                    FROM report.vtiger_account WITH (NOLOCK)
                    WHERE assigned_to IS NOT NULL AND assigned_to > 0
                    GROUP BY assigned_to
                """)
                return {r["operator_id"]: dict(r) for r in cur.fetchall()}
        result = _db_retry(_fetch)
    except Exception:
        result = {}
    if result:
        _cache_set(key, result)
    return result


def operator_ftd_stats() -> dict:
    """Per-operator first-time-deposit counts from v_agent_desk_dealing_tableau. Cached 30 min."""
    key = "operator_ftd_stats"
    cached = _cache_get(key, _TTL_OPERATORS)
    if cached is not None:
        return cached
    try:
        def _fetch():
            with crm() as cur:
                cur.execute("""
                    SELECT
                        assigned_to AS operator_id,
                        COUNT(*) AS ftd_count,
                        SUM(amount) AS ftd_volume
                    FROM dbo.v_agent_desk_dealing_tableau WITH (NOLOCK)
                    WHERE assigned_to IS NOT NULL AND assigned_to > 0
                    GROUP BY assigned_to
                """)
                return {r["operator_id"]: dict(r) for r in cur.fetchall()}
        result = _db_retry(_fetch)
    except Exception:
        result = {}
    if result:
        _cache_set(key, result)
    return result


# ── Signals (CMTrading_Signals_EA ingestion + GUI) ─────────────────────────
# Dedicated Postgres (signals_postgres); no SQLite mode, unlike fees_db().

_SIGNAL_COLUMNS = [
    "symbol", "timeframe", "direction", "entry_price", "sl_price", "tp_price",
    "rsi_value", "status", "signal_time",
    "resistance_1", "resistance_2", "resistance_3",
    "support_1", "support_2", "support_3",
]


def ensure_signal_tables():
    """Idempotent DDL for the signals table — called once at app startup."""
    ddl = """
        CREATE TABLE IF NOT EXISTS signals (
            id            SERIAL PRIMARY KEY,
            symbol        TEXT NOT NULL,
            timeframe     TEXT NOT NULL,
            direction     TEXT NOT NULL,
            entry_price   DOUBLE PRECISION NOT NULL,
            sl_price      DOUBLE PRECISION NOT NULL,
            tp_price      DOUBLE PRECISION NOT NULL,
            rsi_value     DOUBLE PRECISION,
            status        TEXT NOT NULL DEFAULT 'ACTIVE',
            signal_time   TIMESTAMPTZ NOT NULL,
            resistance_1  DOUBLE PRECISION,
            resistance_2  DOUBLE PRECISION,
            resistance_3  DOUBLE PRECISION,
            support_1     DOUBLE PRECISION,
            support_2     DOUBLE PRECISION,
            support_3     DOUBLE PRECISION,
            outcome       TEXT,
            close_price   DOUBLE PRECISION,
            close_time    TIMESTAMPTZ,
            updated_at    TIMESTAMPTZ,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_signals_status     ON signals (status);
        CREATE INDEX IF NOT EXISTS idx_signals_symbol_tf  ON signals (symbol, timeframe);
    """
    with signals() as cur:
        cur.execute(ddl)


def insert_signal(data: dict) -> int:
    """Insert a row from the EA's POST /rest/v1/signals body. Returns the new id.

    resistance_1/2/3 and support_1/2/3 are optional in the EA's payload — only
    the keys BuildSRLevels() actually found (0-3 each) are sent — so they're
    read with a None default rather than required.
    """
    values = [data.get(c) for c in _SIGNAL_COLUMNS]
    placeholders = ", ".join(["%s"] * len(_SIGNAL_COLUMNS))
    with signals() as cur:
        cur.execute(
            f"INSERT INTO signals ({', '.join(_SIGNAL_COLUMNS)}) VALUES ({placeholders}) RETURNING id",
            values,
        )
        return cur.fetchone()["id"]


def close_signal(signal_id: int, data: dict) -> bool:
    """Apply the EA's PATCH /rest/v1/signals?id=eq.<id> body. Returns True if a row matched."""
    cols = [c for c in ("status", "outcome", "close_price", "close_time", "updated_at") if c in data]
    if not cols:
        return False
    set_clause = ", ".join(f"{c} = %s" for c in cols)
    values = [data[c] for c in cols] + [signal_id]
    with signals() as cur:
        cur.execute(f"UPDATE signals SET {set_clause} WHERE id = %s", values)
        return cur.rowcount > 0


def list_active_signals(status: str = "ACTIVE") -> list:
    """Restart-safe reload for the EA's GET /rest/v1/signals?status=eq.<status>."""
    with signals() as cur:
        cur.execute("""
            SELECT id, symbol, timeframe, direction, entry_price, sl_price, tp_price
            FROM signals WHERE status = %s
            ORDER BY id
        """, (status,))
        return [dict(r) for r in cur.fetchall()]


# Planned reward:risk of a setup, by direction. NULLIF guards a zero/malformed
# risk (→ NULL, excluded by any `>= min_rr`). level=None → the EA framing
# (reward to tp_price, risk to sl_price); level=N → the S/R framing (BUY: target
# resistance_N / stop support_N, SELL inverted). Mirrors the JS effLevels()+R:R.
def _planned_rr_sql(level=None):
    if level is None:
        reward = "(CASE WHEN direction='BUY' THEN (tp_price-entry_price) ELSE (entry_price-tp_price) END)"
        risk   = "(CASE WHEN direction='BUY' THEN (entry_price-sl_price) ELSE (sl_price-entry_price) END)"
    else:
        r, s = f"resistance_{int(level)}", f"support_{int(level)}"
        reward = f"(CASE WHEN direction='BUY' THEN ({r}-entry_price) ELSE (entry_price-{s}) END)"
        risk   = f"(CASE WHEN direction='BUY' THEN (entry_price-{s}) ELSE ({r}-entry_price) END)"
    return f"{reward} / NULLIF({risk}, 0)"

_PLANNED_RR_SQL = _planned_rr_sql()  # EA framing (kept for reference)


def _signal_where(symbol=None, timeframe=None, direction=None, status=None, outcome=None,
                  date_from=None, date_to=None, min_rr=None, rr_level=None):
    """Shared WHERE builder for the signals table — used by both list_signals()
    and get_signal_stats() so the list and the stat cards filter identically.
    rr_level selects which framing the min_rr filter uses (None = EA, N = S/R)."""
    where, params = [], []
    if symbol:
        if isinstance(symbol, (list, tuple)):       # multi-select: symbol IN (...)
            where.append("symbol = ANY(%s)");       params.append(list(symbol))
        else:
            where.append("symbol = %s");            params.append(symbol)
    if timeframe: where.append("timeframe = %s");   params.append(timeframe)
    if direction: where.append("direction = %s");    params.append(direction)
    if status:    where.append("status = %s");      params.append(status)
    if outcome:   where.append("outcome = %s");      params.append(outcome)
    if date_from: where.append("signal_time >= %s"); params.append(date_from)
    if date_to:   where.append("signal_time < %s");  params.append(date_to)
    if min_rr is not None:
        where.append(f"({_planned_rr_sql(rr_level)}) >= %s");  params.append(min_rr)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    return where_sql, params


_SR_HORIZON_DAYS = 120  # don't backtest signals older than this (bounds the tick fetch)
_SR_TTL = 180           # cache the backtest ~3 min (it costs a few seconds)
_sr_lock = threading.Lock()  # serialize the backtest so the 2s poll can't stampede it


def _sr_target_stop(row, level):
    """(target, stop) prices for a signal at S/R level N, or (None, None) if the
    level's columns are missing. BUY: target=resistance_N (up), stop=support_N
    (down). SELL: target=support_N (down), stop=resistance_N (up)."""
    res = row.get(f"resistance_{level}")
    sup = row.get(f"support_{level}")
    if res is None or sup is None:
        return None, None
    return (res, sup) if row["direction"] == "BUY" else (sup, res)


def sr_outcomes(level: int) -> dict:
    """Backtest every signal at S/R level N against the dealio 30-min feed: which
    of {target, stop} was hit first after signal_time. Returns
    {id: 'WIN'|'LOSS'|'PENDING'|None}. The full backtest costs a few seconds (a
    dealio query per symbol), so it's cached on a pure ~3-min TTL — new signals
    just get backtested at the next refresh, and PENDING ones resolve as ticks
    arrive. A lock serializes computation so the 2s poll can't stampede it.

    Approximate: the feed is a 30-min snapshot, so a level touched and reversed
    within a window is missed. dealio.ticks.lastmodified is a naive UTC wall
    clock, so signal_time (tz-aware) is normalised to naive UTC to compare."""
    key = f"sr_outcomes:{level}"
    cached = _cache_get(key, _SR_TTL)
    if cached is not None:
        return cached
    with _sr_lock:
        cached = _cache_get(key, _SR_TTL)   # another thread may have just filled it
        if cached is not None:
            return cached
        return _compute_sr_outcomes(level, key)


def _compute_sr_outcomes(level: int, key: str) -> dict:
    with signals() as cur:
        cur.execute("""
            SELECT id, symbol, direction, signal_time,
                   resistance_1, resistance_2, resistance_3,
                   support_1, support_2, support_3
            FROM signals
        """)
        rows = [dict(r) for r in cur.fetchall()]
    for r in rows:
        st = r["signal_time"]
        r["_st"] = st.astimezone(timezone.utc).replace(tzinfo=None) if st.tzinfo else st

    horizon = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=_SR_HORIZON_DAYS)
    by_symbol = {}
    for r in rows:
        by_symbol.setdefault(r["symbol"], []).append(r)

    out = {}
    for symbol, sigs in by_symbol.items():
        start = min(r["_st"] for r in sigs)
        if start < horizon:
            start = horizon
        with dealio() as cur:
            cur.execute("""
                SELECT lastmodified, (bid + ask) / 2.0 AS mid
                FROM dealio.ticks
                WHERE symbol = %s AND lastmodified >= %s
                ORDER BY lastmodified
            """, (symbol, start))
            ticks = cur.fetchall()
        times = [t["lastmodified"] for t in ticks]
        mids  = [float(t["mid"]) for t in ticks]

        for r in sigs:
            target, stop = _sr_target_stop(r, level)
            if target is None:
                out[r["id"]] = None
                continue
            is_buy = r["direction"] == "BUY"
            res = "PENDING"
            for j in range(bisect.bisect_left(times, r["_st"]), len(mids)):
                mid = mids[j]
                if is_buy:
                    if mid >= target: res = "WIN";  break
                    if mid <= stop:   res = "LOSS"; break
                else:
                    if mid <= target: res = "WIN";  break
                    if mid >= stop:   res = "LOSS"; break
            out[r["id"]] = res

    _cache_set(key, out)
    return out


def list_signals(symbol=None, timeframe=None, direction=None, status=None, outcome=None,
                  date_from=None, date_to=None, min_rr=None, sr_level=None, page=1, page_size=50) -> dict:
    """Filtered, paginated signal history for the Signals GUI table. When sr_level
    is set, min_rr filters on the S/R framing and each row gets its backtested
    sr_outcome (WIN/LOSS/PENDING)."""
    where_sql, params = _signal_where(symbol, timeframe, direction, status, outcome,
                                      date_from, date_to, min_rr, rr_level=sr_level)
    page = max(page, 1)
    page_size = min(max(page_size, 1), 500)

    with signals() as cur:
        cur.execute(f"SELECT COUNT(*) AS n FROM signals {where_sql}", params)
        total = cur.fetchone()["n"]
        cur.execute(
            f"SELECT * FROM signals {where_sql} ORDER BY signal_time DESC LIMIT %s OFFSET %s",
            params + [page_size, (page - 1) * page_size],
        )
        rows = [dict(r) for r in cur.fetchall()]

    if sr_level is not None:
        outcomes = sr_outcomes(sr_level)
        for r in rows:
            r["sr_outcome"] = outcomes.get(r["id"])

    for r in rows:
        for k in ("signal_time", "close_time", "updated_at", "created_at"):
            if r.get(k) is not None:
                r[k] = r[k].isoformat()

    return {"rows": rows, "total": total, "page": page, "page_size": page_size}


def get_signal_stats(symbol=None, timeframe=None, direction=None, min_rr=None, sr_level=None) -> dict:
    """Win-rate / R:R aggregates for the Signals GUI stat cards, computed over the
    filtered set (symbol / timeframe / direction / min planned-R:R). The status
    toggle deliberately does NOT filter these — win rate is inherently over closed
    trades and the Active count should stay truthful regardless of the list view.
    Also returns the full (unfiltered) symbol list for the picker.

    When sr_level is set, the cards use the backtested S/R framing at that level
    (TP Hits = target hit first, SL Hits = stop hit first, Active = pending)."""
    where_sql, params = _signal_where(symbol=symbol, timeframe=timeframe,
                                      direction=direction, min_rr=min_rr, rr_level=sr_level)

    if sr_level is not None:
        r_col, s_col = f"resistance_{int(sr_level)}", f"support_{int(sr_level)}"
        with signals() as cur:
            cur.execute(f"SELECT id, direction, entry_price, {r_col} AS res, {s_col} AS sup "
                        f"FROM signals {where_sql}", params)
            rows = [dict(r) for r in cur.fetchall()]
            cur.execute("SELECT DISTINCT symbol FROM signals ORDER BY symbol")
            symbols = [r["symbol"] for r in cur.fetchall()]
        outcomes = sr_outcomes(sr_level)
        wins = losses = pending = 0
        rr_ach = []
        for r in rows:
            oc = outcomes.get(r["id"])
            if oc is None:
                continue
            if r["res"] is not None and r["sup"] is not None:
                if r["direction"] == "BUY":
                    reward, risk = r["res"] - r["entry_price"], r["entry_price"] - r["sup"]
                else:
                    reward, risk = r["entry_price"] - r["sup"], r["res"] - r["entry_price"]
                rr = (reward / risk) if risk and risk > 0 else None
            else:
                rr = None
            if oc == "WIN":
                wins += 1
                if rr is not None: rr_ach.append(rr)
            elif oc == "LOSS":
                losses += 1
                rr_ach.append(-1.0)
            else:
                pending += 1
        closed = wins + losses
        overall = {
            "active_count": pending,
            "tp_count":     wins,
            "sl_count":     losses,
            "win_rate":     round(100.0 * wins / closed, 1) if closed else None,
            "avg_rr":       round(sum(rr_ach) / len(rr_ach), 2) if rr_ach else None,
        }
        return {"overall": overall, "symbols": symbols}

    achieved_rr_expr = """
        CASE WHEN status = 'CLOSED' THEN
            (CASE WHEN direction = 'BUY' THEN (close_price - entry_price) ELSE (entry_price - close_price) END)
            / NULLIF(CASE WHEN direction = 'BUY' THEN (entry_price - sl_price) ELSE (sl_price - entry_price) END, 0)
        END
    """
    with signals() as cur:
        cur.execute(f"""
            SELECT
                COUNT(*) FILTER (WHERE status = 'ACTIVE')  AS active_count,
                COUNT(*) FILTER (WHERE outcome = 'TP_HIT') AS tp_count,
                COUNT(*) FILTER (WHERE outcome = 'SL_HIT') AS sl_count,
                AVG({achieved_rr_expr})                     AS avg_rr
            FROM signals {where_sql}
        """, params)
        o = dict(cur.fetchone())
        cur.execute("SELECT DISTINCT symbol FROM signals ORDER BY symbol")
        symbols = [r["symbol"] for r in cur.fetchall()]

    tp = o["tp_count"] or 0
    sl = o["sl_count"] or 0
    closed = tp + sl
    overall = {
        "active_count": o["active_count"] or 0,
        "tp_count":     tp,
        "sl_count":     sl,
        "win_rate":     round(100.0 * tp / closed, 1) if closed else None,
        "avg_rr":       round(float(o["avg_rr"]), 2) if o["avg_rr"] is not None else None,
    }
    return {"overall": overall, "symbols": symbols}


def get_signal_by_id(signal_id: int) -> dict:
    """Single signal row, for the Boxes-view chart endpoint."""
    with signals() as cur:
        cur.execute("SELECT * FROM signals WHERE id = %s", (signal_id,))
        row = cur.fetchone()
        return dict(row) if row else None


# Bucket size per the EA's own timeframe labels — distinct from the FX tab's
# period vocabulary (5m/15m/1h/...), since a signal's chart is anchored on its
# own bar size, not a fixed set of zoom levels.
# dealio.ticks is a periodic snapshot, not a real tick stream — verified
# directly against production: every row lands exactly 1800s (30 min) apart,
# for every symbol checked, at every time of day. Below that interval a bar
# can hold at most one snapshot (open == high == low == close, a flat dash
# no matter how it's bucketed); above it, a bar spans multiple snapshots and
# gets genuine range. A fixed 1-hour bucket reliably captures 2-3 snapshots
# per bar (confirmed on real data) — so charts use this uniformly rather than
# the EA's own M1/M5/M15/M30 label, none of which survive a 30-min data floor.
_CHART_BUCKET_SECS = 3600


def get_signal_chart(sig: dict, min_half_span_hours: int = 12, max_bars: int = 72) -> dict:
    """Historical OHLC anchored on a signal's own signal_time (and close_time)
    rather than "now" — reuses get_fx_ohlc()'s epoch-bucketing technique
    against dealio.ticks, which carries live data under the EA's own symbol
    strings (BTCUSD, CRUDE.OIL, USTECH, GOLD, SILVER, EURUSD — verified
    directly against production, not assumed from FX_GROUPS).

    The window is centered on signal_time rather than a fixed lookback: the
    "before" side mirrors however much time has actually elapsed since the
    signal fired (floored at min_half_span_hours so a brand-new signal still
    gets real context, capped by max_bars so a long-running trade doesn't
    balloon the query). A fixed pre-signal lookback made the signal candle
    sit almost at the right edge for any signal younger than that lookback —
    which is most of them.

    Cached briefly for ACTIVE signals (price still moving) and effectively
    permanently for CLOSED ones (history is final).
    """
    bucket_secs = _CHART_BUCKET_SECS
    is_active = sig["status"] == "ACTIVE"
    key = f"sig_chart:{sig['id']}:{sig.get('updated_at')}"
    cached = _cache_get(key, 25 if is_active else 24 * 3600)
    if cached is not None:
        return cached

    signal_time = sig["signal_time"]
    end_time = sig.get("close_time") or datetime.now(timezone.utc)
    elapsed_secs = max((end_time - signal_time).total_seconds(), 0)
    half_span_secs = max(elapsed_secs, min_half_span_hours * 3600)
    start_time = signal_time - timedelta(seconds=half_span_secs)
    min_start = end_time - timedelta(seconds=max_bars * bucket_secs)
    if start_time < min_start:
        start_time = min_start

    # dealio.ticks.lastmodified is `timestamp without time zone`, and this
    # connection's session timezone is Europe/Nicosia (+3) — confirmed via
    # SHOW TIMEZONE. EXTRACT(EPOCH FROM lastmodified) treats that naive value
    # as UTC directly (Postgres semantics for naive timestamps), but comparing
    # it against a timezone-AWARE bind parameter implicitly casts through the
    # *session* timezone instead — a different rule, ~3 hours apart, verified
    # directly (a query bounded to end at 20:52 UTC was returning bars up to
    # 23:00 UTC). Binding naive UTC wall-clock values instead keeps both the
    # bucketing and the WHERE-clause bounds on the same rule.
    start_naive = start_time.replace(tzinfo=None)
    end_naive = end_time.replace(tzinfo=None)

    with dealio() as cur:
        cur.execute("""
            SELECT
                TO_TIMESTAMP(FLOOR(EXTRACT(EPOCH FROM lastmodified) / %(b)s) * %(b)s) AS ts,
                ROUND(((array_agg((bid+ask) ORDER BY lastmodified))[1] / 2.0)::numeric, 5)      AS o,
                ROUND((MAX(bid+ask) / 2.0)::numeric, 5)                                          AS h,
                ROUND((MIN(bid+ask) / 2.0)::numeric, 5)                                          AS l,
                ROUND(((array_agg((bid+ask) ORDER BY lastmodified DESC))[1] / 2.0)::numeric, 5) AS c
            FROM dealio.ticks
            WHERE symbol = %(sym)s
              AND lastmodified >= %(start)s
              AND lastmodified <= %(end)s
            GROUP BY 1
            ORDER BY 1
        """, {"b": bucket_secs, "sym": sig["symbol"], "start": start_naive, "end": end_naive})
        bars = [
            {"ts": r["ts"].isoformat(), "o": float(r["o"]), "h": float(r["h"]), "l": float(r["l"]), "c": float(r["c"])}
            for r in cur.fetchall()
        ]

    result = {
        "bars": bars,
        "entry_price": sig["entry_price"], "sl_price": sig["sl_price"], "tp_price": sig["tp_price"],
        "resistance_1": sig.get("resistance_1"), "resistance_2": sig.get("resistance_2"), "resistance_3": sig.get("resistance_3"),
        "support_1": sig.get("support_1"), "support_2": sig.get("support_2"), "support_3": sig.get("support_3"),
        "signal_time": signal_time.isoformat(),
        "close_time": sig["close_time"].isoformat() if sig.get("close_time") else None,
        "direction": sig["direction"], "status": sig["status"], "outcome": sig.get("outcome"),
    }
    _cache_set(key, result)
    return result
