"""All reconciliation SQL lives here."""

import os
import time
import psycopg2
from db import dealio, crm, fees_db, FEES_MODE

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
            is_conflict = 'conflict with recovery' in msg or code in ('57014', '40001')
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


def mt4_summary(year: int, month: int):
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

    def _fetch_mt4():
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

    return _db_retry(_fetch_mt4)


def _load_praxis_account_map() -> dict:
    """
    Build {session_cid (str) -> [mt4_login (int), ...]} lookup from Antelope CRM.
    vtiger_trading_accounts.vtigeraccountid == praxis_transactions.session_cid.
    Cached 30 minutes — the mapping rarely changes.
    """
    key = "praxis_account_map"
    cached = _cache_get(key, 1800)
    if cached is not None:
        return cached

    try:
        mapping: dict = {}
        def _fetch():
            with crm() as cur:
                cur.execute("SELECT login, vtigeraccountid FROM report.vtiger_trading_accounts WHERE (deleted IS NULL OR deleted = 0)")
                m: dict = {}
                for r in cur.fetchall():
                    cid = str(r["vtigeraccountid"]).strip()
                    if cid:
                        m.setdefault(cid, []).append(int(r["login"]))
                return m
        mapping = _db_retry(_fetch)
    except Exception:
        mapping = {}

    _cache_set(key, mapping)
    return mapping


def praxis_summary(year: int, month: int) -> dict:
    """
    Per-MT4-login Praxis deposit/withdrawal totals for a month. Cached 5 min.

    Join: praxis_transactions.session_cid = vtiger_trading_accounts.vtigeraccountid
          vtiger_trading_accounts.login = MT4 login
    """
    import datetime
    key = f"praxis:{year}:{month}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached

    month_start = datetime.date(year, month, 1)
    month_end   = datetime.date(year + 1, 1, 1) if month == 12 else datetime.date(year, month + 1, 1)

    # Load CRM account mapping (session_cid -> [login, ...])
    account_map = _load_praxis_account_map()

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
    except Exception:
        rows = []

    # Map each session_cid to MT4 login(s).
    # Only assign when the mapping is unambiguous (exactly one MT4 login per Praxis customer).
    # When a Praxis customer has multiple MT4 accounts we cannot reliably attribute the
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

    This bridges the gap between MT4 net deposit (post-fee) and CRM gross deposit
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
    """Join MT4 netdeposit vs CRM cash transactions per login (cached 5 min)."""
    key = f"reconcile:{year}:{month}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached

    crm    = crm_summary(year, month)
    mt4    = mt4_summary(year, month)
    praxis = praxis_summary(year, month)
    fees   = crm_expected_fees(year, month)   # {login: expected_fee_usd}

    rows = []
    for login in set(crm) | set(mt4):
        c = crm.get(login, {})
        m = mt4.get(login, {})

        mt4_net      = round(float(m.get("net_usd") or 0), 2)
        crm_cash     = round(c.get("cash_net", 0), 2)
        expected_fee = round(fees.get(login, 0.0), 2)
        diff         = round(mt4_net - crm_cash, 2)
        # Fee adj is informational only — company pays fees so they don't affect matching
        fee_adj_diff = round(diff - expected_fee, 2)

        if login not in crm:
            status = "mt4_only"
        elif login not in mt4:
            status = "crm_only"
        elif abs(diff) < 1.0:
            status = "matched"
        else:
            status = "discrepancy"

        p = praxis.get(login, {})
        praxis_net = round(
            p.get("praxis_deposits", 0) - p.get("praxis_withdrawals", 0), 2
        )
        rows.append({
            "login":              login,
            "mt4_net":            mt4_net,
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
        })

    rows.sort(key=lambda r: r["abs_diff"], reverse=True)

    # Add cash_methods: only the subset of payment_methods that are cash-relevant
    for r in rows:
        raw = [m.strip() for m in (r.get("payment_methods") or "").split(",") if m.strip()]
        cash_only = [m for m in raw if m in CASH_METHODS]
        r["cash_methods"] = ", ".join(cash_only) if cash_only else None

    _cache_set(key, rows)
    return rows


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
    account_map = _load_praxis_account_map()   # {cid: [login, ...]} cached 30 min
    login_to_cid = {}
    for cid, logins in account_map.items():
        for login in logins:
            if login not in login_to_cid:
                login_to_cid[login] = cid

    STATUS_PRIORITY = {"discrepancy": 3, "crm_only": 2, "mt4_only": 2, "matched": 1}

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
            "mt4_net":        round(sum(r["mt4_net"]        for r in logins), 2),
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
        })

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


_TTL_CLIENT_LIST = 3600   # 1 hour — expensive cross-DB aggregate


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

    # Opening snapshot (first day per login in range)
    def _fetch_open():
        with dealio() as cur:
            cur.execute("SET statement_timeout = 30000")
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
            cur.execute("SET statement_timeout = 30000")
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
            cur.execute("SET statement_timeout = 30000")
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

    open_snap  = _db_retry(_fetch_open)
    close_snap = _db_retry(_fetch_close)
    agg        = _db_retry(_fetch_agg)

    # CID + name mapping
    account_map = _load_praxis_account_map()  # {cid: [login, ...]}
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

    # CRM name fallback (vtiger_account joined to trading_accounts)
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
        dealio_names = _db_retry(_fetch_du)
    except Exception:
        dealio_names = {}

    rows = []
    for login, a in agg.items():
        cid   = login_to_cid.get(login, "")
        name  = praxis_names.get(cid) or dealio_names.get(login) or ""
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


def client_list(date_from=None, date_to=None) -> list:
    """
    Full client list with Company P&L from MT4 trading data.
    Aggregates across all logins in daily_profits for the date range.

    Returns list of dicts sorted by company_total descending (most valuable first):
      login, net_deposit_usd, client_realised_pnl, company_trading_pnl,
      company_total, currency, cid (if mapped), name (from Praxis if available)

    Cached 1 hour — this is an expensive query (~4s on full dataset).
    """
    import datetime as _dt
    if date_from is None:
        date_from = _dt.date(2021, 1, 1)
    if date_to is None:
        date_to = _dt.date.today() + _dt.timedelta(days=1)

    key = f"client_list:{date_from}:{date_to}"
    cached = _cache_get(key, _TTL_CLIENT_LIST)
    if cached is not None:
        return cached

    # 1. MT4 aggregate per login using existing dealio() context manager.
    # SET statement_timeout inline so the replica doesn't kill us mid-scan.
    def _fetch_mt4():
        with dealio() as cur:
            cur.execute("SET statement_timeout = 30000")
            cur.execute("""
                SELECT
                    login,
                    SUM(convertednetdeposit)  AS net_deposit,
                    SUM(convertedclosedpnl)   AS client_realised_pnl,
                    MAX(groupcurrency)        AS currency,
                    MAX(date)                 AS last_active
                FROM dealio.daily_profits
                WHERE date >= %s AND date < %s
                GROUP BY login
            """, (date_from, date_to))
            return {r["login"]: dict(r) for r in cur.fetchall()}
    mt4 = _db_retry(_fetch_mt4)

    # 2. CID mapping login → cid
    account_map = _load_praxis_account_map()   # {cid: [login, ...]}
    login_to_cid = {}
    for cid_str, logins in account_map.items():
        for login in logins:
            if login not in login_to_cid:
                login_to_cid[login] = cid_str

    # 3. Names: Praxis (preferred) then dealio_users fallback
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

    # Fallback: Antelope CRM name keyed by login (covers clients with no Praxis activity)
    def _fetch_dealio_names():
        with crm() as cur:
            cur.execute("""
                SELECT ta.login,
                       RTRIM(a.first_name + ' ' + a.last_name) AS name,
                       a.email
                FROM report.vtiger_trading_accounts ta
                JOIN report.vtiger_account a ON a.accountid = ta.vtigeraccountid
                WHERE (ta.deleted IS NULL OR ta.deleted = 0)
                  AND ta.login IS NOT NULL
                  AND a.first_name IS NOT NULL AND a.first_name != ''
            """)
            return {r["login"]: {"name": (r["name"] or "").strip(), "email": r["email"] or ""}
                    for r in cur.fetchall()}
    try:
        dealio_names = _db_retry(_fetch_dealio_names)
    except Exception:
        dealio_names = {}

    # 4. Build result rows
    rows = []
    for login, m in mt4.items():
        net_dep   = float(m["net_deposit"] or 0)
        client_pnl= float(m["client_realised_pnl"] or 0)
        co_trading= round(-client_pnl, 2)
        co_total  = round(co_trading + net_dep, 2)

        cid        = login_to_cid.get(login, "")
        praxis_info= praxis_names.get(cid, {})
        dealio_info= dealio_names.get(login, {})
        # Prefer Praxis name (has first+last), fall back to dealio_users
        name  = praxis_info.get("name") or dealio_info.get("name") or ""
        email = praxis_info.get("email") or dealio_info.get("email") or ""

        rows.append({
            "login":               login,
            "cid":                 cid,
            "name":                name,
            "email":               email,
            "net_deposit":         round(net_dep, 2),
            "client_realised_pnl": round(client_pnl, 2),
            "company_trading_pnl": co_trading,
            "company_total":       co_total,
            "currency":            m.get("currency") or "USD",
            "last_active":         str(m.get("last_active") or ""),
        })

    rows.sort(key=lambda r: r["company_total"], reverse=True)
    _cache_set(key, rows)
    return rows


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
    except Exception:
        praxis_rows = []

    # 2. Group Praxis transactions by session_cid
    from collections import defaultdict
    by_cid: dict = defaultdict(list)
    for r in praxis_rows:
        by_cid[str(r["session_cid"]).strip()].append(dict(r))

    # 3. Account map cid → [login]
    account_map = _load_praxis_account_map()

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

        mt4_accounts = []
        for login in (logins if logins else [None]):
            acct_txs = txs   # when ambiguous, all txs shown under each login
            p_dep  = sum(float(t["usd_amount"] or 0) for t in acct_txs if t["direction"] == "payment")
            p_with = sum(float(t["usd_amount"] or 0) for t in acct_txs
                        if t["direction"] in ("withdrawal", "payout"))
            p_net  = round(p_dep - p_with, 2)

            c_net  = round(crm.get(login or 0, {}).get("cash_net", 0), 2) if login else 0
            diff   = round(p_net - c_net, 2)

            if login is None:
                match = "no_mt4"
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

            mt4_accounts.append({
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
            "mt4_accounts":  mt4_accounts,
            "has_issue":     ambiguous or any(a["match"] not in ("matched",) for a in mt4_accounts),
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
                account_map = _load_praxis_account_map()
                rows = []
                for r in cur.fetchall():
                    row = dict(r)
                    row["usd_amount"] = float(row["usd_amount"] or 0)
                    row["fee_actual"] = float(row["fee_actual"] or 0)
                    cid = str(row.get("login") or "").strip()
                    logins = account_map.get(cid, [])
                    row["mt4_login"] = logins[0] if logins else None
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


def cid_full_profile(cid: str, date_from, date_to) -> dict:
    """
    Full profile for a Praxis customer (session_cid) across all their MT4 accounts.
    Returns a unified dict with name, email, all logins, Praxis txs, CRM + MT4 per login.
    Cached 5 minutes.
    """
    key = f"cid_profile:{cid}:{date_from}:{date_to}"
    cached = _cache_get(key, _TTL_RECONCILE)
    if cached is not None:
        return cached

    account_map = _load_praxis_account_map()
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

    # 2. CRM + MT4 per login
    crm_by_login = {}
    mt4_by_login = {}
    for login in logins:
        crm_by_login[login] = client_crm_detail(login, date_from, date_to)
        mt4_by_login[login] = client_mt4_detail(login, date_from, date_to)

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

    # 4. Trading P&L from MT4 (converted to USD), also compute per-login
    client_realised_pnl = 0.0
    client_unrealised_eod = 0.0
    pnl_by_login = {}
    for login, rows in mt4_by_login.items():
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
        "mt4_accounts": logins,
        "praxis_txs":   praxis_txs,
        "crm_by_login": crm_by_login,
        "mt4_by_login": mt4_by_login,
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
            # Total company value from client = trading profit + net deposit margin
            # (money deposited but never withdrawn stays with the company)
            "company_total_value":  round(company_trading_pnl + net_deposit, 2),
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


def client_mt4_detail(login: int, date_from, date_to) -> list:
    """MT4 daily profits for a login over an arbitrary date range."""
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
                  AND netdeposit != 0
                ORDER BY date
            """, (login, date_from, date_to))
            return [dict(r) for r in cur.fetchall()]
    return _db_retry(_fetch)


def client_praxis_detail(login: int, date_from, date_to) -> list:
    """Individual Praxis transactions for a login over an arbitrary date range.
    Resolves MT4 login → Praxis session_cid via vtiger_trading_accounts.
    """
    account_map = _load_praxis_account_map()
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
