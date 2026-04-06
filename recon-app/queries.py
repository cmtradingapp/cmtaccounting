"""All reconciliation SQL lives here."""

from db import dealio, backoffice, backoffice_rw

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
    with backoffice() as cur:
        cur.execute("""
            SELECT DISTINCT TO_CHAR(DATE_TRUNC('month', confirmation_time), 'YYYY-MM') AS month
            FROM vtiger_mttransactions
            WHERE transactionapproval = 'Approved'
              AND confirmation_time IS NOT NULL
            ORDER BY month DESC
            LIMIT 36
        """)
        return [r["month"] for r in cur.fetchall()]


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
    """Join MT4 netdeposit vs CRM cash transactions per login."""
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
    """Create fee tables if they don't exist. Safe to call on every startup."""
    with backoffice_rw() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS psp_agreements (
                id SERIAL PRIMARY KEY,
                psp_name VARCHAR(100) NOT NULL,
                provider_name VARCHAR(200),
                agreement_entity VARCHAR(200),
                agreement_date DATE,
                addendum_date DATE,
                auto_settlement BOOLEAN DEFAULT FALSE,
                settlement_bank VARCHAR(200),
                active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS psp_fee_rules (
                id SERIAL PRIMARY KEY,
                agreement_id INTEGER NOT NULL REFERENCES psp_agreements(id) ON DELETE CASCADE,
                payment_method VARCHAR(100),
                fee_type VARCHAR(50) NOT NULL,
                country VARCHAR(100) DEFAULT 'GLOBAL',
                sub_provider VARCHAR(100),
                fee_kind VARCHAR(20) NOT NULL
                    CHECK (fee_kind IN ('percentage','fixed','fixed_plus_pct','tiered')),
                pct_rate DECIMAL(10,6),
                fixed_amount DECIMAL(14,2),
                fixed_currency VARCHAR(10),
                description TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS psp_fee_tiers (
                id SERIAL PRIMARY KEY,
                fee_rule_id INTEGER NOT NULL REFERENCES psp_fee_rules(id) ON DELETE CASCADE,
                volume_from DECIMAL(14,2) NOT NULL DEFAULT 0,
                volume_to DECIMAL(14,2),
                pct_rate DECIMAL(10,6) NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_fee_rules_agreement ON psp_fee_rules(agreement_id);
            CREATE INDEX IF NOT EXISTS idx_fee_tiers_rule ON psp_fee_tiers(fee_rule_id);
        """)


# --- Agreements ---

def get_all_agreements():
    with backoffice() as cur:
        cur.execute("""
            SELECT a.*, COUNT(r.id) AS rule_count
            FROM psp_agreements a
            LEFT JOIN psp_fee_rules r ON r.agreement_id = a.id
            WHERE a.active = TRUE
            GROUP BY a.id
            ORDER BY a.psp_name
        """)
        return [dict(r) for r in cur.fetchall()]


def get_agreement(psp_id):
    with backoffice() as cur:
        cur.execute("SELECT * FROM psp_agreements WHERE id = %s", (psp_id,))
        row = cur.fetchone()
        return dict(row) if row else None


def create_agreement(data):
    with backoffice_rw() as cur:
        cur.execute("""
            INSERT INTO psp_agreements (psp_name, provider_name, agreement_entity,
                agreement_date, addendum_date, auto_settlement, settlement_bank)
            VALUES (%(psp_name)s, %(provider_name)s, %(agreement_entity)s,
                %(agreement_date)s, %(addendum_date)s, %(auto_settlement)s, %(settlement_bank)s)
            RETURNING id
        """, data)
        return cur.fetchone()["id"]


def update_agreement(psp_id, data):
    data["id"] = psp_id
    with backoffice_rw() as cur:
        cur.execute("""
            UPDATE psp_agreements SET
                psp_name = %(psp_name)s, provider_name = %(provider_name)s,
                agreement_entity = %(agreement_entity)s,
                agreement_date = %(agreement_date)s, addendum_date = %(addendum_date)s,
                auto_settlement = %(auto_settlement)s, settlement_bank = %(settlement_bank)s,
                updated_at = NOW()
            WHERE id = %(id)s
        """, data)


def delete_agreement(psp_id):
    with backoffice_rw() as cur:
        cur.execute("UPDATE psp_agreements SET active = FALSE WHERE id = %s", (psp_id,))


# --- Fee Rules ---

def get_fee_rules(agreement_id):
    with backoffice() as cur:
        cur.execute("""
            SELECT * FROM psp_fee_rules
            WHERE agreement_id = %s
            ORDER BY payment_method, country, fee_type
        """, (agreement_id,))
        rules = [dict(r) for r in cur.fetchall()]

        rule_ids = [r["id"] for r in rules]
        tiers_by_rule = {}
        if rule_ids:
            cur.execute("""
                SELECT * FROM psp_fee_tiers
                WHERE fee_rule_id = ANY(%s)
                ORDER BY fee_rule_id, volume_from
            """, (rule_ids,))
            for t in cur.fetchall():
                tiers_by_rule.setdefault(t["fee_rule_id"], []).append(dict(t))

        for r in rules:
            r["tiers"] = tiers_by_rule.get(r["id"], [])
        return rules


def create_fee_rule(agreement_id, data):
    with backoffice_rw() as cur:
        cur.execute("""
            INSERT INTO psp_fee_rules (agreement_id, payment_method, fee_type, country,
                sub_provider, fee_kind, pct_rate, fixed_amount, fixed_currency, description)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (agreement_id, data["payment_method"], data["fee_type"], data["country"],
              data["sub_provider"], data["fee_kind"], data["pct_rate"],
              data["fixed_amount"], data["fixed_currency"], data["description"]))
        rule_id = cur.fetchone()["id"]

        if data["fee_kind"] == "tiered" and data.get("tiers"):
            for t in data["tiers"]:
                cur.execute("""
                    INSERT INTO psp_fee_tiers (fee_rule_id, volume_from, volume_to, pct_rate)
                    VALUES (%s, %s, %s, %s)
                """, (rule_id, t["volume_from"], t["volume_to"], t["pct_rate"]))
        return rule_id


def delete_fee_rule(rule_id):
    with backoffice_rw() as cur:
        cur.execute("DELETE FROM psp_fee_rules WHERE id = %s", (rule_id,))
