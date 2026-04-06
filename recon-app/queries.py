"""All reconciliation SQL lives here."""

from db import dealio, backoffice

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
