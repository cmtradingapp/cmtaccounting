"""All reconciliation SQL lives here."""

from db import dealio, backoffice

# Payment methods that represent real cash (exclude bonuses, credits, fees)
CASH_METHODS = (
    'Wire transfer', 'Wire', 'External', 'Credit card', 'CreditCard',
    'Electronic payment', 'ElectronicPayment', 'CryptoWallet', 'Crypto',
    'Cash', 'CashDeposit', 'Transfer', 'None', '',
)


def available_months():
    """Months that have data in both sources, most recent first."""
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
    """Per-login deposit/withdrawal totals from backoffice CRM for a given month."""
    with backoffice() as cur:
        cur.execute("""
            SELECT
                login,
                SUM(CASE WHEN transactiontype = 'Deposit' THEN usdamount ELSE 0 END)            AS deposits,
                SUM(CASE WHEN transactiontype IN ('Withdrawal','Withdraw') THEN usdamount ELSE 0 END) AS withdrawals,
                SUM(CASE
                    WHEN transactiontype = 'Deposit' THEN usdamount
                    WHEN transactiontype IN ('Withdrawal','Withdraw') THEN -usdamount
                    ELSE 0
                END)                                                                              AS net,
                COUNT(*)                                                                          AS tx_count,
                STRING_AGG(DISTINCT payment_method, ', ')                                         AS payment_methods
            FROM vtiger_mttransactions
            WHERE transactionapproval = 'Approved'
              AND EXTRACT(YEAR  FROM confirmation_time) = %s
              AND EXTRACT(MONTH FROM confirmation_time) = %s
            GROUP BY login
        """, (year, month))
        return {r["login"]: dict(r) for r in cur.fetchall()}


def mt4_summary(year: int, month: int):
    """Per-login net deposit totals from dealio daily_profits for a given month."""
    with dealio() as cur:
        cur.execute("""
            SELECT
                login,
                SUM(netdeposit)          AS net,
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
    """
    Join MT4 netdeposit vs CRM approved transactions per login.
    Returns list of dicts sorted by absolute discrepancy descending.
    """
    crm = crm_summary(year, month)
    mt4 = mt4_summary(year, month)

    all_logins = set(crm) | set(mt4)
    rows = []

    for login in all_logins:
        c = crm.get(login, {})
        m = mt4.get(login, {})

        crm_net = float(c.get("net") or 0)
        mt4_net = float(m.get("net_usd") or 0)
        diff = round(mt4_net - crm_net, 2)
        abs_diff = abs(diff)

        if abs_diff < 0.01:
            status = "matched"
        elif login not in crm:
            status = "mt4_only"
        elif login not in mt4:
            status = "crm_only"
        else:
            status = "discrepancy"

        rows.append({
            "login":           login,
            "mt4_net":         round(mt4_net, 2),
            "crm_net":         round(crm_net, 2),
            "crm_deposits":    round(float(c.get("deposits") or 0), 2),
            "crm_withdrawals": round(float(c.get("withdrawals") or 0), 2),
            "difference":      diff,
            "abs_diff":        abs_diff,
            "status":          status,
            "payment_methods": c.get("payment_methods", ""),
            "tx_count":        c.get("tx_count", 0),
            "currency":        m.get("groupcurrency", "USD"),
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
