"""Reconciliation engine — orchestrates SQL-based matching.

Runs the match queries in order:
1. Primary CRM ↔ PSP match (psp_transaction_id = reference_id)
2. Fallback CRM ↔ PSP match (transactionid = reference_id)
3. CRM ↔ Bank match (psp_transaction_id = client_id)
4. Collect unmatched rows on both sides
5. Compute stats and write to final tables
"""

from datetime import datetime, timezone

from sqlalchemy import text

from db.engine import get_session
from db.models import Reconciliation, ReconciliationRun
from reconciliation.queries import (
    PRIMARY_PSP_MATCH, FALLBACK_PSP_MATCH, BANK_MATCH,
    COUNT_CRM, COUNT_PSP, COUNT_BANK,
)


def run_reconciliation(report_month: str = None, triggered_by: str = "manual") -> dict:
    """Execute the full reconciliation pipeline.

    Returns a summary dict with match stats.
    """
    session = get_session()

    try:
        # ── Totals ──────────────────────────────────────────────────────────
        total_crm = session.execute(text(COUNT_CRM)).scalar()
        total_psp = session.execute(text(COUNT_PSP)).scalar()
        total_bank = session.execute(text(COUNT_BANK)).scalar()

        # ── Create reconciliation run ───────────────────────────────────────
        run = ReconciliationRun(
            report_month=report_month,
            run_at=datetime.now(timezone.utc),
            triggered_by=triggered_by,
        )
        session.add(run)
        session.flush()

        matched_crm_ids = set()
        matched_psp_ids = set()
        matched_bank_ids = set()
        results = []

        # ── Pass 1: Primary CRM ↔ PSP match ────────────────────────────────
        primary_rows = session.execute(text(PRIMARY_PSP_MATCH)).fetchall()
        for row in primary_rows:
            crm_id, psp_tx_id, crm_amt, psp_amt, crm_ccy, psp_ccy, psp_name = row
            if crm_id in matched_crm_ids:
                continue  # avoid double-counting
            matched_crm_ids.add(crm_id)
            matched_psp_ids.add(psp_tx_id)

            diff = abs((crm_amt or 0) - (psp_amt or 0))
            ccy_match = _currency_match(crm_amt, psp_amt, crm_ccy, psp_ccy)

            results.append(Reconciliation(
                run_id=run.id,
                crm_id=crm_id,
                psp_tx_id=psp_tx_id,
                match_status="matched",
                crm_amount=crm_amt,
                psp_amount=psp_amt,
                amount_diff=diff,
                currency_match=ccy_match,
            ))

        # ── Pass 2: Fallback CRM ↔ PSP match ───────────────────────────────
        if matched_crm_ids and matched_psp_ids:
            crm_placeholder = ",".join(str(i) for i in matched_crm_ids)
            psp_placeholder = ",".join(str(i) for i in matched_psp_ids)
        else:
            crm_placeholder = "0"
            psp_placeholder = "0"

        fallback_sql = FALLBACK_PSP_MATCH.format(
            matched_crm_ids=crm_placeholder,
            matched_psp_ids=psp_placeholder,
        )
        fallback_rows = session.execute(text(fallback_sql)).fetchall()
        for row in fallback_rows:
            crm_id, psp_tx_id, crm_amt, psp_amt, crm_ccy, psp_ccy, psp_name = row
            if crm_id in matched_crm_ids:
                continue
            matched_crm_ids.add(crm_id)
            matched_psp_ids.add(psp_tx_id)

            diff = abs((crm_amt or 0) - (psp_amt or 0))
            ccy_match = _currency_match(crm_amt, psp_amt, crm_ccy, psp_ccy)

            results.append(Reconciliation(
                run_id=run.id,
                crm_id=crm_id,
                psp_tx_id=psp_tx_id,
                match_status="matched",
                crm_amount=crm_amt,
                psp_amount=psp_amt,
                amount_diff=diff,
                currency_match=ccy_match,
            ))

        # ── Pass 3: CRM ↔ Bank match ───────────────────────────────────────
        crm_placeholder = ",".join(str(i) for i in matched_crm_ids) if matched_crm_ids else "0"
        bank_sql = BANK_MATCH.format(matched_crm_ids=crm_placeholder)
        bank_rows = session.execute(text(bank_sql)).fetchall()
        for row in bank_rows:
            crm_id, bank_tx_id, crm_amt, bank_amt, crm_ccy, bank_ccy = row
            if crm_id in matched_crm_ids:
                continue
            matched_crm_ids.add(crm_id)
            matched_bank_ids.add(bank_tx_id)

            diff = abs((crm_amt or 0) - (bank_amt or 0))
            ccy_match = _currency_match(crm_amt, bank_amt, crm_ccy, bank_ccy)

            results.append(Reconciliation(
                run_id=run.id,
                crm_id=crm_id,
                bank_tx_id=bank_tx_id,
                match_status="matched",
                crm_amount=crm_amt,
                bank_amount=bank_amt,
                amount_diff=diff,
                currency_match=ccy_match,
            ))

        # ── Unmatched CRM rows ──────────────────────────────────────────────
        all_crm_ids_q = "SELECT id, amount FROM clean_crm_transactions"
        all_crm = session.execute(text(all_crm_ids_q)).fetchall()
        for crm_id, crm_amt in all_crm:
            if crm_id not in matched_crm_ids:
                results.append(Reconciliation(
                    run_id=run.id,
                    crm_id=crm_id,
                    match_status="unmatched_crm",
                    crm_amount=crm_amt,
                ))

        # ── Unmatched PSP rows ──────────────────────────────────────────────
        all_psp_ids_q = "SELECT id, amount FROM clean_psp_transactions"
        all_psp = session.execute(text(all_psp_ids_q)).fetchall()
        for psp_id, psp_amt in all_psp:
            if psp_id not in matched_psp_ids:
                results.append(Reconciliation(
                    run_id=run.id,
                    psp_tx_id=psp_id,
                    match_status="unmatched_psp",
                    psp_amount=psp_amt,
                ))

        # ── Bulk insert results ─────────────────────────────────────────────
        session.add_all(results)

        # ── Compute stats ───────────────────────────────────────────────────
        matched_count = len(matched_crm_ids)
        unmatched_crm_count = total_crm - matched_count
        unmatched_psp_count = total_psp - len(matched_psp_ids)

        # Unrecon amount: sum of amount_diff for same-currency matched pairs
        unrecon = sum(r.amount_diff or 0 for r in results
                      if r.match_status == "matched" and r.currency_match == "same")

        match_rate = (matched_count / total_crm * 100) if total_crm > 0 else 0

        run.match_rate = round(match_rate, 2)
        run.matched = matched_count
        run.unmatched_crm = unmatched_crm_count
        run.unmatched_psp = unmatched_psp_count
        run.unrecon_amount = round(unrecon, 2)

        session.commit()

        summary = {
            "run_id": run.id,
            "report_month": report_month,
            "total_crm": total_crm,
            "total_psp": total_psp,
            "total_bank": total_bank,
            "matched": matched_count,
            "matched_via_psp_id": len(primary_rows),
            "matched_via_txn_id": len([r for r in fallback_rows
                                        if r[0] not in {pr[0] for pr in primary_rows}]),
            "matched_via_bank": len(matched_bank_ids),
            "unmatched_crm": unmatched_crm_count,
            "unmatched_psp": unmatched_psp_count,
            "match_rate": round(match_rate, 2),
            "unrecon_amount": round(unrecon, 2),
        }

        return summary

    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _currency_match(amt1, amt2, ccy1, ccy2):
    """Determine if amounts are same-currency or cross-currency.

    Same-currency: ratio within 0.8–1.2 (accounts for rounding/fees).
    Cross-currency: ratio outside that range (likely different currencies).
    """
    if not amt1 or not amt2:
        return "unknown"

    a1 = abs(float(amt1))
    a2 = abs(float(amt2))

    if a1 == 0:
        return "unknown"

    ratio = a2 / a1
    if 0.8 <= ratio <= 1.2:
        return "same"
    return "cross_ccy"
