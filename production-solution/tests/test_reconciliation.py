"""Tests for the reconciliation engine — SQL-based matching."""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.models import (
    CleanCRMTransaction, CleanPSPTransaction, CleanBankTransaction,
    ReconciliationRun, Reconciliation,
)


class TestReconciliationLogic:
    """Test reconciliation matching with synthetic data."""

    def _seed_test_data(self, session):
        """Insert test CRM + PSP rows with known matches."""
        # CRM rows
        crm_rows = [
            CleanCRMTransaction(id=1, psp_transaction_id="AAA111", transactionid="TXN001",
                                amount=100.0, currency="USD", transaction_type="Deposit"),
            CleanCRMTransaction(id=2, psp_transaction_id="BBB222", transactionid="TXN002",
                                amount=200.0, currency="EUR", transaction_type="Deposit"),
            CleanCRMTransaction(id=3, psp_transaction_id="CCC333", transactionid="TXN003",
                                amount=300.0, currency="USD", transaction_type="Withdraw"),
            CleanCRMTransaction(id=4, psp_transaction_id="DDD444", transactionid="TXN004",
                                amount=400.0, currency="ZAR", transaction_type="Deposit"),
            CleanCRMTransaction(id=5, psp_transaction_id="NOMATCH", transactionid="TXN005",
                                amount=500.0, currency="USD", transaction_type="Deposit"),
        ]
        session.add_all(crm_rows)

        # PSP rows — AAA111, BBB222 match on psp_transaction_id
        # TXN003 matches on fallback transactionid
        psp_rows = [
            CleanPSPTransaction(id=1, psp_name="nuvei", reference_id="AAA111",
                                amount=100.0, currency="USD", source_file="test.csv"),
            CleanPSPTransaction(id=2, psp_name="korapay", reference_id="BBB222",
                                amount=200.0, currency="EUR", source_file="test.csv"),
            CleanPSPTransaction(id=3, psp_name="zotapay", reference_id="TXN003",
                                amount=300.0, currency="USD", source_file="test.csv"),
            CleanPSPTransaction(id=4, psp_name="nuvei", reference_id="ORPHAN",
                                amount=999.0, currency="USD", source_file="test.csv"),
        ]
        session.add_all(psp_rows)

        # Bank rows — DDD444 matches CRM row 4
        bank_rows = [
            CleanBankTransaction(id=1, bank_name="nedbank", client_id="DDD444",
                                 amount=400.0, currency="ZAR", source_file="test.csv"),
        ]
        session.add_all(bank_rows)

        session.commit()

    def test_primary_match(self, test_db):
        """CRM rows with matching psp_transaction_id should be matched."""
        self._seed_test_data(test_db)

        from sqlalchemy import text
        from reconciliation.queries import PRIMARY_PSP_MATCH

        rows = test_db.execute(text(PRIMARY_PSP_MATCH)).fetchall()
        matched_crm_ids = {r[0] for r in rows}

        # AAA111 and BBB222 should match
        assert 1 in matched_crm_ids
        assert 2 in matched_crm_ids
        assert len(rows) >= 2

    def test_fallback_match(self, test_db):
        """CRM rows unmatched by primary should try fallback on transactionid."""
        self._seed_test_data(test_db)

        from sqlalchemy import text
        from reconciliation.queries import PRIMARY_PSP_MATCH, FALLBACK_PSP_MATCH

        # Get primary matches first
        primary = test_db.execute(text(PRIMARY_PSP_MATCH)).fetchall()
        matched_crm = {r[0] for r in primary}
        matched_psp = {r[1] for r in primary}

        crm_ph = ",".join(str(i) for i in matched_crm) if matched_crm else "0"
        psp_ph = ",".join(str(i) for i in matched_psp) if matched_psp else "0"

        fallback_sql = FALLBACK_PSP_MATCH.format(
            matched_crm_ids=crm_ph, matched_psp_ids=psp_ph
        )
        fallback = test_db.execute(text(fallback_sql)).fetchall()
        fallback_crm_ids = {r[0] for r in fallback}

        # CRM row 3 (CCC333/TXN003) should match PSP row 3 (reference_id=TXN003)
        assert 3 in fallback_crm_ids

    def test_bank_match(self, test_db):
        """CRM rows unmatched by PSP should try bank matching."""
        self._seed_test_data(test_db)

        from sqlalchemy import text
        from reconciliation.queries import BANK_MATCH

        # Assume CRM rows 1,2,3 matched via PSP
        bank_sql = BANK_MATCH.format(matched_crm_ids="1,2,3")
        bank_rows = test_db.execute(text(bank_sql)).fetchall()
        bank_crm_ids = {r[0] for r in bank_rows}

        # CRM row 4 (DDD444) should match bank row 1
        assert 4 in bank_crm_ids

    def test_unmatched_crm(self, test_db):
        """CRM row 5 (NOMATCH) should remain unmatched."""
        self._seed_test_data(test_db)

        from sqlalchemy import text
        from reconciliation.queries import PRIMARY_PSP_MATCH, FALLBACK_PSP_MATCH, BANK_MATCH

        # Run all passes
        primary = test_db.execute(text(PRIMARY_PSP_MATCH)).fetchall()
        matched = {r[0] for r in primary}

        crm_ph = ",".join(str(i) for i in matched) if matched else "0"
        psp_ph = ",".join(str(r[1]) for r in primary) if primary else "0"
        fallback = test_db.execute(text(
            FALLBACK_PSP_MATCH.format(matched_crm_ids=crm_ph, matched_psp_ids=psp_ph)
        )).fetchall()
        matched.update(r[0] for r in fallback)

        crm_ph = ",".join(str(i) for i in matched) if matched else "0"
        bank = test_db.execute(text(
            BANK_MATCH.format(matched_crm_ids=crm_ph)
        )).fetchall()
        matched.update(r[0] for r in bank)

        # CRM row 5 should NOT be matched
        assert 5 not in matched

    def test_unmatched_psp(self, test_db):
        """PSP row 4 (ORPHAN) should remain unmatched."""
        self._seed_test_data(test_db)

        from sqlalchemy import text
        from reconciliation.queries import PRIMARY_PSP_MATCH, FALLBACK_PSP_MATCH

        primary = test_db.execute(text(PRIMARY_PSP_MATCH)).fetchall()
        matched_psp = {r[1] for r in primary}

        crm_ph = ",".join(str(r[0]) for r in primary) if primary else "0"
        psp_ph = ",".join(str(i) for i in matched_psp) if matched_psp else "0"
        fallback = test_db.execute(text(
            FALLBACK_PSP_MATCH.format(matched_crm_ids=crm_ph, matched_psp_ids=psp_ph)
        )).fetchall()
        matched_psp.update(r[1] for r in fallback)

        # PSP row 4 (ORPHAN) should NOT be matched
        assert 4 not in matched_psp
