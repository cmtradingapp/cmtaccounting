"""Tests for database schema creation and basic CRUD operations."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.models import (
    RawPSPTransaction, RawCRMTransaction,
    CleanCRMTransaction, CleanPSPTransaction, CleanBankTransaction,
    PSPSchemaRegistry, ReconciliationRun, Reconciliation,
)


class TestSchemaCreation:
    """Verify all tables are created correctly."""

    def test_tables_exist(self, test_db):
        """All expected tables should be created."""
        from sqlalchemy import inspect
        inspector = inspect(test_db.bind)
        tables = inspector.get_table_names()

        expected = [
            "raw_psp_transactions", "raw_bank_transactions", "raw_crm_transactions",
            "clean_crm_transactions", "clean_psp_transactions", "clean_bank_transactions",
            "psp_schema_registry", "reconciliation_runs", "reconciliation",
        ]
        for table in expected:
            assert table in tables, f"Missing table: {table}"


class TestCRUD:
    """Test basic insert/query operations."""

    def test_insert_raw_psp(self, test_db):
        raw = RawPSPTransaction(
            psp_name="nuvei", source_file="Nuvei.xlsx",
            row_number=0, raw_data={"Transaction ID": "123", "Amount": "100"}
        )
        test_db.add(raw)
        test_db.flush()
        assert raw.id is not None

    def test_insert_clean_crm(self, test_db):
        clean = CleanCRMTransaction(
            psp_transaction_id="ABC123", transactionid="456",
            amount=100.0, currency="USD", transaction_type="Deposit"
        )
        test_db.add(clean)
        test_db.flush()
        assert clean.id is not None

    def test_insert_clean_psp(self, test_db):
        clean = CleanPSPTransaction(
            psp_name="korapay", reference_id="ABC123",
            amount=100.0, currency="NGN", source_file="Korapay.csv"
        )
        test_db.add(clean)
        test_db.flush()
        assert clean.id is not None

    def test_insert_registry(self, test_db):
        reg = PSPSchemaRegistry(
            psp_name="test_psp", ref_column="Transaction ID",
            amount_column="Amount"
        )
        test_db.add(reg)
        test_db.flush()
        assert reg.id is not None

    def test_reconciliation_run_relationship(self, test_db):
        run = ReconciliationRun(report_month="2023-01", match_rate=85.5, matched=100)
        test_db.add(run)
        test_db.flush()

        result = Reconciliation(
            run_id=run.id, crm_id=1, match_status="matched",
            crm_amount=100.0, psp_amount=100.0
        )
        test_db.add(result)
        test_db.flush()

        assert result.run_id == run.id
        assert len(run.results) == 1
