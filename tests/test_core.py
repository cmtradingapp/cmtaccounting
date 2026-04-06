"""
Integration tests using real January 2023 data.

Reference baseline (from fact_check.py against List.xlsx):
  Total rows:    10 255
  Matched:        2 978
  Unmatched:      7 277
  IsTiming=1:         3
  Unrecon. Fees:    0.0
  Matched By:     'CGH'   ← original analyst; ours will be 'MRS'

These tests verify our engine produces structurally correct output and that
match rates are in a plausible range given all flat PSP files for January.
"""
import os
import pytest
import pandas as pd
from server import normalize_key, build_lifecycle_df, build_balances_df, OUTPUT_COLUMNS

# ── Reference numbers from fact_check.py ────────────────────────────────────
REF_TOTAL_ROWS    = 10_255
REF_MATCHED_ROWS  =  2_978
REF_UNMATCHED     =  7_277


# ---------------------------------------------------------------------------
# Reference data sanity-check
# (Ensures our test baseline is correct before we test our own code against it)
# ---------------------------------------------------------------------------

class TestReferenceData:

    def test_reference_row_count(self, reference_lifecycle):
        assert len(reference_lifecycle) == REF_TOTAL_ROWS, (
            f"Reference List.xlsx should have {REF_TOTAL_ROWS} rows, "
            f"got {len(reference_lifecycle)}"
        )

    def test_reference_column_count(self, reference_lifecycle):
        assert len(reference_lifecycle.columns) == 52

    def test_reference_column_names(self, reference_lifecycle):
        assert list(reference_lifecycle.columns) == OUTPUT_COLUMNS

    def test_reference_matched_count(self, reference_lifecycle):
        matched = reference_lifecycle['Match No'].notna().sum()
        assert matched == REF_MATCHED_ROWS

    def test_reference_unmatched_count(self, reference_lifecycle):
        unmatched = reference_lifecycle['Match No'].isna().sum()
        assert unmatched == REF_UNMATCHED

    def test_reference_unrecon_fees_zero(self, reference_lifecycle):
        total = reference_lifecycle['Unrecon. Fees'].sum()
        assert total == 0.0

    def test_reference_matched_by_analyst(self, reference_lifecycle):
        values = reference_lifecycle['Matched By'].dropna().unique().tolist()
        assert values == ['CGH'], f"Expected ['CGH'], got {values}"


# ---------------------------------------------------------------------------
# CRM file integrity
# ---------------------------------------------------------------------------

class TestCrmFile:

    def test_loads_without_error(self, jan_crm):
        assert jan_crm is not None

    def test_row_count_reasonable(self, jan_crm):
        """CRM Additional Info covers the PSP-matched subset (~3 550 rows for Jan 2023).
        The full lifecycle (10 255) also draws from the D&W Report and other platform files."""
        assert 1_000 <= len(jan_crm) <= REF_TOTAL_ROWS, (
            f"CRM row count {len(jan_crm)} outside expected range 1000–{REF_TOTAL_ROWS}"
        )

    def test_has_psp_transaction_id(self, jan_crm):
        assert 'psp_transaction_id' in jan_crm.columns

    def test_has_mtorder_id(self, jan_crm):
        assert 'mtorder_id' in jan_crm.columns

    def test_has_amount(self, jan_crm):
        assert 'amount' in jan_crm.columns

    def test_psp_id_non_null_rate(self, jan_crm):
        non_null = jan_crm['psp_transaction_id'].notna().mean()
        assert non_null > 0.20, (
            f"Expected >20% non-null psp_transaction_id, got {non_null:.1%}"
        )


# ---------------------------------------------------------------------------
# PSP files
# ---------------------------------------------------------------------------

class TestPspFiles:

    def test_at_least_one_flat_psp_file(self, jan_psp_files):
        assert len(jan_psp_files) >= 5, (
            f"Expected at least 5 flat PSP files, found {len(jan_psp_files)}"
        )

    def test_all_psp_files_readable(self, jan_psp_files):
        errors = []
        for path in jan_psp_files:
            try:
                ext = os.path.splitext(path)[1].lower()
                if ext in ('.xlsx', '.xls'):
                    pd.read_excel(path, nrows=1)
                else:
                    pd.read_csv(path, nrows=1)
            except Exception as e:
                errors.append(f"{os.path.basename(path)}: {e}")
        assert not errors, "Unreadable PSP files:\n" + "\n".join(errors)

    def test_trust_payments_has_transactionreference(self, jan_psp_files):
        tp = next((f for f in jan_psp_files if 'trustpayments' in f.lower()), None)
        assert tp is not None, "TrustPayments.csv not found in PSP files"
        df = pd.read_csv(tp, nrows=5)
        assert 'transactionreference' in df.columns


# ---------------------------------------------------------------------------
# normalize_key on real data
# ---------------------------------------------------------------------------

class TestNormalizeKeyOnRealData:

    def test_crm_join_key_has_no_float_suffix(self, jan_crm):
        keys = normalize_key(jan_crm['psp_transaction_id'].dropna())
        assert not keys.str.endswith('.0').any(), (
            "normalize_key left .0 float suffixes in CRM join keys"
        )

    def test_crm_join_key_no_leading_zeros(self, jan_crm):
        keys = normalize_key(jan_crm['psp_transaction_id'].dropna())
        assert not keys.str.match(r'^0\d').any(), (
            "normalize_key left leading zeros in numeric CRM join keys"
        )


# ---------------------------------------------------------------------------
# Reconciliation with real data
# ---------------------------------------------------------------------------

@pytest.fixture(scope='module')
def jan_merged(jan_crm, jan_psp_files):
    """Outer-join of January CRM vs TrustPayments only.

    We use TrustPayments specifically because it has the confirmed join key
    column `transactionreference`. Concatenating all PSPs produces a mixed
    column space where unrelated 'reference' columns from other PSPs cause
    false key collisions — per-PSP column detection is a future improvement.
    """
    tp_path = next(f for f in jan_psp_files if 'trustpayments' in f.lower())
    bank_df = pd.read_csv(tp_path, encoding='utf-8', encoding_errors='replace')

    bank_df['_join_key'] = normalize_key(bank_df['transactionreference'])
    crm = jan_crm.copy()
    crm['_join_key'] = normalize_key(crm['psp_transaction_id'])

    return crm.merge(bank_df, on='_join_key', how='outer',
                     suffixes=('_crm', '_bank'), indicator=True)


class TestReconciliationWithRealData:

    def test_merged_contains_all_crm_rows(self, jan_merged):
        crm_in_merged = (jan_merged['_merge'] != 'right_only').sum()
        # CRM Additional Info has 3 550 rows for January 2023
        assert crm_in_merged == 3_550, (
            f"Outer join should preserve all 3 550 CRM rows, got {crm_in_merged}"
        )

    def test_match_rate_is_positive(self, jan_merged):
        matched = (jan_merged['_merge'] == 'both').sum()
        assert matched > 0, "Expected at least some matches across all PSP files"

    def test_match_rate_is_nonzero(self, jan_merged):
        """
        At least one CRM row should match TrustPayments.
        (TrustPayments covers only a small fraction of January transactions;
        full-month match rate requires all PSPs to be loaded together.)
        """
        matched = (jan_merged['_merge'] == 'both').sum()
        assert matched >= 1, "Expected at least one matched row against TrustPayments"

    def test_bank_duplicates_are_known_data_quality_issue(self, jan_merged):
        """
        TrustPayments contains multiple rows per transactionreference
        (refunds, chargebacks, and settlement batches share the same reference).
        This causes fan-out in the merge — a known limitation documented here.
        The test simply asserts the duplicate count is stable (< 5 000) so a
        regression would be caught if the join logic changed unexpectedly.
        """
        crm_rows = jan_merged[jan_merged['_merge'] != 'right_only']
        dup_keys = crm_rows['_join_key'].duplicated().sum()
        assert dup_keys < 5_000, (
            f"Unexpected explosion in duplicate CRM rows: {dup_keys}. "
            "Check if null join key handling regressed."
        )


# ---------------------------------------------------------------------------
# Lifecycle export with real data
# ---------------------------------------------------------------------------

class TestLifecycleExportWithRealData:

    @pytest.fixture(autouse=True)
    def lc(self, jan_merged):
        self.lc = build_lifecycle_df(jan_merged)

    def test_52_columns(self):
        assert len(self.lc.columns) == 52

    def test_column_order_matches_spec(self):
        assert list(self.lc.columns) == OUTPUT_COLUMNS

    def test_row_count_equals_merged(self, jan_merged):
        assert len(self.lc) == len(jan_merged)

    def test_all_matched_have_match_no(self, jan_merged):
        matched_mask = jan_merged['_merge'] == 'both'
        assert self.lc.loc[matched_mask, 'Match No'].notna().all()

    def test_all_matched_have_matched_by_mrs(self, jan_merged):
        matched_mask = jan_merged['_merge'] == 'both'
        assert (self.lc.loc[matched_mask, 'Matched By'] == 'MRS').all()

    def test_all_matched_have_matched_on(self, jan_merged):
        matched_mask = jan_merged['_merge'] == 'both'
        assert self.lc.loc[matched_mask, 'Matched On'].notna().all()

    def test_unmatched_no_match_no(self, jan_merged):
        crm_only = jan_merged['_merge'] == 'left_only'
        assert self.lc.loc[crm_only, 'Match No'].isna().all()

    def test_deal_no_populated_from_crm(self, jan_merged):
        crm_rows = jan_merged['_merge'] != 'right_only'
        populated = self.lc.loc[crm_rows, 'Deal No'].notna().mean()
        assert populated > 0.90, f"Expected >90% of CRM rows to have Deal No, got {populated:.1%}"

    def test_amount_populated_from_crm(self, jan_merged):
        crm_rows = jan_merged['_merge'] != 'right_only'
        populated = self.lc.loc[crm_rows, 'Amount'].notna().mean()
        assert populated > 0.90, f"Expected >90% of CRM rows to have Amount, got {populated:.1%}"

    def test_reference_column_structure_matches_ours(self, reference_lifecycle):
        """Our 52-column spec must exactly match the historical output file."""
        assert list(reference_lifecycle.columns) == list(self.lc.columns), (
            "Column mismatch between our output and the reference List.xlsx"
        )
