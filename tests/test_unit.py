"""
Unit tests for pure functions in server.py:
  - normalize_key
  - build_lifecycle_df (column structure + business rules)
  - build_balances_df (column structure)
"""
import pytest
import pandas as pd
import numpy as np
from server import normalize_key, build_lifecycle_df, build_balances_df, OUTPUT_COLUMNS


# ---------------------------------------------------------------------------
# normalize_key
# ---------------------------------------------------------------------------

class TestNormalizeKey:

    def _k(self, value):
        return normalize_key(pd.Series([value])).iloc[0]

    def test_strips_whitespace(self):
        assert self._k('  REF123  ') == 'REF123'

    def test_uppercases(self):
        assert self._k('ref-abc-123') == 'REF-ABC-123'

    def test_removes_float_suffix(self):
        """IDs stored as floats in pandas (e.g. 39120162.0) must become 39120162."""
        assert self._k('39120162.0') == '39120162'

    def test_removes_float_suffix_multiple_zeros(self):
        assert self._k('12345.00') == '12345'

    def test_strips_leading_zeros(self):
        assert self._k('0012345') == '12345'

    def test_strips_leading_zeros_single(self):
        assert self._k('01') == '1'

    def test_combined_float_and_whitespace(self):
        assert self._k('  0039120162.0  ') == '39120162'

    def test_already_clean(self):
        assert self._k('ABC123') == 'ABC123'

    def test_alphanumeric_with_dashes(self):
        """Non-numeric reference IDs like TP-ABC-12345 keep their structure."""
        assert self._k('tp-abc-12345') == 'TP-ABC-12345'

    def test_vectorised_batch(self):
        """All rows in a Series are normalised."""
        s = pd.Series(['  ref1.0  ', '0002', 'CLEAN'])
        result = normalize_key(s).tolist()
        assert result == ['REF1', '2', 'CLEAN']

    def test_null_becomes_na(self):
        """None/NaN must normalise to NA so null rows never match each other in a merge."""
        result = normalize_key(pd.Series([None])).iloc[0]
        assert pd.isna(result), f"Expected NA, got {result!r}"

    def test_nan_float_becomes_na(self):
        import numpy as np
        result = normalize_key(pd.Series([np.nan])).iloc[0]
        assert pd.isna(result), f"Expected NA, got {result!r}"


# ---------------------------------------------------------------------------
# build_lifecycle_df — column structure
# ---------------------------------------------------------------------------

def _make_minimal_merged(n_matched=3, n_crm_only=2, n_bank_only=1):
    """Build a minimal synthetic merged DataFrame for testing."""
    rows = []
    for i in range(n_matched):
        rows.append({
            '_join_key': f'REF{i}',
            '_merge': 'both',
            'psp_transaction_id': f'REF{i}',
            'mtorder_id': 1000 + i,
            'amount_crm': 100.0 + i,
            'amount_bank': 100.0 + i,
            'currency_id': 'USD',
            'usdamount': 100.0 + i,
            'transactiontype': 'Deposit',
            'first_name': 'Alice',
            'last_name': 'Smith',
            'payment_method': 'CARD',
        })
    for i in range(n_crm_only):
        rows.append({
            '_join_key': f'CRM_ONLY_{i}',
            '_merge': 'left_only',
            'psp_transaction_id': f'CRM_ONLY_{i}',
            'mtorder_id': 2000 + i,
            'amount_crm': 50.0,
            'currency_id': 'EUR',
            'usdamount': 55.0,
            'transactiontype': 'Withdrawal',
            'first_name': 'Bob',
            'last_name': 'Jones',
        })
    for i in range(n_bank_only):
        rows.append({
            '_join_key': f'BANK_ONLY_{i}',
            '_merge': 'right_only',
            'transactionreference': f'BANK_ONLY_{i}',
            'amount_bank': 75.0,
        })
    return pd.DataFrame(rows)


class TestBuildLifecycleDf:

    @pytest.fixture(autouse=True)
    def df(self):
        self.merged = _make_minimal_merged()
        self.result = build_lifecycle_df(self.merged)

    def test_exactly_52_columns(self):
        assert len(self.result.columns) == 52, (
            f"Expected 52 columns, got {len(self.result.columns)}: {list(self.result.columns)}"
        )

    def test_column_names_match_spec(self):
        assert list(self.result.columns) == OUTPUT_COLUMNS

    def test_matched_rows_have_match_no(self):
        matched = self.result[self.result['Recon.Reason Group'] == 'Matched']
        assert matched['Match No'].notna().all(), "All matched rows must have a Match No"

    def test_unmatched_rows_have_no_match_no(self):
        unmatched = self.result[self.result['Recon.Reason Group'] == 'Unmatched']
        assert unmatched['Match No'].isna().all(), "Unmatched rows must not have a Match No"

    def test_matched_by_is_mrs(self):
        matched = self.result[self.result['Recon.Reason Group'] == 'Matched']
        assert (matched['Matched By'] == 'MRS').all()

    def test_unmatched_matched_by_is_null(self):
        unmatched = self.result[self.result['Recon.Reason Group'] == 'Unmatched']
        assert unmatched['Matched By'].isna().all()

    def test_match_count_is_1_for_matched(self):
        matched = self.result[self.result['Recon.Reason Group'] == 'Matched']
        assert (matched['MatchCount'] == 1).all()

    def test_match_count_is_0_for_unmatched(self):
        unmatched = self.result[self.result['Recon.Reason Group'] == 'Unmatched']
        assert (unmatched['MatchCount'] == 0).all()

    def test_is_timing_false_for_matched(self):
        matched = self.result[self.result['Recon.Reason Group'] == 'Matched']
        assert (~matched['IsTiming']).all()

    def test_matched_on_populated_for_matched(self):
        matched = self.result[self.result['Recon.Reason Group'] == 'Matched']
        assert matched['Matched On'].notna().all()

    def test_remarks_combines_name(self):
        matched = self.result[self.result['Recon.Reason Group'] == 'Matched']
        assert (matched['Remarks'] == 'Alice Smith').all()

    def test_category_code_deposit(self):
        matched = self.result[self.result['Recon.Reason Group'] == 'Matched']
        assert (matched['CategoryCode'] == 'Deposit').all()

    def test_category_code_withdrawal(self):
        crm_only = self.result[self.result['Recon.Reason Group'] == 'Unmatched']
        assert (crm_only['CategoryCode'] == 'Withdrawal').all()

    def test_match_nos_are_sequential(self):
        matched = self.result[self.result['Match No'].notna()]
        nos = sorted(matched['Match No'].tolist())
        assert nos == list(range(1, len(nos) + 1))


# ---------------------------------------------------------------------------
# build_balances_df — column structure
# ---------------------------------------------------------------------------

class TestBuildBalancesDf:

    def test_columns(self):
        merged = _make_minimal_merged()
        merged['currency_id'] = 'USD'
        merged['amount_crm'] = 100.0
        merged['usdamount'] = 100.0
        result = build_balances_df(merged)
        assert list(result.columns) == ['Currency', 'Equity', 'Equity EUR', 'Equity USD', 'Perc']

    def test_perc_sums_to_100(self):
        merged = _make_minimal_merged()
        result = build_balances_df(merged)
        assert abs(result['Perc'].sum() - 100.0) < 0.1, (
            f"Percentages should sum to ~100, got {result['Perc'].sum()}"
        )
