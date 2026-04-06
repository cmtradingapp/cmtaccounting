# Unreconciled Amount Always Showed $0.00

**Status:** Fixed (`web-gui/server.py` — unrecon_fees calculation)
**Impact:** The "Unreconciled amount" figure on the results page was always $0.00 regardless of actual discrepancies

## Root Cause

After merging 17+ PSP files into a single DataFrame, the code searched for a bank amount column using:

```python
bank_amt_col = next((c for c in both.columns
                     if 'amount' in c.lower() and c != crm_amt and not c.endswith('_crm')), None)
```

This picked `Net Amount (local)` — a column from Directa24 (10 rows) that was first in iteration order. Two problems:

1. **Wrong column**: Only 10/1,481 matched rows had a value in `Net Amount (local)`. The other 1,471 rows came from different PSPs with different amount column names.
2. **Comma-formatted numbers**: Even the 10 non-null values were strings like `583,000.00` which `pd.to_numeric` coerced to NaN.

Result: 0 valid `(crm_amount, bank_amount)` pairs → `abs().sum() = 0.0`.

## Fix

Each PSP file now gets its amount column detected via `_detect_bank_amount_col()` during the per-PSP loop (before merging), and the value is normalized into a unified `_bank_amount` column with comma-stripping:

```python
_bamt_col = _detect_bank_amount_col(bank_df)
if _bamt_col:
    bank_df['_bank_amount'] = pd.to_numeric(
        bank_df[_bamt_col].astype(str).str.replace(',', '', regex=False),
        errors='coerce')
```

The unrecon calculation now compares `amount_crm` vs `_bank_amount`, using only rows where both are non-null and taking absolute values before differencing (CRM stores all amounts as positive, some PSP files report withdrawals as negative).
