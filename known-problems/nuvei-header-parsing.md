# Nuvei — Bad Excel Header + Float Precision Loss

**Status:** Fixed (`web-gui/server.py` — `_load_psp_file`)
**Impact:** 383 SafeCharge/Nuvei CRM rows completely unmatched (0 matches from Nuvei.xlsx)

## Root Cause

`Nuvei.xlsx` is a CPanel export with 11 rows of metadata before the real column headers:

```
row 0:  CPanel report:   Transactions
row 1:  Generated on:    2023-03-24 14:23:24
row 2:  Total rows:      384
...
row 11: Date  Transaction ID  Payment Method  Currency  Amount ...
```

`_load_psp_file` called `pd.read_excel(path)` with no `skiprows`, so pandas picked up row 0 as the header. This produced columns like `['CPanel report:', 'Transactions', 'Unnamed: 2', ...]`. The column filter already skipped files where first col starts with `CPanel`, but the real issue was that `_detect_bank_ref_col` found no valid reference column → the entire file went to `bank_only_frames` → 0 CRM rows matched.

### Second problem: float precision on 19-digit IDs

Even when read with the correct `skiprows=11`, pandas inferred `Transaction ID` as `float64`. IEEE 754 double has ~15.7 significant decimal digits; SafeCharge IDs are 19 digits:

```
CRM psp_transaction_id:  1130000004097387874   (exact)
Nuvei float read:        1.1300000042694845e18 → 1130000004269484500 (wrong last digits)
```

With `dtype=str`, full precision is preserved and 377/383 CRM SafeCharge IDs match.

## Fix

`_load_psp_file` was updated to:
1. Read Excel files with `dtype=str` to preserve large integer IDs.
2. Auto-detect the real header row: if >50% of columns are `Unnamed` (or first column contains report-like keywords), scan the first 20 rows for one that has 3+ header-keyword hits (`date`, `amount`, `id`, `transaction`, `currency`, etc.) and re-read from that row.

## Residual gap (6 rows)

6 SafeCharge IDs are in the CRM but not in the Nuvei file at all. These may be transactions that were voided/refunded before settlement, or belong to a different Nuvei account/MID not included in this export.
