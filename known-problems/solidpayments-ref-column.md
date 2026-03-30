# SolidPayments — Wrong Reference Column Selected

**Status:** Fixed (`web-gui/server.py` — per-PSP ref column selection)
**Impact:** 80 SolidPayments CRM rows unmatched (only 12 of 92 matched)

## Root Cause

`Solidpayments.csv` has two candidate reference columns:

| Column | Overlap with CRM `psp_transaction_id` |
|---|---|
| `TransactionId` | 12 rows |
| `UniqueId` | 79 rows |

`_detect_bank_ref_col` returns the **first** match from its priority list. `transactionid` is priority-1 exact, so it always picks `TransactionId` — even though `UniqueId` matches 6× more CRM rows.

`TransactionId` in SolidPayments is their internal transaction counter. `UniqueId` is the merchant-side unique identifier that the CRM stores as `psp_transaction_id`.

## Fix

The per-PSP matching loop was changed to:
1. Collect all candidate reference columns from `_detect_bank_ref_cols` (new plural variant).
2. For each candidate, compute overlap against both CRM join keys (`psp_transaction_id` and `transactionid`).
3. Pick the `(bank_col, crm_key)` pair with the highest overlap.

This is the same philosophy as the existing CRM-side key selection that already picks between `psp_transaction_id` and `transactionid` based on overlap.

## Why Only 79 of 80 Matched

One SolidPayments CRM row has a `psp_transaction_id` that is not present in the `UniqueId` column of the January Solidpayments.csv. This is likely a transaction that was approved in CRM but rejected or refunded before appearing in the settlement export.
