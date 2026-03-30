# Korapay — Wrong Join Key (settlementreference before paymentreference)

**Status:** Fixed (`web-gui/server.py` — `_detect_bank_ref_col` priority list)
**Impact:** 101 Korapay CRM rows completely unmatched

## Root Cause

`Korapay Pay-ins.csv` has three reference columns:

| Column | Values | Matches CRM? |
|---|---|---|
| `payment_reference` | `1065583320`, `1065575144`, ... | Yes — matches CRM `transactionid` |
| `settlement_reference` | `KPY-SET-QXnjYQBWnzzZOckz`, ... | No |
| `transaction_reference` | `KPY-CM-gwLuddIOn8eo`, ... | No |

In `_detect_bank_ref_col`, `settlementreference` appeared **before** `paymentreference` in the priority list. `settlement_reference` (normalized: `settlementreference`) was therefore selected as the join column. Settlement references are batch-level IDs with no CRM counterpart, so 0 rows matched.

The existing per-PSP CRM key selection would have correctly switched to `transactionid` once the right bank column was found, since `payment_reference` values have full 101/101 overlap with CRM `transactionid`.

## Fix

`paymentreference` was moved above `settlementreference` in the priority list of `_detect_bank_ref_col`. `payment_reference` is now selected, and the overlap check automatically switches the CRM key from `psp_transaction_id` to `transactionid`, yielding 101 matches.

## Note on Korapay Payouts

`Korapay Payouts.csv` uses a `reference` column with different values that do not match CRM `transactionid`. Korapay withdrawal rows in CRM appear to reference pay-in IDs (possibly indicating internal refund/reversal tracking). These 0 rows matched from Payouts are expected.
