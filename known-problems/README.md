# Known Problems

Issues discovered during reconciliation analysis that affect match rates, data quality, or output accuracy. Each file covers one root cause, its impact, and its fix status.

| File | Status | Impact |
|---|---|---|
| [nuvei-header-parsing.md](nuvei-header-parsing.md) | Fixed | 383 unmatched SafeCharge/Nuvei rows |
| [solidpayments-ref-column.md](solidpayments-ref-column.md) | Fixed | 80 unmatched SolidPayments rows |
| [korapay-join-key.md](korapay-join-key.md) | Fixed | 101 unmatched Korapay rows |
| [unrecon-amount-zero.md](unrecon-amount-zero.md) | Fixed | Unreconciled amount always showed $0.00 |
| [unfixable-gaps.md](unfixable-gaps.md) | Data gap | ~279 rows — needs different source files |
