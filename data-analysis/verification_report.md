# Lifecycle Report — Verification Report

**Generated:** 2026-03-27
**Reference file:** `relevant-data/Life cycle report/2023/1. January/Life Cycle Report-final.xlsx`
**Test input:** All flat January 2023 PSP files + CRM Transactions Additional info.xlsx

---

## Summary Scorecard

| Tab | REF rows | GEN rows | Column match | Content match | Status |
|-----|----------|----------|--------------|---------------|--------|
| MT4-Transactions | 10,253 | 6,350 | YES (44 cols) | PARTIAL | ⚠ |
| MT4 CCY Life Cycle | 19,307 | 2,099 | YES (4 cols) | PARTIAL | ⚠ |
| MT4 USD per acc Life Cycle | 14,838 | 2,099 | YES (4 cols) | PARTIAL | ⚠ |
| PM USD Life Cycle | 165 | 10 | YES (3 cols) | PARTIAL | ⚠ |
| PM CCY Life Cycle | 230 | 2 | YES (4 cols) | PARTIAL | ⚠ |
| PM-Transactions | 3,460 | 6,350 | YES (31 cols) | NO | ✗ |
| Mapping Rules | 82 | 78 | YES (8 cols) | PARTIAL | ⚠ |

---

## Tab 1: MT4-Transactions

### ✅ Confirmed working
- Column count: 44 (correct)
- Column names: match spec exactly
- `ClientAccount` = `login` (verified: login == ClientAccount for all matched rows)
- Reconciliation metadata (Match No, Matched By, Matched On, Recon.Reason Group) structurally correct

### Bug 1 — Deal No format (FIXABLE)
| | REF | GEN |
|--|-----|-----|
| Format | String: "33945316" or UUID "00335488-9074-4ce3-b712-6eb73ca8441b" | Float: 33894137.0 |
| Fix | Convert `mtorder_id` to integer → string in `build_mt4_transactions_df` |

UUID entries in REF (e.g. "00335488-9074-4ce3-b712-6eb73ca8441b") come from the MT4 platform P&L entries (PRF rows) which are not in our CRM file. These will remain absent until a full MT4 export is added as an input.

### Bug 2 — Currency is a FK integer, not a code (FIXABLE)
| | REF | GEN |
|--|-----|-----|
| Values | "USD", "EUR", "ZAR", "NGN", etc. | "1.0", "2.0" |
| Fix | Lookup table: currency_id 1 → USD, 2 → EUR (verified via join on 3,548 rows) |

```
currency_id=1 → USD (3,504 rows)
currency_id=2 → EUR (42 rows, 2 exceptions mapped to USD)
```

More currencies (ZAR, NGN, etc.) must come from the PSP side or other platform files not in the CRM Additional Info file.

### Bug 3 — Payment Method uses raw CRM text instead of 2-letter PM codes (FIXABLE)
| | REF | GEN |
|--|-----|-----|
| Format | 2-letter codes: ZP, SC, INA, PRF, TRF, BN, ADJ, etc. | Mix: some correct codes (INA, ZP) + raw text (Processing fees, Transfer, Wire transfer) |

The mapping from CRM `payment_method` text → PM code is:
| CRM payment_method | PM code |
|--------------------|---------|
| Credit card | Derived from `payment_processor` (ZP, SC, INA, KP, FRX, etc.) |
| Wire transfer | BT (Banks) |
| Electronic payment | Derived from `payment_processor` (ZP, KP, OZ, etc.) |
| CryptoWallet | FRX |
| Transfer | TRF |
| Processing fees | PRF |
| Bonus | BN |
| Adjustment | ADJ |
| FRF commission | ADJ |
| IB commission | ADJ |
| Commission | PRF |
| Cash | ADJ |
| Chargeback | SC (or ADJ) |

Fix: Add `_map_pm_text_to_code()` that uses `payment_processor` for PSP methods and fixed codes for non-PSP methods (PRF, TRF, BN, ADJ).

### Bug 4 — TRX Type: "Wire transfer" incorrectly maps to "4. Transfer" (FIXABLE)
Wire transfer is a PSP flow via Banks (BT) → should produce "2. DP" or "2. WD".
Remove "wire transfer" from `_PM_TEXT_TO_TRX_TYPE` so it falls through to PSP flow detection.

### Data Gap — Missing P&L rows (NOT FIXABLE without additional data)
| Category | REF count | GEN count |
|----------|-----------|-----------|
| 5. Realised Profits | 3,633 | 0 |
| 5. Realized Storage | 1,526 | 0 |
| 5. Unrealised Profits | 1,385 | 0 |
| 5. Accumulated P&L | 24 | 0 |
| 5. Platform Balances | 5 | 0 |

These rows come from the PRF (Processing/Platform) entries in the full MT4 platform export. They are not present in the CRM Transactions Additional Info file. A full MT4 trade history export would be needed.

### Bug 5 — MT4-Transactions includes bank-only rows (FIXABLE)
- GEN has 6,350 rows because it includes PSP-side rows (bank_only + matched)
- MT4-Transactions should only contain CRM-side rows (left_only + both)
- Fix: Filter to `_merge != 'right_only'` in `build_mt4_transactions_df`

---

## Tab 2: MT4 CCY Life Cycle

### Bug 6 — Currency FK not resolved (FIXABLE — same as Bug 2)
- GEN currencies: "1.0", "2.0"
- REF currencies: "USD", "EUR", "ZAR", "NGN", "USDT", etc.
- Zero matches on (Client Account, Currency) because keys never match

### Bug 7 — Confirmed: login == ClientAccount (correct, no fix needed)
Verified via join: `login == ClientAccount` is True for all 3,548 matched rows.

### Data Gap — Missing attributes (NOT FIXABLE without equity + full MT4 data)
| Attribute | REF count | GEN count |
|-----------|-----------|-----------|
| 1. Opening Balance | 6,164 | 0 (opening balance file not loading) |
| Total Movement | 4,411 | 0 |
| 5. Realised Profits | 3,633 | 0 |
| 5. Realized Storage | 1,526 | 0 |
| 5. Unrealised Profits | 1,385 | 0 |
| 5. Accumulated P&L | 11 | 0 |
| 5. Platform Balances | 5 | 0 |

Opening Balance issue: even though `Unrealised.xlsx` is loaded, the key lookup fails because:
- Our key: `(login, "1.0")` (currency FK not resolved)
- Reference key: `(login, "USD")`

Fix currency first (Bug 2/6) and the opening balance should populate.

### Data Gap — REF covers 6,924 accounts vs GEN covers 1,371 accounts
REF has small-numbered corporate accounts (2003, 2004, 2012…) that come from PRF/ADJ entries not in our CRM file. Our 1,371 accounts (all 14-digit client accounts) are a valid subset.

---

## Tab 3: MT4 USD per acc Life Cycle

Same issues as Tab 2 (CCY FK, missing P&L, filtered to same 1,371 accounts). Additionally:
- REF attribute "1. Opening Before (USD)" requires the opening balance with resolved currency

---

## Tab 4: PM-Transactions

### Bug 8 — Row count wrong (FIXABLE)
- GEN outputs ALL 6,350 merged rows
- PM-Transactions should only include PSP-side rows (bank-only + matched bank side)
- Fix: filter to `_merge.isin(['right_only', 'both'])` in `build_pm_transactions_df`

### Bug 9 — PM Name is upload filename, not PSP name (FIXABLE)
| | REF | GEN |
|--|-----|-----|
| PM Name | "AstroPay", "Banks", "KoraPay" | "bankFile_0.csv", "bankFile_16.csv" |

Fix: Build a filename → PM Name lookup using the Mapping Rules table.
Example: `TrustPayments.csv` → PM code `TP` → PM Name `TrustPayments`.

### Bug 10 — TRX Type all "4. Transfer" (PARTIALLY FIXABLE)
- REF PM-Transactions has: 2. DP (1,752), 2. WD (611), 5. Fees/Charges (534), 4. Transfer (394), etc.
- The TRX Type for PM entries can be derived from the amount sign: positive = 2. DP, negative = 2. WD
- Fee entries need the bank-side transaction type which varies by PSP file
- Fix: use amount sign to determine 2. DP vs 2. WD for main PSP entries

### Data Gap — Reference format differs
- REF Reference: "1061710220/Payout" (Ozow format: `{client_account}/Payout`)
- GEN Reference: raw bank reference numbers (653252956)
- This is different source formatting; not necessarily wrong, just different

### Data Gap — Match No numbering
- REF Match No: large numbers (96841, 96884, etc.)
- GEN Match No: sequential 1, 2, 3, 4, 5
- By design: our engine generates its own sequential match numbers; the reference used a different numbering system

---

## Tab 5 & 6: PM CCY/USD Life Cycle

### Root cause of near-empty output
PM CCY lifecycle aggregates from PM-Transactions by `(PM Name, Currency)`. Since:
1. PM Name is the filename (Bug 9) not a valid PM name
2. Currency for PSP rows often can't be detected (depends on each PSP file having a "Currency" column)
3. PM-Transactions includes CRM rows (Bug 8) which pollutes the aggregation

Fixing Bugs 8 and 9 should substantially improve this tab.

---

## Tab 7: Mapping Rules

- GEN: 78 rows (from reference file) ← correct, from actual reference
- REF: 82 rows (reference file has 4 extra rows: header/metadata rows)
- The 4-row difference is the file path metadata at top of the Mapping Rules sheet
- Content is correct

---

## Fix Status

### Fixed
| Bug | Description | Status |
|-----|-------------|--------|
| Bug 1 | Deal No as integer string (no .0) | FIXED |
| Bug 2/6 | Currency FK → ISO code (1=USD, 2=EUR) | FIXED |
| Bug 3 | Payment Method → 2-letter PM code | FIXED |
| Bug 4 | Wire transfer → BT → 2. DP/WD | FIXED |
| Bug 5 | MT4-Transactions: CRM rows only | FIXED |
| Bug 8 | PM-Transactions: PSP rows only | FIXED |
| Bug 9 | PM Name: real PSP name from filename map | FIXED |
| Sign | CRM amounts unsigned → apply direction sign | FIXED |
| Fan-out | CRM row double-counted via bank duplicates | FIXED |

### Amount Accuracy (after fixes)
| Attribute | Match rate | Notes |
|-----------|-----------|-------|
| 2. DP | **100%** | Perfect |
| 5. Fees/Charges | **100%** | Perfect |
| 2. WD | **96%** | ~4% differ: ZP Electronic payment sometimes → 4. Transfer in REF |
| 5. Bonuses | **96%** | Minor edge cases |

### Remaining P2 — Requires additional data
1. **P&L entries** (5. Realised Profits, 5. Unrealised Profits, 5. Realized Storage): need full MT4 platform export — not in CRM Additional Info file
2. **Additional currencies** (ZAR, NGN, GBP, USDT, etc.): CURRENCY_ID_MAP only covers IDs 1-19 from known data; remaining IDs need full platform currency lookup table
3. **Opening Balance**: resolves automatically with currency fix; CURRENCY_ID_MAP must be extended for non-USD/EUR accounts
4. **PM TRX Type variety**: PM-Transactions all "4. Transfer" — reference has 2. DP / 2. WD / 5. Fees but differentiating requires full bank-side parsing
5. **Rational tab**: needs both MT4 and PM totals complete before reconciliation analysis can be generated
6. **ZP WD→Transfer misclassification** (~4%): requires `payment_subtype` or `chb_status` analysis

---

## Verification Methodology

For future regression testing, these checks should be automated:

| Check | Method |
|-------|--------|
| Column names | Exact match against spec |
| Currency codes are 3-letter ISO | `currency.str.match(r'^[A-Z]{3}$')` |
| Deal No no .0 suffix | `deal_no.str.endswith('.0').sum() == 0` |
| PM codes are 2-3 letters | `pm.str.match(r'^[A-Z]{2,4}$')` |
| MT4-Transactions has no bank-only rows | Count rows with no CRM data |
| PM-Transactions has no CRM-only rows | Count rows with no bank data |
| CCY Lifecycle attributes are valid | All in the 18-attribute spec |
| Opening balance present for major accounts | Non-zero count in 1. Opening Balance rows |
| Closing balance = opening + movements | Sum check per (account, currency) |
