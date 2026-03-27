# Life Cycle Report — Comprehensive Tab-by-Tab Specification

**Reference file:** `relevant-data/Life cycle report/2023/1. January/Life Cycle Report-final.xlsx`
**Total sheets:** 9
**Total data rows:** ~36,500

---

## Architecture Overview

The report is a **dual-sided reconciliation workbook** that tracks money movements from two perspectives:

| Side | What it represents | Transaction tab | Lifecycle tabs |
|------|--------------------|-----------------|----------------|
| **MT4** | Trading platform (CRM) | MT4-Transactions | MT4 CCY Life Cycle, MT4 USD per acc Life Cycle |
| **PM** | Payment Method (PSP/Bank) | PM-Transactions | PM CCY Life Cycle, PM USD Life Cycle |

The **Rational** tab then reconciles the two sides to identify and explain differences.

All lifecycle tabs use an **Attribute-based aggregation** system to categorise money movements into numbered groups:

| Prefix | Category | Examples |
|--------|----------|----------|
| 1. | Opening/Closing | Opening Balance, Closing Balance |
| 2. | Core flows | DP (Deposits), WD (Withdrawals) |
| 3. | Timing adjustments | Timing Deposit, Timing Withdrawal, Transfer in Transit |
| 4. | Transfers/Movements | Transfer, Exchange, Internal Transfer, Opening Timings |
| 5. | P&L and adjustments | Fees/Charges, Realised Profits, Unrealised Profits, Bonuses, IB Payment, Realised Commissions, Realized Storage, Platform Balances, Over/Under-payment, Fee Compensation, Cl balance in Corporate Account, Transfer to/from Corporate Account, Payment Method Corrections |
| — | Computed | Total Movement, Exchange difference (USD) |

---

## Tab 1: MT4-Transactions

**Purpose:** Every individual transaction on the MT4/CRM side for the reporting period.

**Rows:** 10,254
**Columns:** 44

| # | Column | Type | Source | Notes |
|---|--------|------|--------|-------|
| 1 | Tran.Date | datetime | CRM `Month, Day, Year of confirmation_time` | Transaction date |
| 2 | Reference | string/number | CRM `psp_transaction_id` or `receipt` | PSP reference |
| 3 | Deal No | integer | CRM `mtorder_id` | MT4 deal number |
| 4 | Amount | float | CRM `amount` | In transaction currency |
| 5 | Commission | float | CRM | Usually 0 for deposits/WDs |
| 6 | Total | float | = Amount + Commission | |
| 7 | Currency | string | CRM `currency_id` (resolved to code) | USD, ZAR, EUR, etc. |
| 8 | AmntBC | float | CRM `usdamount` | Amount in base currency (USD) |
| 9 | CommissionBC | float | | Commission in USD |
| 10 | TotalBC | float | = AmntBC + CommissionBC | |
| 11 | Reason Code | string | | Internal classification code |
| 12 | Payment Method | string | CRM `payment_method` | 2-3 letter PM code (ZP, OZ, BT…) |
| 13 | Bank | string | CRM `bank_name` | |
| 14 | Institution | string | CRM `payment_processor` | |
| 15 | Details1 | string | CRM `mtorder_id` (as string) | |
| 16 | Details2 | string | | Additional reference |
| 17 | Comment | string | CRM `comment` | |
| 18 | Remarks | string | CRM `first_name` + `last_name` | Client name |
| 19 | Exch.Diff. % | float | | Exchange rate difference percentage |
| 20 | Take To Profit | boolean | | Whether transaction should be taken to profit |
| 21 | Unrecon. Fees | float | | Unreconciled fee amount |
| 22 | Match No | integer | | Sequential match group number |
| 23 | Recon.Reason Group | string | | Matched / Unmatched / Bank Only |
| 24 | Recon Reason | string | | Detailed match reason |
| 25 | IsTiming | boolean | | True = orphan/timing difference |
| 26 | MatchCount | integer | | 1 if matched, 0 otherwise |
| 27 | EOD | | | End of day marker |
| 28 | IsEODTran | boolean | | |
| 29 | ClientAccount | integer | CRM `tradingaccountsid` | Trading account ID |
| 30 | ReasonCodeD | string | | Reason code description |
| 31 | CategoryCode | string | | Deposit / Withdrawal / etc. |
| 32 | Reason Code Group | string | | |
| 33 | MRS Notes | string | | Analyst notes |
| 34 | Country | string | | |
| 35 | IsSeggr | boolean | | Segregated flag |
| 36 | PlatformName | string | | Platform identifier |
| 37 | Country_1 | string | | Secondary country field |
| 38 | ClientType | string | | |
| 39 | ClientGroup | string | | |
| 40 | Matched By | string | | Analyst initials (CGH) or engine (MRS) |
| 41 | ReasonCodeGroupName | string | | |
| 42 | Matched On | datetime | | When the match was performed |
| 43 | Index | integer | | Sequential row number (1-based) |
| 44 | TRX Type | string | | Category: "2. DP", "2. WD", "5. Fees/Charges", "5. Unrealised Profits", etc. |

**Key observation:** `TRX Type` uses the same attribute numbering system as the lifecycle tabs. This column drives the lifecycle aggregation.

---

## Tab 2: MT4 CCY Life Cycle

**Purpose:** Per-account, per-currency aggregation of all MT4 transactions into lifecycle attributes. This is the **local currency** view.

**Rows:** 19,308
**Columns:** 4

| Column | Type | Notes |
|--------|------|-------|
| Client Account | integer | Trading account ID |
| Currency | string | USD, ZAR, EUR, NGN, etc. |
| Attribute | string | One of 18 lifecycle categories (see list below) |
| Amount | float | Sum in local (transaction) currency |

**Attribute values (18 distinct, in order):**

1. `1. Opening Balance`
2. `2. DP`
3. `2. WD`
4. `3. Timing Deposit`
5. `3. Timing Withdrawal`
6. `4. Transfer`
7. `5. Accumulated P&L`
8. `5. Bonuses`
9. `5. Fee Compensation`
10. `5. Fees/Charges`
11. `5. IB Payment`
12. `5. Over/Under - payment`
13. `5. Platform Balances`
14. `5. Realised Commissions`
15. `5. Realised Profits`
16. `5. Realized Storage`
17. `5. Unrealised Profits`
18. `Total Movement`

**Aggregation logic:**
- One row per (Client Account, Currency, Attribute) triple
- `Amount` = `SUM(MT4-Transactions.Amount)` where `ClientAccount` and `Currency` match, grouped by `TRX Type` → `Attribute`
- `Total Movement` = sum of all non-opening attributes
- `1. Opening Balance` comes from the equity/balance report (external data)

---

## Tab 3: MT4 USD per acc Life Cycle

**Purpose:** Same as MT4 CCY Life Cycle but all amounts are converted to USD.

**Rows:** 14,839
**Columns:** 4

| Column | Type | Notes |
|--------|------|-------|
| Client Account | integer | |
| Currency | string | Original transaction currency |
| Attribute | string | One of 17 categories |
| Amount USD | float | Sum converted to USD |

**Differences from MT4 CCY:**
- Uses `1. Opening Before (USD)` instead of `1. Opening Balance`
- Uses `Exchange difference (USD)` instead of `Total Movement` (shows the FX conversion impact)
- Does **not** include `5. Accumulated P&L`
- Amounts use `AmntBC` (base currency = USD) from MT4-Transactions

---

## Tab 4: Rational

**Purpose:** The reconciliation analysis sheet. Compares the MT4 side against the PM side to identify and explain differences.

**Rows:** ~2,384
**Structure:** Multi-section layout (not a simple table)

### Section A (Columns A–B): PSP/Banks Total Summary
Aggregate amounts for each lifecycle attribute across all PSPs:
- Opening Before (USD): 7,114,587.49
- 2. DP: 2,601,439.55
- 2. WD: -967,672.49
- 3. Timing Withdrawal: -139.58
- 3. Transfer in Transit: -64,403.34
- 4. Exchange: -11,154.72
- 4. Internal Transfer: -3,106.33
- 4. Opening Timings: 66,494.02
- 4. Transfer: -2,179.38
- 5. Cl balance in Corporate Account: -223,981.06
- 5. Fees/Charges: -60,880.57
- 5. IB Payment: -41,128.02
- 5. Over/Under - payment: -877.12
- 5. Payment Method Corrections: 37.75
- 5. Transfer to/from Corporate Account: -1,217,787.36
- Exchange difference (USD): -42,557.91
- **Closing Balance: 4,761,987.09**

### Section B (Columns E–G): MT4 Total Summary
Same structure but from MT4 perspective.

### Section C (Columns L–P): Per-PM Reconciliation Table
Shows each Payment Method code (SC, SLP, DRC, SKR, ZP, INA, SW, OZ, EFT…) with:
- PM amount (USD)
- MT4 amount (USD)
- Difference

### Section D (Rows 65+): Detailed Match Breakdown
Individual transactions with Match No, PM Amount, MT4 Amount, difference.

**This tab contains the flowchart diagram** (visible in screenshots) showing the decision logic for how PM transactions are categorised.

---

## Tab 5: PM USD Life Cycle

**Purpose:** Payment Method lifecycle summary in USD. One row per (Payment Method, Attribute).

**Rows:** 166 (data starts at row 14)
**Named table:** `Table_PM_USD_per_PSP_Life_Cycle_Summary`

| Column | Type | Notes |
|--------|------|-------|
| Payment Method | string | Full PSP name (AstroPay, Banks, Directa24, etc.) |
| Attribute | string | Lifecycle category |
| Amount USD | float | |

**Rows 1–13:** Empty / slicer placeholders (the reference file has Excel slicers/filters above the data).

**Attributes include:** `1. Opening Balance USD`, `2. DP`, `2. WD`, `3. Transfer in Transit`, `4. Exchange`, `4. Internal Transfer`, `4. Opening Timings`, `4. Transfer`, `5. Cl balance in Corporate Account`, `5. Fees/Charges`, `5. IB Payment`, `5. Over/Under - payment`, `5. Payment Method Corrections`, `5. Transfer to/from Corporate Account`, `Exchange difference (USD)`, `6. Closing Balance`

**Note:** Has `6. Closing Balance` row — computed as the sum of all other attributes.

---

## Tab 6: PM CCY Life Cycle

**Purpose:** Payment Method lifecycle in local currency, broken down by currency. One row per (Payment Method, Currency, Attribute).

**Rows:** 349 (data starts at row 22)
**Named table:** `Table_PM_CCY_Life_Cycle_Summary`

| Column | Type | Notes |
|--------|------|-------|
| Payment Method | string | Full PSP name |
| Currency | string | USD, ZAR, EUR, NGN, etc. |
| Attribute | string | Lifecycle category |
| Value | float | Amount in local currency |

**Rows 1–21:** Empty / slicer placeholders with Payment Method and Currency slicers (visible in screenshots showing filter buttons for PSP names and currency codes).

**Same attribute set as PM USD Life Cycle** but amounts are in the transaction's native currency.

---

## Tab 7: PM-Transactions

**Purpose:** Every individual PSP/bank-side transaction for the reporting period. This is the counterpart to MT4-Transactions.

**Rows:** 3,461
**Columns:** 31

| # | Column | Type | Notes |
|---|--------|------|-------|
| 1 | Index | integer | Sequential row number |
| 2 | Tran.Date | date | Transaction date (dd/mm/yyyy format) |
| 3 | Reference | string | Bank/PSP reference (e.g. "1061710220/Payout") |
| 4 | Amount | float | In transaction currency (e.g. -17000 ZAR) |
| 5 | Currency | string | Transaction currency |
| 6 | AmntBC | float | Amount in base currency (USD) |
| 7 | Payment Method | string | 2-letter PM code (OZ, BT, NED, etc.) |
| 8 | Bank | string | Bank name |
| 9 | Details1 | string | Client name |
| 10 | Details2 | string | Account ID |
| 11 | Comment | string | Bank reference / voucher code |
| 12 | Remarks | string | Account ID |
| 13 | ExcRate | float | Exchange rate used |
| 14 | Exch.Diff. % | float | Exchange rate difference |
| 15 | ToEmail | string | |
| 16 | TranStatus | string | "Settlement" for bank-side |
| 17 | Reference2 | string | UUID-format bank settlement reference |
| 18 | Take To Profit | boolean | False for bank-side entries |
| 19 | ReasonCodeD | string | |
| 20 | MRS Notes | string | |
| 21 | Recon.Reason Group | string | "Contra", "TheirToTheir" |
| 22 | Recon Reason | string | "Manual" |
| 23 | Match No | integer | Links to MT4-Transactions Match No |
| 24 | ReasonCodeGroupName | string | |
| 25 | TRX Type | string | "4. Transfer" for all PM entries |
| 26 | PM Name | string | Full PSP name (Ozow, Banks, etc.) |
| 27 | PM-Cur | string | Composite code (OZ-ZAR, BT-ZAR, etc.) |
| 28 | Is Balance Currency | boolean | Whether this is the PSP's balance currency |
| 29 | Balance Currency | string | The PSP's balance/settlement currency |
| 30 | Amount in Bal Curr | float | Amount converted to PSP's balance currency |
| 31 | Amount USD | float | Amount converted to USD |

**Key observations:**
- `Reference` format varies by PSP: "1061710220/Payout" (Ozow), "CAPITEC 1063769604" (Banks), "GC-1061451359" (Banks), etc.
- `Recon.Reason Group` is either "Contra" or "TheirToTheir" — categorising the type of PM match
- `TRX Type` is always "4. Transfer" — all PM entries are transfers into/out of PSP accounts
- `PM-Cur` encodes both PM code and currency: "OZ-ZAR", "BT-ZAR", "NED-USD"

---

## Tab 8: Mapping Rules

**Purpose:** The PM Code → PM Name lookup table with currency configuration. This is the **master reference** for how PSP accounts are coded and which currencies they operate in.

**Rows:** ~78 entries (data starts at row 5)
**Row 1:** SharePoint file path
**Row 4:** Label "to convert PM in List report"

| Column | Type | Notes |
|--------|------|-------|
| PM Code | string | 2-4 letter abbreviation |
| PM Name | string | Full PSP name |
| PM-Cur | string | Composite: "{PMCode}-{Currency}" |
| Is Balance Currency | boolean | True if this is the PSP's balance/settlement currency |
| Balance Currency | string | The currency used for PSP settlement |
| PM-Bal-Cur | string | Standardised balance currency code |
| Processing Currency | string | Currency for processing |
| Amount factor | integer | Multiplier (1 for most; 1,000,000 for BTC) |

**Complete PM Code inventory (from reference file):**

| Code | Name | Currencies |
|------|------|------------|
| ASP | AstroPay | USD |
| BT | Banks | ZAR, USD, EUR, NGN, AED |
| CEL | Celluland | ZMW, GHS, KES, UGX |
| DRC | Directa24 | COP, USD, MXN |
| EFT | EFTPay | ZAR |
| FRX | Finrax | USDT, EUR |
| HVN | Hayvn | USD, USDT |
| KP | KoraPay | NGN |
| LKP | LetKnow Pay | USD, USDT, BTC |
| NT | Neteller CMT Processing | USD |
| NTT | Neteller GCMT South Africa | USD |
| SC | Nuvei | USD, EUR, TRY, KES, UGX, ZMW, GHS, ZAR, BWP, AED, GBP |
| RR_SC | Nuvei RR | EUR |
| OZ | Ozow | ZAR |
| PS | PagSmile | MXN, COP |
| INA | Payabl | USD, ZAR, EUR, GHS, AED, ZMW |
| RR_INA | Payabl RR | EUR |
| PMo | PerfectMoney | USD |
| SKR | Skrill | USD |
| SLP | SolidPayments | USD, EUR |
| RR_SLP | SolidPayments RR | EUR |
| SW | Swiffy | ZAR, USD |
| RR_SW | Swiffy RR | USD |
| TP | TrustPayments | EUR |
| RR_TP | Trustpayments RR | EUR |
| VP | Virtual Pay | USD, UGX, KES, TZS |
| RR_VP | Virtual Pay RR | USD |
| ZP | Zotapay | USD, IDR, NGN, MXN, ZAR, PHP, VND, GHS, TZS |
| RR_ZP | Zotapay RR | USD |
| VLT | Vaultspay | AED, USD |

**Note:** `RR_` prefix denotes "Rolling Reserve" accounts — separate from the main PSP account.

---

## Tab 9: Documentation

**Purpose:** Process steps and lifecycle category definitions.

**Content:**
1. **Steps:**
   - 1. Unpivot balance report and export
   - 2. Save it in folder as "Balance"
   - 3. Export Transaction list only for archived periods

2. **Lifecycle Category Definitions:**

| Category | Description |
|----------|-------------|
| 1. Opening Balance | Account balance at start of period |
| 2. DP | Deposits received |
| 2. WD | Withdrawals processed |
| 3. Timing Deposit/Withdrawal | Matched deposits/withdrawals with timing differences |
| 4. Transfer | Transfers between accounts |
| 4. Internal Transfer | Transfers within the same PSP |
| 4. Exchange | Currency exchange transactions |
| 5. Fees/Charges | Processing fees charged by PSPs |
| 5. Realised Profits | Closed trading position profits |
| 5. Unrealised Profits | Open trading position mark-to-market |
| 5. Realised Commissions | Trading commissions |
| 5. Realized Storage | Swap/overnight charges |
| 5. Bonuses | Promotional bonuses |
| 5. IB Payment | Introducing Broker payments |
| 5. Fee Compensation | Fee refunds/adjustments |
| 5. Platform Balances | Platform-level balance adjustments |
| 5. Over/Under-payment | Payment amount discrepancies |
| 6. Closing Balance | = Opening + all movements (computed) |

---

## Relationship Between Tabs

```
MT4-Transactions  ──(aggregate by ClientAccount, Currency, TRX Type)──▶  MT4 CCY Life Cycle
                  ──(aggregate by ClientAccount, Currency, TRX Type, convert to USD)──▶  MT4 USD per acc Life Cycle

PM-Transactions   ──(aggregate by PM Name, Currency, TRX Type)──▶  PM CCY Life Cycle
                  ──(aggregate by PM Name, TRX Type, convert to USD)──▶  PM USD Life Cycle

Mapping Rules     ──(PM Code → PM Name, currency config)──▶  PM-Transactions (PM-Cur, Balance Currency, etc.)

MT4 totals vs PM totals  ──(compare by PM)──▶  Rational (reconciliation analysis)

Match No links MT4-Transactions ↔ PM-Transactions (the bipartite join key from reconciliation)
```

---

## What Our Engine Currently Generates vs What It Should Generate

| Tab | Reference has | We generate | Gap |
|-----|---------------|-------------|-----|
| MT4-Transactions | 10,254 rows × 44 cols | Yes (44 cols) | TRX Type mapping needs refinement; no Reason Code logic yet |
| MT4 CCY Life Cycle | 19,308 rows × 4 cols | Partial (DP/WD/Transfer/Timing only) | Missing: Opening Balance (needs equity file), P&L attributes (5.*), Total Movement |
| MT4 USD per acc Life Cycle | 14,839 rows × 4 cols | Partial (same gaps) | Missing: Opening Before (USD), Exchange difference |
| Rational | Complex multi-section | Not generated | Needs both MT4 and PM lifecycle totals first |
| PM USD Life Cycle | 166 rows × 3 cols | Empty tab | Needs PM-Transactions aggregated by PM Name |
| PM CCY Life Cycle | 349 rows × 4 cols | Empty tab | Needs PM-Transactions aggregated by PM Name + Currency |
| PM-Transactions | 3,461 rows × 31 cols | Partial (populated from bank files) | Missing: PM Name/PM-Cur mapping (needs Mapping Rules), TranStatus, Reference2, ExcRate |
| Mapping Rules | 78 entries × 8 cols | Metadata only | Needs the full PM Code table |
| Documentation | Process steps | Not generated | Static content; can be copied from reference |
