Jan 2023
---

## Example 1 — Standard PSP match (Nuvei / SafeCharge)

The simplest case. The CRM holds the PSP's transaction ID exactly, and the PSP file
has a matching row. One-to-one, same currency, amounts agree.

**CRM** — `platform/CRM Transactions Additional info.xlsx`

| Field | Value |
|---|---|
| `transactionid` | `1061314622` |
| `psp_transaction_id` | **`1130000004097387874`** |
| `login` | `140856184` |
| `amount` | `4800` |
| `currency_id` | `1` (= USD) |
| `transactiontype` | `Deposit` |
| `payment_processor` | `SafeChargeS2S3Dv2` |

**PSP file** — `PSPs/Nuvei.xlsx` *(real header is at row 12 — 11 metadata rows come first)*

| Field | Value |
|---|---|
| `Transaction ID` | **`1130000004097387874`** |
| `Amount` | `4800` |
| `Currency` | `USD` |
| `Transaction Result` | `Approved` |
| `Date` | `2023-01-01 11:30:35` |

**Join:** `CRM.psp_transaction_id = Nuvei."Transaction ID"` → exact match.
Amounts agree. Match is clean.

---

## Example 2 — Fallback key match (Korapay)

Korapay does not echo the CRM's `psp_transaction_id` in its file. Instead, Korapay's
`payment_reference` column contains the CRM's own internal `transactionid`. This means
the primary join (Pass 1) finds nothing, and the fallback join (Pass 2) is required.

**CRM** — `platform/CRM Transactions Additional info.xlsx`

| Field | Value |
|---|---|
| `transactionid` | **`1061417432`** |
| `psp_transaction_id` | `39780225` *(does not appear anywhere in the Korapay file)* |
| `login` | `140793546` |
| `amount` | `67.21` |
| `currency_id` | `1` (= USD) |
| `payment_processor` | `KorapayAPM` |

**PSP file** — `PSPs/Korapay Pay-ins.csv`

| Field | Value |
|---|---|
| `payment_reference` | **`1061417432`** |
| `settlement_reference` | `KPY-SET-ATXm9KVO47O9mUp5` |
| `amount_paid` | `48,387.50` |
| `currency` | `NGN` |
| `status` | `success` |
| `transaction_date` | `2023-01-02 08:15:52` |

**Join:** Pass 1 (`CRM.psp_transaction_id` = `39780225`) finds zero matches.
Pass 2 (`CRM.transactionid` = `Korapay.payment_reference`) finds the match.

**Amount note:** CRM records USD `67.21`. Korapay records the NGN equivalent `48,387.50`.
The ratio is ~720 NGN/USD, which is a valid exchange rate — this is a cross-currency pair,
not a discrepancy. The reconciliation marks it matched but flags it for the currency
mismatch column.

---

## Example 3 — Wrong column selected (SolidPayments)

SolidPayments exports two ID columns. The obvious one (`TransactionId`) never matches
the CRM. The correct one (`UniqueId`) does.

**CRM** — `platform/CRM Transactions Additional info.xlsx`

| Field | Value |
|---|---|
| `transactionid` | `1061565730` |
| `psp_transaction_id` | **`8acda4a084ec1da6018576f7ca7b02d7`** |
| `login` | `140902444` |
| `amount` | `500` |
| `currency_id` | `1` (= USD) |
| `payment_processor` | `SolidPayments3DSv2` |

**PSP file** — `PSPs/Solidpayments.csv`

| Field | Value |
|---|---|
| `UniqueId` | **`8acda4a084ec1da6018576f7ca7b02d7`** |
| `TransactionId` | `1000-1061565730` *(looks like an ID but doesn't match anything in CRM)* |
| `PaymentType` | `DB` (debit = deposit) |
| `RequestTimestamp` | `2023-01-03 09:30:28` |

**Join:** `CRM.psp_transaction_id = SolidPayments.UniqueId` → match.

---

## Example 4 — Bank wire transfer (Nedbank Blackstone)

No PSP is involved. The client sent a ZAR bank wire directly to the company's Nedbank
account. The bank statement uses the client's MT4 login number as the payment reference,
which also appears verbatim in the CRM's `psp_transaction_id` field.

**CRM** — `platform/CRM Transactions Additional info.xlsx`

| Field | Value |
|---|---|
| `transactionid` | `140900854` |
| `psp_transaction_id` | **`140900854`** *(the login itself, used as the wire reference)* |
| `login` | `140900854` |
| `amount` | `568.59` |
| `currency_id` | `1` (= USD) |
| `payment_method` | `Wire transfer` |
| `bank_name` | *(blank — client sent from their own bank)* |

**Bank file** — `Banks/NedbankBlackstone January.csv`

| Field | Value |
|---|---|
| Reference (raw column) | **`140900854`** |
| Amount | `10,000.00` |
| Date | `03/01/2023` |
| Currency | ZAR |

**Join:** `CRM.psp_transaction_id = Bank.reference` (Pass 3 — bank-only pass).

**Amount note:** CRM records the USD equivalent `568.59`. Nedbank records the ZAR
amount received `10,000.00`. At ~17.6 ZAR/USD that's consistent — another cross-currency
pair. The reconciliation marks it matched and records `currency_match = cross_ccy`.

---

## Summary

| # | PSP / Bank | Join pass | Left key (CRM) | Right key (PSP/Bank) | Amount currency |
|---|---|---|---|---|---|
| 1 | Nuvei | Pass 1 (primary) | `psp_transaction_id` | `Transaction ID` | USD = USD |
| 2 | Korapay | Pass 2 (fallback) | `transactionid` | `payment_reference` | USD ≠ NGN (cross-ccy) |
| 3 | SolidPayments | Pass 1 (primary) | `psp_transaction_id` | `UniqueId` (not `TransactionId`) | USD = USD |
| 4 | Nedbank Blackstone | Pass 3 (bank) | `psp_transaction_id` | bank reference | USD ≠ ZAR (cross-ccy) |

### Why three passes?

A single JOIN would miss examples 2 and 4:

- **Pass 1** covers the majority of deposits — PSPs that echo the CRM's `psp_transaction_id`
- **Pass 2** covers PSPs (like Korapay) that reference the CRM's internal `transactionid` instead
- **Pass 3** covers bank wires, which have no PSP file at all — only a bank statement entry
