# How Reconciliation Actually Works — In Simple Terms

---

## The core idea

We have two independent records of the same money movement:

- The **CRM** says: "Client 140856184 deposited $4,800 via SafeCharge"
- **Nuvei's statement** says: "We processed a $4,800 transaction"

Reconciliation is just answering: **are these the same transaction?**

The answer is yes — if the ID in the CRM matches the ID in the Nuvei file.

---

## The main file: CRM Transactions Additional info.xlsx

This is the **source of truth** on our side. Every deposit and withdrawal the company knows about is in here. The columns we care about:

| Column | What it contains | Example |
|---|---|---|
| `login` | Client's trading account number | `140856184` |
| `psp_transaction_id` | The ID the PSP gave this transaction | `1130000004097387874` |
| `transactionid` | Our internal CRM ID for this transaction | `1061314622` |
| `amount` | How much (always positive) | `4800` |
| `payment_method` | How they paid | `Credit card` |
| `payment_processor` | Which PSP handled it | `SafeChargeS2S3Dv2` |
| `transactiontype` | Deposit or Withdraw | `Deposit` |

---

## Step 1 — Match CRM against PSP files

For each PSP, we open their statement file and look for a column that contains IDs matching `psp_transaction_id` in the CRM.

### Example: Nuvei

CRM says `psp_transaction_id = 1130000004097387874`

Nuvei file (`Nuvei.xlsx`) has a column called **`Transaction ID`**:
```
Transaction ID          Amount   Currency   Date
1130000004097387874     4800     USD        2023-01-15
```

**Match found.** The CRM row and the Nuvei row describe the same real deposit.

---

### Example: Zotapay

CRM row has `psp_transaction_id = 38635098` but that's actually Zotapay's `id` column (their internal order ID). The CRM `transactionid = 1061420033` matches Zotapay's `merchant_order_id`.

```
Zotapay file:
id          merchant_order_id   order_amount   order_currency
38635098    1061420033          31000          KES
```

So for Zotapay, the join goes:
- CRM `transactionid` → Zotapay `merchant_order_id`

This is why we have the overlap-detection logic — different PSPs use different column names for the same concept, and the CRM sometimes stores the PSP's ID in `psp_transaction_id` and sometimes in `transactionid`.

---

### Example: Korapay

CRM stores `psp_transaction_id = 39780225` (Korapay's internal order number).
Korapay file (`Korapay Pay-ins.csv`) has **`payment_reference`**:
```
payment_reference   amount_paid   currency   transaction_date
1065583320          19355         NGN        2023-01-31
```

Wait — those numbers don't match. That's because for Korapay, the CRM stores their number in `transactionid`, not `psp_transaction_id`:
```
CRM transactionid = 1061417432  →  Korapay payment_reference = 1061417432  ✓
```

---

### Example: Standard Bank (wire transfer)

A client wired money directly to the company bank account. No PSP involved.
The CRM records the client's own login number as the reference:
```
CRM psp_transaction_id = 140910682
```

Standard Bank PDF, after parsing, gives us:
```
date        description                  credit      reference       client_id
20230203    REAL TIME TRANSFER FROM      250,000     BOB 140910682   140910682
```

The `client_id` column (extracted from the reference line) = `140910682`.
Match found against CRM `psp_transaction_id`.

---

## Step 2 — What happens to each row

After checking all PSP and bank files, every CRM deposit/withdrawal row ends up in one of three buckets:

| Bucket | Meaning | Example cause |
|---|---|---|
| **Matched** | Found in a PSP/bank file | Nuvei `Transaction ID` = CRM `psp_transaction_id` |
| **Unmatched CRM** | In CRM but not in any PSP file | Missing PSP file, wrong ID format, wire with no bank CSV |
| **Unmatched Bank** | In PSP file but not in CRM | PSP processed it but CRM never recorded it |

---

## Step 3 — Check the amounts

For matched pairs, we compare amounts:
```
CRM amount:   4800  (USD)
Nuvei amount: 4800  (USD)
Difference:   0.00  ✓
```

But some PSPs report in local currency:
```
CRM amount:   31    (USD)
Zotapay:      31000 (KES)   ← ratio = 1000, not a discrepancy, just different currencies
```

So we only flag a real discrepancy when the ratio between the two amounts is close to 1:1 (within ±20%). Everything else is a currency difference, not an error.

---

## Step 4 — Output

From all the above we produce:

- **Lifecycle Report** — every CRM row enriched with its PSP match, deal number, client info, PM code, TRX type (the 52-column .xlsx that accounting uses)
- **Balances** — total equity per currency
- **Issues Report** — the three problem categories:
  - Unmatched deposits/withdrawals
  - Same-currency amount discrepancies
  - Cross-currency pairs that couldn't be compared fairly

---

## Full picture in one diagram

```
CRM Transactions Additional info.xlsx
│
│  psp_transaction_id ──────────────────────────────────────┐
│  transactionid ────────────────────────────────────┐      │
│  login, amount, payment_processor ...              │      │
│                                                    │      │
│                           ┌────────────────────────┘      │
│                           │    (secondary key)             │ (primary key)
│                           ▼                               ▼
│              ┌────────────────────┐         ┌────────────────────┐
│              │ Zotapay.csv        │         │ Nuvei.xlsx         │
│              │ merchant_order_id  │         │ Transaction ID     │
│              │ order_amount (KES) │         │ Amount (USD)       │
│              └────────────────────┘         └────────────────────┘
│
│              ┌────────────────────┐         ┌────────────────────┐
│              │ Korapay Pay-ins    │         │ Standard Bank PDF  │
│              │ payment_reference  │         │ client_id          │
│              │ amount_paid (NGN)  │         │ credit (ZAR)       │
│              └────────────────────┘         └────────────────────┘
│
│              + 16 more PSP files, each with their own column names
│
▼
Matched ──────────────────────────────────────────► Lifecycle Report
Unmatched CRM ────────────────────────────────────► Issues Report (Sheet 1)
Amount discrepancies ─────────────────────────────► Issues Report (Sheet 2)
Cross-currency pairs ─────────────────────────────► Issues Report (Sheet 3)
```

---

## Why it's hard

1. **Every PSP uses different column names** for the same thing — "transaction reference" is called `Transaction ID`, `payment_reference`, `merchant_order_id`, `UniqueId`, `ReferenceNo` depending on the provider.

2. **The CRM stores the PSP reference in two different columns** depending on the PSP — sometimes `psp_transaction_id`, sometimes `transactionid`. We have to try both.

3. **Amounts are in local currency** on the PSP side (KES, NGN, ZAR) but USD on the CRM side — so a $31 deposit looks like 31,000 KES and you can't just compare the numbers.

4. **Bank statements arrive as PDFs** with no standard format — we parse them but it's fragile.

5. **Some CRM rows have junk** in the reference field (amounts typed as IDs, wallet addresses, free text) — those can never match anything.

Once the ETL database exists, problems 1 and 2 go away entirely — each PSP's data is standardized into `clean.psp_transactions` with a single `reference_id` column before reconciliation even runs, and the join becomes one clean SQL query.