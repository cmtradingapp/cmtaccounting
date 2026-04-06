# Data Sources — Simple Reference

Everything we need, where it comes from, and which files represent it.

---

## accounts
**What it is:** One row per client trading account.
**Comes from:** CRM (Gil) + MT4 (Ibrahim/Leonidas)
**Current files:**
- `platform/Accounts Report.csv` — CRM account list
- `platform/Client Balance check.xlsx` — balances per account
**Future:** Direct DB read from CRM and MT4.

---

## transactions (deposits / withdrawals / credits)
**What it is:** Every money movement a client made — deposits, withdrawals, bonuses, fees.
**Comes from:** CRM (Gil)
**Current files:**
- `platform/CRM Transactions Additional info.xlsx` — the main file we use for reconciliation. Contains `psp_transaction_id`, `amount`, `payment_method`, `payment_processor`, `login`, etc.
- `platform/Deposit and Withdrawal Report.csv` — simpler version, fewer columns
**Future:** Direct DB read from CRM.

---

## psp_transactions
**What it is:** The same transactions, but seen from the PSP's side — what they actually processed and settled.
**Comes from:** ~20 PSPs, currently via manual download. Eventually via Praxis API.
**Current files (Jan 2023 examples):**

| PSP | Files |
|---|---|
| Nuvei / SafeCharge | `PSPs/Nuvei.xlsx` |
| Zotapay | `PSPs/Zotapay.csv`, `PSPs/Zota Operations.csv` |
| Korapay | `PSPs/Korapay Pay-ins.csv`, `PSPs/Korapay Payouts.csv` |
| SolidPayments | `PSPs/Solidpayments.csv`, `PSPs/Solidpayment fees.xlsx` |
| EFTpay | `PSPs/EFTpay 1.csv`, `PSPs/EFTpay 2.csv` |
| Finrax | `PSPs/Finrax all.xlsx`, `PSPs/Finrax - USD.csv`, `PSPs/Finrax - EUR.csv` |
| Ozow | `PSPs/Ozow Deposits.csv`, `PSPs/Ozow Refunds.csv` |
| Neteller | `PSPs/Neteller group EEA.csv`, `PSPs/Neteller group ROW.csv`, `PSPs/Neteller processing EEA.csv`, `PSPs/Neteller processing ROW.csv` |
| Skrill | `PSPs/Skrill Processing.csv`, `PSPs/Skrill 2.csv` |
| VirtualPay | `PSPs/VP Deposits.csv`, `PSPs/VP Refunds.csv` |
| TrustPayments | `PSPs/TrustPayments.csv` |
| Directa24 | `PSPs/Directa24.csv` |
| Inatec | `PSPs/Inatec.csv` |
| Swiffy | `PSPs/Swiffy.csv` |
| LetKnow | `PSPs/Letknow.xlsx` |
| Payabl | `PSPs/Payabl/` (subfolder) |

**Owner:** Despina's team downloads these manually today. They should come through Praxis API.

---

## bank_transactions
**What it is:** Wire transfer movements through the company's corporate bank accounts. Used to match client deposits/withdrawals that went via bank transfer instead of a PSP gateway.
**Comes from:** 4 banks — downloaded manually by Despina's team. No API confirmed yet.
**Current files:**

| Bank | Files |
|---|---|
| ABSA | `Banks/ABSA USD January.xls` |
| Nedbank | `Banks/Nedbank Client Funds January.csv`, `Banks/NedbankBlackstone January.csv` |
| Standard Bank | `Banks/Standard Jan 2023 Zar.pdf`, `Banks/Standard all.csv` |
| SD Bank Group | `Banks/SD BANK GROUP USD.pdf` |
| Dixipay | `Banks/Dixipay EUR B2C account.pdf` |
| Other (corp) | `Banks/Corp banks 1.23.xlsx`, `Banks/Corp banks to import.xlsx` |

**Note:** Several are PDFs. We can parse most of them but CSV/XLSX from the bank is far more reliable. Despina needs to check if any of the 4 banks offer API access or at minimum email delivery.

---

## trades_open / trades_closed
**What it is:** Actual trading activity — every position a client opened or closed on the platform (forex, CFDs, etc.).
**Comes from:** MT4 (Ibrahim/Leonidas)
**Current files:** Not in the MRS dataset yet. Currently unknown — need to ask Ibrahim/Leonidas for export format and access method.
**Future:** Direct DB read from MT4.

---

## client_equity
**What it is:** Snapshot of each client's account value at a point in time — balance, equity, margin usage.
**Comes from:** MT4 raw data, shaped by Elis's rules. Currently exported via Tableau.
**Current files:**
- `web-gui/uploads/equityFile.csv` — the manually uploaded equity report we use in reconciliation today
- Reference versions in `relevant-data/MRS/` per month
**Future:** Generated automatically from MT4 raw data using Elis's documented rules. No manual Tableau export.

---

## agents / commissions
**What it is:** IB (Introducing Broker) agents and the commissions they earn on client activity.
**Comes from:** Proline system
**Current files:** Not in the MRS dataset. Proline is a separate system — need to establish connection.
**Future:** Direct read from Proline DB or API.

---

## fx_rates
**What it is:** Daily exchange rates between currency pairs, used to convert amounts to USD/EUR for comparison.
**Comes from:** Frankfurter API (ECB rates, 28 fiat pairs) + CoinGecko (5 crypto pairs)
**Current files:** Fetched live at runtime. Also several rate files in the platform folder:
- `platform/Rates.xlsx`, `platform/Rates 2.xlsx`, `platform/Rates 3.xlsx` — historical manual rate tables (superceded by the live API)
**Owner:** Already automated. No action needed.

---

## psp_schema_registry
**What it is:** Config table that tells the system which column in each PSP file is the transaction reference, amount, date, etc.
**Comes from:** Built by us (Aleh), maintained by Despina's team when PSPs change formats.
**Current files:** Hardcoded in `web-gui/server.py` today. Needs to move to a DB table with a web UI.
**Owner:** Technical (setup), Accounting (maintenance).

---

## reconciliation / lifecycle_report / balances
**What it is:** The output. The matched/unmatched result, the 52-column Lifecycle List, and the currency balance summary.
**Comes from:** Generated by the reconciliation engine from all sources above.
**Current files:** Downloaded manually from MRS web app today:
- `Lifecycle List YYYY-MM-DD.xlsx`
- `Balances YYYY-MM-DD.xlsx`
- `Issues YYYY-MM-DD.xlsx`
**Future destination:** Automatically deposited to the corporate OneDrive folder in date-named subfolders.
**Owner:** Despina / Accounting consume them. Aleh's system produces them.

---

## Summary table

| DB Table | Source System | Who provides it | How we get it today | How we'll get it |
|---|---|---|---|---|
| accounts | CRM + MT4 | Gil, Ibrahim/Leo | Manual export | DB read |
| transactions | CRM | Gil | Manual .xlsx export | DB read |
| psp_transactions | ~20 PSPs | Despina's team | Manual download | Praxis API |
| bank_transactions | 4 banks | Despina's team | Manual download (PDFs/CSV) | Email parse or API |
| trades_open/closed | MT4 | Ibrahim/Leonidas | Not in system yet | DB read |
| client_equity | MT4 via Tableau | Elis | Manual Tableau export | Generated from MT4 |
| agents/commissions | Proline | ? | Not in system yet | Proline DB/API |
| fx_rates | Frankfurter/CoinGecko | External API | Already automated | Same |
| psp_schema_registry | Built by us | Aleh / Despina | Hardcoded in Python | DB table + web UI |
| reconciliation/reports | Generated | Aleh's system | Manual clicks + download | Scheduled, auto-delivered |
