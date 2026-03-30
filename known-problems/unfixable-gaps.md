# Unfixable Gaps — Require Different Source Files

**Status:** Data gap — no code fix possible
**Impact:** ~279 rows (~11.6% of reconcilable CRM transactions) permanently unmatched with current data

After all code fixes (Nuvei, Korapay, SolidPayments) the projected match rate is ~84%. The remaining ~16% breaks down as follows.

---

## E. Wire Transfers with Bank References (~188 rows)

**payment_method:** Wire transfer
**psp_transaction_id:** Bank reference numbers like `0723646275-140155263`, `NEFT/...`

These are deposits/withdrawals made via bank wire (not through a PSP gateway). The CRM records a bank reference number as the `psp_transaction_id`. The matching counterpart would be in the bank's own statement file (Nedbank, ABSA, Standard Bank, SD Bank Group).

**Problem:** All January bank statement files are PDFs or have inconsistent CSV formats that the current parser can't read:
- `Nedbank Client Funds January.csv` — malformed CSV (quote/delimiter issues)
- `Standard all.csv` — malformed CSV
- `ABSA USD January.xls`, `Corp banks 1.23.xlsx` — may be parseable but don't contain transaction-level reference numbers matching the CRM format
- `Dixipay EUR B2C account.pdf`, `SD BANK GROUP USD.pdf`, `Standard Jan 2023 Zar.pdf` — PDFs

**To fix:** Export Nedbank/ABSA/Standard statements as clean CSVs with a column that matches the reference format the CRM records.

---

## F. CryptoWallet (~44 rows)

**payment_method:** CryptoWallet
**psp_transaction_id:** Either a wallet address (`USDT ERC20 account 0xe16bfe7...`) or a Finrax-style reference (`33894905 Finrax`)

Crypto withdrawals go through Finrax. The CRM records the blockchain wallet address in `psp_transaction_id` instead of a Finrax transaction reference. The Finrax export uses numeric `ReferenceNo` which has no relation to wallet addresses.

**To fix:** Finrax would need to provide an export that includes the wallet address (or the CRM needs to record the Finrax ReferenceNo for crypto transactions, not the wallet address).

---

## G. Skrill / Neteller ID Mismatch (~26 rows)

**Skrill:** CRM stores Skrill transaction IDs (e.g. `4583476678`) that do not appear in any column of the provided Skrill Processing or Skrill 2 CSV exports. The Skrill files appear to be settlement/batch exports, not transaction-level exports.

**Neteller:** CRM stores UUIDs (e.g. `3457166e-1a72-4c5a-9144-7a38d2ae6d85`) while Neteller files contain numeric transaction IDs (e.g. `4600214057`). These are entirely different reference systems.

**To fix:** Request transaction-level exports from Skrill and Neteller that include the same reference ID the CRM records.

---

## H. Credit Card, Blank Processor (~11 rows)

These rows have `psp_transaction_id` values that look like SafeCharge IDs (starting with `113...`) but have trailing tab characters (`\t`) in the CRM field and no `payment_processor` value. They appear to be data entry errors in the CRM where the processor field was left blank.

**To fix:** Clean up these CRM rows by filling in the `payment_processor` field. The trailing `\t` is already stripped by `normalize_key()`, so these would match Nuvei once the processor is populated.

---

## I. Other Small-Count Processors (~10 rows)

| Processor | Rows | Likely cause |
|---|---|---|
| AstroPay | 1 | No AstroPay PSP file provided |
| Powercash/VirtualPay variants | 4 | Name spelling variants not matching any uploaded file |
| LetKnow | 1 | Only 1 row in Letknow.xlsx (may not cover this transaction) |
| TrustPayments | 1 | File only covers a subset of January |

---

## Summary

| Category | Rows | Fix required |
|---|---|---|
| Wire transfers (bank refs) | ~188 | Clean bank CSV exports from Nedbank/ABSA/Standard |
| CryptoWallet | ~44 | Finrax export with wallet address column |
| Skrill/Neteller | ~26 | Transaction-level exports matching CRM reference format |
| Credit card, blank processor | ~11 | CRM data cleanup (fill payment_processor) |
| Other | ~10 | Obtain missing PSP files or fix CRM spelling |
| **Total** | **~279** | |
