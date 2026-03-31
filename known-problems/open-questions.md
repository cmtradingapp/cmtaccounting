# MRS — Open Data Science Questions

**Context:** The reconciliation system achieves ~84.5% match rate on January 2023 data. The remaining gap is partly unfixable (missing source files, crypto wallets, wrong PSP export types) but a significant portion is structural: the system is built on heuristics that break when PSPs change their export format, rename columns, or introduce new schemas. The PSP landscape analysis shows 606 unique column schemas across 17 months, HIGH naming volatility, and directory structure changes month-to-month. This document catalogs every open question by subsystem so they can be worked on individually.

---

## 1. SCHEMA DETECTION — Column Mapping

> *Core question: How do we find the right join-key column in a file we've never seen before?*

**Q1.1** — How many distinct "transaction reference" column naming patterns exist across all PSP exports in the dataset? Is the current priority list (`transactionreference`, `merchantreference`, `uniqueid`, `paymentreference`, etc.) complete, or are there patterns it misses?

**Q1.2** — Given that `_detect_bank_ref_cols()` returns candidates ranked by a hard-coded priority list, then picks the one with highest CRM overlap: what is the false-positive rate of the overlap selection? (i.e., how often does highest-overlap == wrong column?)

**Q1.3** — Can we learn per-PSP column schemas from past reconciliations and store them as a lookup table? If `Nuvei.xlsx` → `Transaction ID` has been confirmed as the join key 12 months in a row, should month 13 skip the detection heuristics entirely?

**Q1.4** — What is the minimum overlap count needed to trust a column selection? Is 5 matches sufficient? What threshold separates "genuine join key" from "coincidental numeric overlap"?

**Q1.5** — When a PSP renames a column (e.g., Nuvei `Transaction ID` → `PPP Order ID`), how do we detect the rename vs. a genuine schema change? Can embedding similarity between old and new column names help?

**Q1.6** — How do we detect when `_detect_bank_ref_cols()` returned the wrong column? Right now a wrong detection produces 0 matches silently. What signal could expose this?

---

## 2. KEY NORMALIZATION — Join Robustness

> *Core question: Two records refer to the same transaction. What transformations make their IDs match?*

**Q2.1** — What is the full taxonomy of ID formats used across all PSPs? (UUIDs, numeric sequences, alphanumeric with dashes, `DX`-prefixed, `KPY-`-prefixed, SWIFT references, login IDs, etc.) For each format, what normalization is required?

**Q2.2** — The current `normalize_key()` strips leading zeros. What fraction of IDs across the dataset have leading zeros that are genuinely significant vs. padding artifacts? Is stripping them safe across all PSPs?

**Q2.3** — The current normalizer does NOT strip embedded whitespace (e.g., `"TXN 12345"` vs `"TXN12345"`). How many unmatched pairs in the dataset would match if embedded whitespace were stripped?

**Q2.4** — Null-like values: CRM uses `None`, some PSPs use `"N/A"`, `"NULL"`, `"-"`, `"0"`. How many unmatched CRM rows have a `psp_transaction_id` that is a null-like string rather than true null? Would expanding the null-synonym list recover matches?

**Q2.5** — How often do IDs in the same PSP file appear with inconsistent formatting across rows (e.g., `"00012345"` in one row and `"12345"` in another)? Is this a CRM data quality issue or a PSP export issue?

---

## 3. PSP FILE LOADING — Format Resilience

> *Core question: How many distinct file format patterns exist, and can we load all of them without bespoke code per provider?*

**Q3.1** — Across 606 unique column schemas in the dataset: how many require the existing auto-detection to select a non-standard header row (like Nuvei's 11-row preamble)? How many would still fail with the current `_load_psp_file` logic?

**Q3.2** — The Excel header auto-detection scans the first 20 rows. What is the distribution of metadata row counts across all Excel files in the dataset? Is 20 rows sufficient, or should this be extended?

**Q3.3** — European number format (`1.234,56` — comma as decimal separator, period as thousands separator) currently breaks `_bank_amount` parsing. How many PSP files in the dataset use European formatting? Which PSPs/currencies are affected?

**Q3.4** — The PDF extraction has 3 strategies. Strategy 2 (Standard Bank text parser) has a hardcoded `r'\b(1[34]\d{6,8})\b'` regex for client IDs. How many other PDFs in the dataset would benefit from a more general client-ID extraction pattern? What are the other ID formats used?

**Q3.5** — 411 PDF files exist across the dataset (12% of all files). What fraction are parseable by pdfplumber vs. scanned images? For scanned images, is OCR feasible or is the correct answer "request CSV from bank"?

**Q3.6** — File naming volatility is HIGH (Zotapay `Zotapay.csv` → `Orders 2023-3.csv`, Nuvei typos). The system matches PSP files to PM codes via `_PROCESSOR_TO_PM_CODE` which is keyed on filename keywords. How many months in the dataset would produce incorrect PM code assignments due to renamed files?

---

## 4. CURRENCY & AMOUNT HANDLING

> *Core question: Can we know the transaction currency for both CRM and bank sides, and compare amounts fairly?*

**Q4.1** — The current unrecon filter (ratio 0.8–1.2) conflates "amount in different currency" with "genuine discrepancy". What is the correct approach: compare `usdamount_crm` (CRM's pre-converted USD value) against the bank amount converted to USD via the live FX rate?

**Q4.2** — For each PSP in the dataset, what currency do they report amounts in — native transaction currency, settlement currency, or USD? Is this consistent across months or does it change?

**Q4.3** — The `_detect_bank_amount_col()` priority list does not distinguish between gross, net, and settlement amounts. For PSPs that report multiple amount columns (e.g., Zotapay has `order_amount`, `effective_order_amount`, `amount_to_merchant_balance`, `txn_fee_amount`), which column best matches CRM `amount` for reconciliation purposes?

**Q4.4** — What is the distribution of the `unrecon_fees` amount across matched pairs after the currency filter? Are there systematic discrepancies for specific PSPs (e.g., Finrax shows $67K diff — is this due to FX rates, fees, or refunds)?

**Q4.5** — The ratio filter threshold (0.8–1.2) is hardcoded. For currency pairs present in the dataset (USD/ZAR, USD/KES, USD/NGN, USD/COP, USD/EUR), what would be the historically observed min/max ratio for same-day conversions? This would let us set a data-driven threshold per PSP.

---

## 5. MATCH QUALITY ASSESSMENT

> *Core question: How do we know if a match is correct, not just that a match was found?*

**Q5.1** — The system matches purely on reference ID. For matched pairs where the amount ratio is outside 0.8–1.2, what fraction are genuine matches with a currency difference vs. false positives (different transactions that share the same reference ID)?

**Q5.2** — What is the false-positive rate of the current join? I.e., what fraction of "matched" rows actually correspond to different real-world transactions that happen to share a reference ID? (A short reference ID namespace increases collision probability.)

**Q5.3** — For the 375 unmatched CRM D/W rows remaining after all fixes: what is the breakdown by root cause? (Missing PSP file, ID format mismatch, CRM data quality, PSP file has wrong date range, etc.) Can each root cause be programmatically detected?

**Q5.4** — When a CRM row is matched by the "first PSP that processes it" (first-wins ordering), how often is this actually the wrong PSP? Could a post-hoc validation step (compare CRM `payment_processor` field against the matched PSP filename) detect misattributions?

**Q5.5** — What is the precision/recall of the `crm_psp_total` denominator? If `_map_trx_type()` misclassifies internal rows as `2.DP`/`2.WD` (or vice versa), the match rate percentage is wrong. How many such misclassifications exist in January 2023 data?

---

## 6. PSP SCHEMA VOLATILITY — Detecting & Surviving Change

> *Core question: How do we know when a PSP changed their export format, and how do we not break?*

**Q6.1** — How many PSP providers exist in the dataset? What is the total number of distinct (provider, month) combinations with at least one file?

**Q6.2** — For each provider, how often does the column schema change month-to-month? Can we define a "schema stability score" (e.g., % of columns preserved) and rank providers by it?

**Q6.3** — When a schema change occurs (new column added, column renamed, column removed), how does it affect match rate for that month? Is there a detectable drop in match rate that could serve as an alert?

**Q6.4** — What is the minimum set of "anchor columns" that every PSP export must contain for the system to function? (A reference column + an amount column + a date column?) Could we define a PSP file validation schema that fails fast if anchors are missing?

**Q6.5** — Can we build a "schema registry" that stores the last-known column layout for each PSP and flags deviations? What metadata would this registry need to store?

**Q6.6** — Some column renames are semantically equivalent (e.g., `TransactionID` → `Transaction ID` → `TxID`). Can embedding-based column matching (e.g., small LLM, sentence transformers) replace the hard-coded priority list and generalize to unseen column names?

---

## 7. CRM DATA QUALITY

> *Core question: How clean is the CRM data, and where does poor CRM quality cause unmatched rows?*

**Q7.1** — What fraction of CRM Deposit/Withdrawal rows have a null or blank `psp_transaction_id`? What fraction have a value that looks like a different field (login ID, ZAR amount string, wallet address)?

**Q7.2** — How many CRM rows have a `psp_transaction_id` that does not appear in any PSP file for the same month, but DOES appear in a PSP file for an adjacent month (±1 month)? These could be timing mismatches (deposit recorded in CRM in January but PSP file is February).

**Q7.3** — How many CRM rows have `payment_processor` blank but a non-null `psp_transaction_id` that looks like a known PSP's ID format (e.g., `113XXXXXXXXXXXXXXXXX` = SafeCharge)? Could we backfill `payment_processor` from the ID format?

**Q7.4** — The `currency_id` in CRM is a numeric FK (1, 2, ...), not a 3-letter code. What is the complete mapping from `currency_id` → ISO code? The current code uses a partial lookup; how many transactions have an unmapped `currency_id`?

**Q7.5** — What fraction of unmatched rows in each month are attributable to CRM data entry errors vs. genuinely missing PSP files?

---

## 8. SYSTEM ARCHITECTURE — Resilience & Observability

> *Core question: How do we make the system fail loudly and recover gracefully instead of silently degrading?*

**Q8.1** — The system currently has no confidence score per match. Could we define one? Candidate signals: overlap %, amount ratio closeness, date proximity, CRM `payment_processor` matching PSP filename. A confidence score would allow "high-confidence auto-match" vs. "low-confidence manual review" workflows.

**Q8.2** — The reconciliation state is stored as a single pickle file (`_recon_state.pkl`). If two users run reconciliation simultaneously, the file is overwritten. Is multi-tenancy or session isolation needed?

**Q8.3** — There is no audit trail of which PSP file produced which match. If a PSP file is later found to be wrong (duplicate rows, wrong date range), how do we identify which CRM rows were matched against it and re-reconcile only those rows?

**Q8.4** — The month-to-month directory structure is inconsistent (`PSPs/` vs `PSP/`, date-based subdirs). Is there a canonical directory layout that the system should enforce (or at least warn about deviations)? What would a "data intake validator" look like?

**Q8.5** — The system processes all PSP files in sorted filename order. Does the order affect match rate? (It does, because first-PSP-wins for overlapping CRM rows.) Should processing order be deterministic and auditable?

**Q8.6** — Currently there are no alerts when match rate drops unexpectedly between months. For a production system: what is a reasonable baseline match rate for each month (given its specific PSP mix), and what delta should trigger a review?

---

## Priority Tiers

| Tier | Questions | Rationale |
|---|---|---|
| **Immediate** (structural fixes) | Q1.3, Q2.3, Q2.4, Q5.4, Q7.3 | Quick wins: per-PSP schema caching, whitespace normalization, null synonym expansion, backfilling empty processor |
| **Short-term** (data science) | Q3.3, Q4.1, Q4.2, Q4.3, Q5.3, Q6.3, Q6.4 | Requires dataset analysis; unblocks currency-aware comparison and schema change detection |
| **Medium-term** (architecture) | Q1.6, Q3.6, Q5.1, Q5.2, Q6.1, Q6.2, Q6.5, Q7.2, Q8.3 | Requires building new infrastructure (schema registry, audit trail, per-PSP baseline) |
| **Research** (ML/LLM) | Q1.5, Q2.1, Q6.6, Q8.1 | Requires experimentation: embedding-based column matching, confidence scoring |
| **Policy/ops** (not code) | Q3.5, Q7.5, Q8.2, Q8.4, Q8.6 | Requires decisions from the team, not implementation |
