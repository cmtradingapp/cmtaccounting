# MRS Automation Plan — From Manual Reconciliation to Fully Automated Pipeline

**Author:** Aleh C.
**Date:** 2026-03-31
**Status:** Draft for Thursday meeting review

---

## Executive Summary

We currently operate a Flask-based reconciliation tool (MRS 2.0) that requires manual file uploads from 5+ sources, manual triggering of the reconciliation engine, and manual downloading of results. This document outlines how we transition to a fully automated system where:

1. All data sources feed into a central ETL database (PostgreSQL)
2. Reconciliation runs as a SQL operation against clean, standardized tables
3. Lifecycle Reports are generated automatically on a configurable schedule (daily/weekly/monthly)
4. Reports are deposited into the corporate OneDrive folder automatically
5. Nobody touches the system manually — but everything is logged and auditable

This plan is informed by the lead's ETL database initiative, which will centralize all company data (CRM, MT4, affiliate system, Praxis, bank statements) into a single queryable database with raw/clean/final layers.

---

## Current State (What We Do Today)

### Manual Steps

| Step | Who does it | Source | How |
|---|---|---|---|
| Download PSP statements (~20 PSPs) | Accounting team | Praxis portal, PSP portals | Manual download, save to shared folder |
| Download bank statements (ABSA, Nedbank, Standard) | Despina's team | Bank portals | Manual download — often PDFs |
| Export CRM Transactions Additional Info | Someone | CRM system (Gil's domain) | Manual export to .xlsx |
| Export MT4 Transactions | Someone | MT4 platform (Ibrahim/Leonidas) | Manual export |
| Export Client Equity Report | Someone | MT4 → Tableau | Manual, rules from Elis |
| Upload all files to MRS web app | Aleh / operator | Local machine | Browser drag-and-drop |
| Click "Detect & Review" → "Run Reconciliation" | Aleh / operator | MRS web app | Manual clicks |
| Download Lifecycle Report + Balances | Aleh / operator | MRS web app | Manual download |
| Copy reports to OneDrive | Aleh / operator | Local → OneDrive | Manual copy to date-named subfolder |

**Total manual touchpoints: 9 steps, involving 4-5 different people, 6+ different source systems.**

---

## Target State (Fully Automated)

```
Data Sources                    ETL Database (PostgreSQL)              Output
============                    ========================              ======

Praxis API ──────────┐
CRM API/Export ──────┤          ┌──────────────┐
MT4 API/Export ──────┤──Extract─┤  Raw Tables   │
Bank statements ─────┤          │  (as-is)      │
Affiliate system ────┤          └──────┬───────┘
FX rates (Frankfurter)┘                │ Transform
                                ┌──────┴───────┐
                                │ Clean Tables  │
                                │ (standardized)│
                                └──────┬───────┘
                                       │ Load
                                ┌──────┴───────┐        ┌──────────────┐
                                │ Final Tables  │──SQL──→│ Lifecycle    │
                                │ (unified view)│  JOIN  │ Report .xlsx │
                                └───────────────┘        └──────┬───────┘
                                                                │
                                                         OneDrive folder
                                                         (auto-deposited)
```

### What changes

| Step | Before | After |
|---|---|---|
| PSP data collection | Manual download from portals | Praxis API delivers to ETL DB automatically |
| Bank statements | Manual PDF download | Auto-ingest from email attachment or bank API (if available) |
| CRM data | Manual .xlsx export | Direct DB connection (Gil to provide access) |
| MT4 data | Manual export | Direct DB connection or API (Ibrahim/Leonidas to provide access) |
| Equity reports | Tableau manual export | Generated from MT4 raw data in ETL DB (rules from Elis) |
| Reconciliation | Flask web app, click buttons | SQL JOIN query against clean tables |
| Report generation | Download from web app | Automated script, scheduled (daily/weekly/monthly) |
| Report storage | Manual copy to OneDrive | Auto-upload to OneDrive via Microsoft Graph API |
| Monitoring | None | Dashboard showing last sync, row counts, match rate per run |

---

## The Three Database Layers

Based on the lead's ETL architecture:

### Layer 1: Raw Tables

Store data exactly as it arrives. No cleaning, no transformation. One table per source.

| Table | Source | Ingestion method | Frequency |
|---|---|---|---|
| `raw.crm_transactions` | CRM (Gil) | DB connection / API | Incremental + daily full upsert |
| `raw.mt4_transactions` | MT4 (Ibrahim/Leonidas) | DB connection / API | Incremental + daily full upsert |
| `raw.mt4_equity` | MT4 | DB connection / API | Daily |
| `raw.praxis_transactions` | Praxis API | API pull | Incremental + daily full upsert |
| `raw.psp_{name}` | Individual PSP portals (via Praxis or direct) | API / file ingest | Daily or as available |
| `raw.bank_absa` | ABSA Bank | Email parser or API | When received |
| `raw.bank_nedbank` | Nedbank | Email parser or API | When received |
| `raw.bank_standard` | Standard Bank | Email parser or API | When received |
| `raw.bank_freemarket` | FreeMarket | Email parser or API | When received |
| `raw.fx_rates` | Frankfurter API | API pull | Daily at market close |
| `raw.affiliate_data` | Affiliate system | DB connection / API | As needed |

### Layer 2: Clean Tables

Standardized schemas. All PSPs normalized to the same column structure.

| Table | Contents |
|---|---|
| `clean.crm_transactions` | CRM data with: `transaction_id`, `psp_transaction_id`, `amount`, `currency` (resolved from FK), `payment_method`, `payment_processor`, `transaction_type`, `date`, `login`, `trading_account_id` |
| `clean.mt4_transactions` | MT4 data with: `deal_no`, `login`, `type`, `amount`, `currency`, `date`, `comment` |
| `clean.psp_transactions` | ALL PSPs in one table with: `psp_name`, `reference_id`, `amount`, `currency`, `date`, `fee`, `status`, `raw_source_file` |
| `clean.bank_transactions` | ALL banks in one table with: `bank_name`, `reference`, `amount`, `currency`, `date`, `description`, `client_id` |
| `clean.fx_rates` | Date-indexed FX rates: `date`, `base_currency`, `target_currency`, `rate` |
| `clean.client_equity` | Generated from MT4 raw data using Elis's rules |

**The key transformation:** Each PSP's unique column names are mapped to the standard `clean.psp_transactions` schema using per-PSP mapping rules (the ~20 rules we need to maintain, per Q&A answer #2).

### Layer 3: Final Tables

Ready for reconciliation queries.

| Table | Contents |
|---|---|
| `final.reconciliation` | Result of the CRM ↔ PSP/Bank join: `match_status` (matched/unmatched_crm/unmatched_bank), `crm_amount`, `bank_amount`, `amount_diff`, `currency_match`, `psp_source` |
| `final.lifecycle_report` | 52-column Lifecycle List format, ready for Excel export |
| `final.balances` | Currency equity summary with EUR & USD totals |

---

## Reconciliation as SQL

Once data is in the `clean` layer, the core reconciliation becomes a SQL query:

```sql
-- Core reconciliation: CRM ↔ PSP match
SELECT
    c.transaction_id,
    c.psp_transaction_id,
    c.amount AS crm_amount,
    c.currency AS crm_currency,
    p.amount AS psp_amount,
    p.currency AS psp_currency,
    p.psp_name,
    CASE
        WHEN p.reference_id IS NOT NULL THEN 'matched'
        ELSE 'unmatched_crm'
    END AS match_status
FROM clean.crm_transactions c
LEFT JOIN clean.psp_transactions p
    ON UPPER(TRIM(c.psp_transaction_id)) = UPPER(TRIM(p.reference_id))
WHERE c.transaction_type IN ('Deposit', 'Withdraw')
  AND c.report_month = '2023-01'
```

All the heuristics we currently do in Python (normalize_key, detect_bank_ref_col, overlap-based key selection) are replaced by:

1. **Per-PSP mapping rules** stored in a config table that map each PSP's column names to the standard schema
2. **Key normalization** done once during the Transform step (strip whitespace, remove .0 suffix, uppercase)
3. **JOIN** on the already-clean data

---

## Data Source Acquisition Plan

### Confirmed Sources

| Source | Current method | Automated method | Owner to confirm |
|---|---|---|---|
| **CRM Transactions** | Manual .xlsx export | Direct DB read or API endpoint | **Gil** |
| **MT4 Transactions** | Manual export | Direct DB read or API endpoint | **Ibrahim / Leonidas** |
| **MT4 Equity** | Tableau manual export | Generate from MT4 raw data | **Elis** (provides rules) |
| **PSP Statements** | Manual download from portals | **Praxis API** (linked to all PSPs) | **Despina** to confirm scope |
| **FX Rates** | Frankfurter API (already automated) | Same — daily pull | Already working |

### Needs Investigation

| Source | Question | Who to ask |
|---|---|---|
| **Bank statements (ABSA, Nedbank, Standard, FreeMarket)** | Do they have APIs? If not, can they send reports by email? Can we parse email attachments automatically? | **Despina** (she manages the relationship) |
| **Opening Balances** | Received from PSPs — which PSPs, in which format? | **Despina / Elis** |
| **Praxis API** | What endpoints are available? Does it cover all ~20 PSPs? Does it provide settlement-level detail or just transaction-level? | **Technical team** (whoever manages Praxis integration) |
| **CRM/MT4 DB access** | Can we get a read-only SQL user for the CRM database? Same for MT4? | **Gil** (CRM), **Ibrahim/Leonidas** (MT4) |

---

## PSP Schema Management

Per the Q&A (answer #2): there are ~20 PSPs in total, schemas rarely change, and when they do it's a notable event.

### What we need

1. **A folder with all possible PSP statement variants** — one sample file per PSP, per known format variant. This becomes the "ground truth" for building mapping rules.

2. **Per-PSP mapping rules** stored in a database config table:

```
psp_name          | ref_column         | amount_column  | date_column    | currency_column | notes
------------------|--------------------|----------------|----------------|-----------------|------
Nuvei             | Transaction ID     | Amount         | Date           | Currency        | skiprows=11 in Excel
Korapay Pay-ins   | payment_reference  | amount_paid    | transaction_date| currency       |
Zotapay           | id                 | order_amount   | created_at     | order_currency  |
SolidPayments     | UniqueId           | Debit/Credit   | RequestTimestamp| Currency        |
EFTpay            | merchant_reference | amount         | date           | currency        |
...
```

3. **A web interface to manage these rules** — when a new PSP is connected, someone can add its column mapping through a simple form. No code changes required.

4. **Validation on ingest** — when a PSP file is loaded, check that the expected columns exist. If not, flag it for manual review rather than silently failing.

---

## Report Generation & Delivery

### Configurable Schedule

Per Q&A answer #6: must be configurable — daily, weekly, monthly.

```
Schedule config table:
report_type | frequency | day_of_week | day_of_month | recipients | onedrive_path
------------|-----------|-------------|--------------|------------|---------------
Lifecycle   | monthly   | null        | 5            | despina    | /MRS/Monthly/{YYYY-MM}/
Lifecycle   | weekly    | Monday      | null         | aleh       | /MRS/Weekly/{YYYY-Www}/
Balances    | monthly   | null        | 5            | despina    | /MRS/Monthly/{YYYY-MM}/
Issues      | monthly   | null        | 5            | aleh       | /MRS/Monthly/{YYYY-MM}/
```

### OneDrive Integration

Company stores reports in a shared OneDrive folder with date-named subfolders (Q&A answer #6.1).

**Implementation:** Microsoft Graph API with a service account.
- Authenticate via OAuth2 client credentials flow
- Upload `.xlsx` files to the correct subfolder
- Log upload success/failure

### Logging & Monitoring

The lead already built a monitoring dashboard for the retention project showing:
- All connections and last sync timestamp
- How many rows synced per source
- Which row was last (incremental sync tracking)

We replicate this pattern:
- **Per-source sync status:** Last pull timestamp, row count, any errors
- **Per-run reconciliation stats:** Match rate, unmatched counts, unrecon amount
- **Alerts:** Match rate drops below baseline → notification
- **Audit trail:** Which source files produced which matches, when, by whom

---

## Implementation Phases

### Phase 0: Information Gathering (This week → Thursday meeting)

**Deliverable:** Complete list of fields and sources the lead needs.

- Get DB access details from Gil (CRM) and Ibrahim/Leonidas (MT4)
- Confirm Praxis API coverage with Despina
- Get bank statement automation options from Despina
- Get equity report generation rules from Elis
- Compile the per-PSP column mapping table from the sample files folder

### Phase 1: ETL Foundation (Weeks 1-3)

**Goal:** Raw data flowing into PostgreSQL from all sources.

1. Stand up PostgreSQL instance
2. Create `raw.*` tables for each source
3. Build ingestion scripts:
   - CRM → `raw.crm_transactions` (DB connection or API)
   - MT4 → `raw.mt4_transactions` (DB connection or API)
   - Praxis → `raw.psp_{name}` (API pull)
   - FX rates → `raw.fx_rates` (Frankfurter, already working)
4. Bank statements: implement email parser or manual upload pipeline (interim)
5. Set up incremental sync + daily full upsert (same pattern as retention project)

### Phase 2: Transform Layer (Weeks 3-5)

**Goal:** Clean, standardized data ready for SQL joins.

1. Build `clean.*` table schemas
2. Create per-PSP transformation scripts using the mapping rules table
3. Implement key normalization in SQL (strip, uppercase, remove .0 suffix)
4. Generate `clean.client_equity` from MT4 raw data (Elis's rules)
5. Resolve `currency_id` FK → ISO currency code mapping
6. Build the schema management web interface for adding new PSP rules

### Phase 3: Reconciliation as SQL (Weeks 5-7)

**Goal:** Replace the Python reconciliation engine with SQL queries.

1. Write the core LEFT JOIN reconciliation query
2. Implement the multi-key strategy (try `psp_transaction_id` first, fall back to `transactionid`) as a SQL COALESCE or UNION
3. Generate `final.reconciliation` table
4. Build `final.lifecycle_report` (52-column spec) from the reconciled data
5. Build `final.balances` from equity + reconciliation data
6. Validate against existing MRS output for January 2023 (same match rate, same numbers)

### Phase 4: Automated Report Generation (Weeks 7-9)

**Goal:** Reports generated and delivered without human intervention.

1. Build Excel export scripts (openpyxl) that read from `final.*` tables
2. Implement configurable schedule (cron or task scheduler)
3. Integrate OneDrive upload via Microsoft Graph API
4. Build monitoring dashboard (connections, last sync, row counts)
5. Set up alerting: match rate drops, sync failures, schema changes

### Phase 5: Decommission Manual Process (Week 9+)

**Goal:** MRS 2.0 web app becomes a read-only dashboard; all processing is automated.

1. Parallel run: automated pipeline + manual MRS for 1-2 months
2. Compare outputs, resolve discrepancies
3. Cut over to automated pipeline
4. MRS web app becomes the monitoring/diagnostic interface (match rate trends, issues drill-down)

---

## What the Lead Needs From Us

The lead asked: **"Which fields and sources?"**

Here is the complete answer:

### Fields per source

**CRM (Gil):**
- `mtorder_id`, `transactionid`, `psp_transaction_id`, `login`, `tradingaccountsid`
- `amount`, `currency_id`, `usdamount`
- `payment_method`, `payment_processor`, `transactiontype`
- `Month, Day, Year of confirmation_time`
- `first_name`, `last_name`, `comment`
- `receipt`, `ewalletid`, `creditcardlast`
- `bank_name`, `bank_acccount_number`

**MT4 (Ibrahim/Leonidas):**
- `Deal No`, `Login`, `Type`, `Amount`, `Currency`, `Date`, `Comment`
- Whatever fields Elis needs for equity report generation

**Praxis / PSP Statements (Despina):**
- Per-PSP: varies by provider, but minimum: `reference_id`, `amount`, `currency`, `date`, `status`, `fee`
- The per-PSP mapping rules table (section above) defines exactly which column is which

**Bank Statements (Despina):**
- `date`, `reference`, `amount`, `currency`, `description`, `client_id` (if available)
- Delivery method: API, email, or manual upload

**FX Rates:**
- Already automated via Frankfurter API (28 fiat pairs) + CoinGecko (5 crypto pairs)

---

## Open Questions for Thursday Meeting

See companion document: [meeting-agenda-2026-04-03.md](meeting-agenda-2026-04-03.md)

---

## Risk Register

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| CRM/MT4 don't expose DB access or API | Medium | High — blocks Phase 1 | Ask Gil/Ibrahim/Leonidas Thursday; fallback = scheduled CSV export |
| Bank statements have no API | High | Medium — manual step remains | Email parser as interim; lobby banks for API access |
| PSP schema changes during migration | Low | Medium | Schema validation on ingest catches it early |
| Praxis API doesn't cover all ~20 PSPs | Medium | Medium | Identify gaps Thursday; direct PSP API for uncovered ones |
| Match rate differs between SQL and Python engines | Low | High | Phase 5 parallel run catches discrepancies |
| OneDrive API authentication complexity | Low | Low | Microsoft Graph API is well-documented; service account auth is standard |