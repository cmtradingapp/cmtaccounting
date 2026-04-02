# Grand Database Schema — Full Picture

```
╔══════════════════════════════════════════════════════════════════════════════════════════════╗
║                                    DATA SOURCES                                              ║
╠══════════════╦═══════════════╦═══════════════╦══════════════╦═════════════╦══════════════════╣
║   CRM        ║     MT4       ║  Praxis API   ║    Banks     ║  Affiliate  ║   Proline        ║
║  (Gil)       ║ (Ibrahim/Leo) ║  (~20 PSPs)   ║  (Despina)   ║   System    ║ (Commissions)    ║
╚══════╤═══════╩═══════╤═══════╩═══════╤═══════╩══════╤═══════╩══════╤══════╩════════╤═════════╝
       │               │               │               │              │               │
       ▼               ▼               ▼               ▼              ▼               ▼
╔══════════════════════════════════════════════════════════════════════════════════════════════╗
║                               LAYER 1 — RAW (as-is, no transforms)                          ║
╠══════════════╦═══════════════╦═══════════════╦══════════════╦═════════════╦══════════════════╣
║ raw.crm_txns ║raw.mt4_trades ║raw.psp_{name} ║raw.bank_{nm} ║raw.affiliate║raw.proline       ║
║ raw.crm_accs ║raw.mt4_equity ║               ║              ║             ║                  ║
╚══════╤═══════╩═══════╤═══════╩═══════╤═══════╩══════╤═══════╩══════╤══════╩════════╤═════════╝
       │               │               │               │              │               │
       │         TRANSFORM (normalize keys, resolve FKs, map PSP columns, strip/uppercase)
       │               │               │               │              │               │
       ▼               ▼               ▼               ▼              ▼               ▼
╔══════════════════════════════════════════════════════════════════════════════════════════════╗
║                            LAYER 2 — CLEAN (standardized, one schema per domain)            ║
╠══════════════════════════════════════════════════════════════╗                               ║
║  accounts                                                    ║  fx_rates                     ║
║  ─────────                                                   ║  ────────                     ║
║  account_id (PK)      ◄──────────────────────────────────┐  ║  date                         ║
║  login                                                    │  ║  base_currency                ║
║  client_name                                              │  ║  target_currency              ║
║  email                                                    │  ║  rate                         ║
║  country                                                  │  ╠═══════════════════════════════╣
║  account_type                                             │  ║  psp_schema_registry          ║
║  currency                                                 │  ║  ─────────────────            ║
║  vtiger_id (→ CRM)                                        │  ║  psp_name                     ║
║  created_at                                               │  ║  ref_column                   ║
╠══════════════════════════════════════════════════════════╗│  ║  amount_column                ║
║  transactions                           ◄────────────────╝│  ║  date_column                  ║
║  ────────────                                              │  ║  currency_column              ║
║  transaction_id (PK)                                       │  ║  skiprows                     ║
║  account_id (FK → accounts)                                │  ║  notes                        ║
║  type  [deposit | withdrawal | credit]                     │  ╚═══════════════════════════════╣
║  amount                                                    │                                  ║
║  currency                                                  │                                  ║
║  usd_amount                                                │                                  ║
║  payment_method                                            │                                  ║
║  payment_processor                                         │                                  ║
║  psp_transaction_id  ◄─── JOIN KEY ───────────────┐       │                                  ║
║  crm_transaction_id                                │       │                                  ║
║  mt4_order_id                                      │       │                                  ║
║  status                                            │       │                                  ║
║  date                                              │       │                                  ║
║  report_month                                      │       │                                  ║
╠═══════════════════════════════════════════════════╗│       │                                  ║
║  psp_transactions   (from Praxis / PSP exports)   ││       │                                  ║
║  ────────────────                                 ││       │                                  ║
║  psp_tx_id (PK)                                   ││       │                                  ║
║  psp_name                                         ││       │                                  ║
║  reference_id  ◄──────────────────────────────────┘│       │                                  ║
║  amount                                             │       │                                  ║
║  currency                                           │       │                                  ║
║  fee                                                │       │                                  ║
║  status                                             │       │                                  ║
║  date                                               │       │                                  ║
║  raw_source_file                                    │       │                                  ║
╠═════════════════════════════════════════════════════│═══════╗                                  ║
║  bank_transactions                                  │       ║                                  ║
║  ─────────────────                                  │       ║                                  ║
║  bank_tx_id (PK)                                    │       ║                                  ║
║  bank_name                                          │       ║                                  ║
║  reference                                          │       ║                                  ║
║  client_id  ◄───────────────────────────────────────│───────╝                                  ║
║  amount                                             │                                          ║
║  currency                                           │                                          ║
║  date                                               │                                          ║
║  description                                        │                                          ║
╠═════════════════════════════════════════════════════╧═══════════════════════════════════════════╣
║  trades_open                          ║  trades_closed                                          ║
║  ──────────                           ║  ─────────────                                          ║
║  trade_id (PK)                        ║  trade_id (PK)                                          ║
║  account_id (FK → accounts)           ║  account_id (FK → accounts)                             ║
║  symbol                               ║  symbol                                                 ║
║  direction [buy | sell]               ║  direction                                              ║
║  volume (lots)                        ║  volume                                                 ║
║  open_price                           ║  open_price / close_price                               ║
║  open_time                            ║  open_time / close_time                                 ║
║  swap                                 ║  profit                                                 ║
║  commission                           ║  commission                                             ║
║  current_profit                       ║  swap                                                   ║
╠═══════════════════════════════════════╬═════════════════════════════════════════════════════════╣
║  client_equity                        ║  agents (from Proline)                                  ║
║  ─────────────                        ║  ──────                                                 ║
║  account_id (FK → accounts)           ║  agent_id (PK)                                          ║
║  date                                 ║  name, email, tier                                      ║
║  balance                              ║  ─────────────────────────────                          ║
║  equity                               ║  commissions                                            ║
║  margin                               ║  ────────────                                           ║
║  free_margin                          ║  commission_id (PK)                                     ║
║  margin_level                         ║  agent_id (FK → agents)                                 ║
║  currency                             ║  account_id (FK → accounts)                             ║
║  source [mt4 | tableau]               ║  transaction_id (FK → transactions)                     ║
║                                       ║  amount, currency, date, type                           ║
╚═══════════════════════════════════════╩═════════════════════════════════════════════════════════╝
       │               │               │
       │          JOIN / QUERY          │
       │               │               │
       ▼               ▼               ▼
╔══════════════════════════════════════════════════════════════════════════════════════════════╗
║                            LAYER 3 — FINAL (reconciled, report-ready)                       ║
╠═══════════════════════════════════════════╦══════════════════════════════════════════════════╣
║  reconciliation                           ║  lifecycle_report                                ║
║  ───────────────                          ║  ─────────────────                               ║
║  transaction_id (FK → transactions)       ║  52-column Lifecycle List spec                   ║
║  psp_tx_id (FK → psp_transactions)        ║  Tran.Date, Reference, Deal No                   ║
║  bank_tx_id (FK → bank_transactions)      ║  Amount, Commission, Total                       ║
║  match_status                             ║  Currency, AmntBC                                ║
║    [matched |                             ║  Match No, Recon.Reason Group                    ║
║     unmatched_crm |                       ║  Matched By, Matched On                          ║
║     unmatched_psp |                       ║  IsTiming, Type                                  ║
║     unmatched_bank]                       ║  ClientCode, ClientAccount                       ║
║  crm_amount / psp_amount / diff           ║  CategoryCode, PM Code ...                       ║
║  currency_match [same | cross_ccy]        ╠══════════════════════════════════════════════════╣
║  confidence_score                         ║  balances                                        ║
║  run_id (FK → reconciliation_runs)        ║  ────────                                        ║
╠═══════════════════════════════════════════╣  currency, equity_sum                            ║
║  reconciliation_runs                      ║  eur_total, usd_total, pct                       ║
║  ─────────────────────                    ╚══════════════════════════════════════════════════╣
║  run_id (PK)                              ║  report_schedule                                 ║
║  report_month                             ║  ───────────────                                 ║
║  run_at                                   ║  frequency [daily|weekly|monthly]                ║
║  match_rate                               ║  next_run_at                                     ║
║  matched / unmatched_crm / unmatched_psp  ║  onedrive_path                                  ║
║  unrecon_amount                           ║  last_delivered_at                               ║
║  triggered_by [scheduled | manual]        ║  recipients                                      ║
╚═══════════════════════════════════════════╩══════════════════════════════════════════════════╝
       │
       ▼
╔══════════════════════════════════════════════════════════════════════════════════════════════╗
║                               CONSUMERS (read-only access, per service user)                 ║
╠═══════════════╦══════════════╦═══════════════╦══════════════╦════════════════╦═══════════════╣
║  MRS Web App  ║  Retention   ║  Elis Reports ║  Affiliate   ║  Lead's future ║  Despina /    ║
║  (Aleh)       ║  Dashboard   ║               ║  System      ║  analytics     ║  Accounting   ║
║  mrs_user     ║  retention_u ║  elis_user    ║  affiliate_u ║  analyst_user  ║  (OneDrive)   ║
╚═══════════════╩══════════════╩═══════════════╩══════════════╩════════════════╩═══════════════╝
```

---

## Key join paths

| What we're matching | Join |
|---|---|
| CRM deposit ↔ PSP statement | `transactions.psp_transaction_id = psp_transactions.reference_id` |
| CRM deposit ↔ bank wire | `transactions.psp_transaction_id = bank_transactions.client_id` |
| CRM transaction ↔ MT4 deal | `transactions.mt4_order_id = trades_closed.trade_id` |
| Commission ↔ client deposit | `commissions.transaction_id = transactions.transaction_id` |
| Equity ↔ account | `client_equity.account_id = accounts.account_id` |

## What each team gets

| Consumer | Tables they read |
|---|---|
| **MRS / Reconciliation (Aleh)** | `transactions`, `psp_transactions`, `bank_transactions`, `reconciliation`, `lifecycle_report`, `client_equity` |
| **Retention** | `accounts`, `transactions`, `trades_closed`, `client_equity` |
| **Elis / Reports** | `transactions`, `trades_closed`, `client_equity`, `accounts` |
| **Affiliate / Proline** | `accounts`, `transactions`, `commissions`, `agents` |
| **Accounting (Despina)** | `reconciliation`, `lifecycle_report`, `balances` (via OneDrive) |