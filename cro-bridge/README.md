# cro-bridge - MT5 Manager -> /cro/feed push service

Runs on the Linux server as a Docker container. It computes live MT5 data
(floating PnL, closed PnL, deposits, trader counts, by-symbol breakdown, WD
Equity, and workbook cards) and pushes it to the active `recon-app`
`/cro/feed` endpoint.

The active `/cro` page now treats the bridge as the source of truth for cards.
Dealio / warehouse queries are not used for card values in this flow.

## Architecture

```text
Python 3 (native Linux) -> wine /app/MT5LivePusher.exe
                                 |
                                 v
                       MetaQuotes MT5 Manager API
                                 |
                                 v
                           MT5 Manager server
```

The bridge also exposes an on-demand report server:

```text
Python cro_report_server.py -> wine /app/MT5Reporter.exe -> MT5 Manager API
```

That path is used by `recon-app` for historical or filtered workbook-card
snapshots.

## Live payload

The bridge push payload still includes the existing live aggregates, and now
also includes:

- `wd_equity` - raw pre-clamp value
- `wd_equity_z` - clamped `max(0, wd_equity)`
- `wd_equity_legacy` - older comparator formula kept for validation
- `cro_cards` - grouped workbook card bundle for the `/cro` page

WD Equity now uses the live raw-data formula:

```text
WD Equity Raw = Balance USD + Floating PnL USD - Cumulative Bonus USD
WD Equity Z   = max(0, WD Equity Raw)
```

The cumulative bonus side comes from CRM
`report.vtiger_mttransactions` using approved bonus / FRF commission rows net
of approved cancellations.

## CRO workbook cards

The bridge exposes a canonical workbook-card bundle for the active
`recon-app /cro` page.

Bundle sections:

- `daily`
- `monthly`
- `live_inputs`

The bundle metadata includes:

- source
- group mask
- requested/report date
- mode (`live_fast_slow_bundle` or `on_demand_snapshot`)
- live/snapshot flag
- fast/slow refresh timestamps
- market timezone (`Europe/Nicosia`)
- `tables_live_scope_only`

The workbook formulas override older CRO math for cards, including:

- `Daily PnL = delta floating + closed pnl`
- `Monthly PnL = monthly closed pnl + (end floating - month-start floating)`
- `Daily/Monthly PnL Cash` from clean-equity logic
- raw `WD Equity` and clamped `WD Equity Z`
- true retention logic
- FTD and FTD amount from raw balance-deal history
- `#New Acc Reg` from MT5 registration timestamps

## Report server

`cro_report_server.py` accepts `POST /report` with JSON like:

```json
{
  "type": "cro-cards",
  "format": "json",
  "from_date": "2026-04-21",
  "to_date": "2026-04-21",
  "group_mask": "CMV*",
  "source": "AN100"
}
```

Supported `type` values:

- `deposit-withdrawal`
- `positions-history`
- `trading-accounts`
- `wd-equity-audit`
- `cro-cards`

Notes:

- `cro-cards` supports `json` only
- `trading-accounts` and `wd-equity-audit` do not require `from_date` / `to_date`
- `group_mask` and `source` are passed through to the bridge process

## Deploy

First build:

```bash
cd /root/cro-bridge
docker compose up -d --build
docker compose logs -f
```

Code-only update:

```bash
cd /root/cro-bridge
docker compose up -d --build
```

## Required env vars

These are typically inherited from `/root/recon-app/.env`.

| Var | Purpose |
|---|---|
| `MT5_PASSWORD` | MT5 manager password |
| `MT5_LOGIN` | MT5 manager login |
| `MT5_SERVER` | MT5 manager host:port |
| `CRO_BRIDGE_SECRET` | shared secret for `/cro/feed` |
| `CRM_HOST` | CRM SQL Server host |
| `CRM_PORT` | CRM SQL Server port |
| `CRM_DB` | CRM database name |
| `CRM_USER` | CRM SQL user |
| `CRM_PASS` | CRM SQL password |

Optional:

| Var | Default | Purpose |
|---|---|---|
| `CRO_WD_REFRESH_SECONDS` | `900` | refresh cadence for heavy live WD account polling |

## Useful bridge diagnostics

The payload includes live WD metadata such as:

- `wd_equity_balance_usd`
- `wd_equity_floating_usd`
- `wd_equity_cumulative_bonus_usd`
- `wd_equity_pre_clamp_usd`
- `wd_equity_raw_account_count`
- `wd_equity_account_count`
- `wd_equity_bonus_scope_login_count`
- `wd_equity_crm_matched_login_count`
- `wd_equity_crm_transaction_count`
- `wd_equity_refreshed_at`
- `wd_equity_refresh_seconds`

For deeper cross-checking, use the `wd-equity-audit` report. It exports the
same live WD formula inputs the bridge uses, including included Trading
Accounts rows, USD-normalized balance/floating values, CRM cumulative bonus
totals by login, and summary totals.

## Troubleshooting

- MT5 connect errors: verify MT5 creds in the inherited env file.
- CRM errors: verify the CRM SQL env vars and bridge container network reachability.
- `/cro` historical cards not changing: verify `cro_report_server.py` is running and
  `cro-cards` works through the bridge.
- Report timeouts: use narrower dates/groups for heavy on-demand snapshots first.
