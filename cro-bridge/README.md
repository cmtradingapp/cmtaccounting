# cro-bridge — MT5 Manager -> /cro/feed push service

Runs on the Linux server (213.199.45.213) as a Docker container. Computes
live MT5 data (floating PnL, closed PnL, volumes, deposits, trader counts,
by-symbol breakdown) every 30s and POSTs it to the recon-app `/cro/feed`
endpoint. The dashboard at `recon.cmtrading.com/cro` reads entirely from
these live pushes — **Dealio is no longer used** for the CRO panel.

The live pusher now computes `WD Equity Z` from live MT5 Trading Accounts plus
CRM cumulative bonus data:

`max(0, sum(Balance USD) + sum(Floating PNL USD) - Cumulative Bonus USD)`

The MT5 side comes from current Trading Accounts rows. The cumulative bonus
side is read live from CRM `report.vtiger_mttransactions`, using approved
bonus / FRF commission rows net of approved cancellations. Because the Trading
Accounts pull is heavy on large books, the bridge caches that result and
refreshes it every 15 minutes by default instead of recomputing it on each
2-second live push. For backward compatibility the payload still includes
`wd_equity`, but it now carries the cached live `WD Equity Z` value; the old
bridge formula is also emitted as `wd_equity_legacy`.

## Architecture

```
  Python 3 (native Linux) ── wine /app/MT5Bridge.exe (C# compiled with Mono)
                                       │
                                       │ loaded by Wine's Wine-Mono runtime
                                       ▼
                                  MT5APIManager64.dll (Windows PE, via Wine)
                                       │ TCP 1950
                                       ▼
                                  MT5 Manager server
```

The C# helper is compiled at image-build time with `mcs` against the MT5
SDK's .NET wrappers. It prints a single JSON line on stdout containing all
the aggregates; the Python pusher POSTs that line to `/cro/feed` with the
shared `X-Bridge-Secret` header.

## Deploy

### First-time
```bash
cd /root/cro-bridge
docker compose up -d --build          # ~3-5 min first build (Wine-Mono dl)
docker compose logs -f                # watch pushes
```

### Code-only update
```bash
cd /root/cro-bridge
docker compose up -d --build          # overlays the .cs/pusher change,
                                      # reuses the Wine-Mono layer (~10s)
```

CI does this automatically when `cro-bridge/` changes under `main`.

## Required env vars (in `/root/recon-app/.env`)

| Var | Example |
|---|---|
| `MT5_PASSWORD`       | `***` (manager account password) |
| `MT5_LOGIN`          | `1111` |
| `MT5_SERVER`         | `176.126.66.18:1950` |
| `CRO_BRIDGE_SECRET`  | shared secret for `/cro/feed` header |
| `CRM_HOST`           | CRM SQL Server host |
| `CRM_PORT`           | CRM SQL Server port, usually `1433` |
| `CRM_DB`             | CRM database name |
| `CRM_USER`           | CRM SQL user |
| `CRM_PASS`           | CRM SQL password |

## Optional WD Equity Z env vars

| Var | Default | Purpose |
|---|---|---|
| `CRO_WD_REFRESH_SECONDS` | `900` | refresh cadence for the heavy live Trading Accounts WD poll |

The payload now also includes `wd_equity_z`, `wd_equity_legacy`, and a set of
live-account breakdown/metadata fields such as:

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

This makes it easier to validate the live raw formula against MT5 Manager and
CRM source data.

## Zero-downtime guarantees

1. `restart: always` — Docker restarts on any exit (crash, OOM, manual stop).
2. Docker daemon is systemd-enabled — starts on server boot.
3. Container `healthcheck` — if no successful push in 3 min, marked unhealthy.
4. Pusher `while True` loop with exponential backoff — never exits on errors.
5. `@reboot` crontab entry on the host — `docker compose up -d` 60s after boot.

## Troubleshooting

- **Container unhealthy but running**: the C# helper probably succeeded but
  the POST to recon-app failed. Check `docker logs recon-app-recon-1` and
  verify the `recon-app_recon_net` network still contains both containers.
- **MT5 connect errors**: check creds in `/root/recon-app/.env`.
- **`mscoree.dll` load failure**: verify `WINEDLLOVERRIDES=mscoree=n,b;fusion=n,b`
  is still set in `docker-compose.yml` (environment block).

## Image size

~4.5 GB (Wine + Wine-Mono + Mono + Python3 + SDK DLLs). Built from
`tobix/pywine:3.11`.
