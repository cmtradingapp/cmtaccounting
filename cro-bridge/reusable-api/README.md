# cro-bridge — MT5 Manager -> /cro/feed push service

Runs on the Linux server (213.199.45.213) as a Docker container. Computes
live MT5 data (floating PnL, closed PnL, volumes, deposits, trader counts,
by-symbol breakdown) every 30s and POSTs it to the recon-app `/cro/feed`
endpoint. The dashboard at `recon.cmtrading.com/cro` reads entirely from
these live pushes — **Dealio is no longer used** for the CRO panel.

The live pusher now computes `WD Equity Z` from MT5 daily fields plus
protected-bonus balance deals. For backward compatibility the payload still
includes `wd_equity`, but it now carries the `WD Equity Z` value; the old
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

## Optional WD Equity Z env vars

| Var | Default | Purpose |
|---|---|---|
| `CRO_WD_EQUITY_MODE` | `delta_from_start` | `delta_from_start` or `end_only` |
| `CRO_WD_BONUS_COMMENT` | `Bonus Protected Trad` | substring used to identify protected-bonus balance deals |
| `CRO_WD_BONUS_FROM` | current year start | `yyyy-MM-dd` history start used to reconstruct protected-bonus balances |

The payload now also includes `wd_equity_z`, `wd_equity_legacy`, and a set of
start/end breakdown fields (`wd_equity_end_*`, `wd_equity_start_*`) to make
validation against MT5 Manager screens easier.

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
