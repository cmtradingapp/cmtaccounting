# CRO "All in One" Dashboard

Replica of Metabase dashboard 164 (`analytics-cmtrading.dealio.ai/dashboard/164`).

## Why this is separate from recon-app
recon-app runs on Linux Docker. The MetaTrader 5 Manager SDK DLLs are Windows-only, so this dashboard lives on the Windows host and talks to the MT5 Manager API directly via `pythonnet`.

## Setup
```powershell
cd c:\Projects\cmtaccounting\cro-dashboard
py -3 -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python scripts\test_connection.py
```

## Layout
- `mt5_bridge.py` — thin pythonnet wrapper around `MetaQuotes.MT5ManagerAPI64.dll`
- `cro_metrics.py` — metric formulas (Daily PnL, PnL Cash, WD Equity Z, trader counts)
- `cro_cache.py` — SQLite daily-aggregate cache
- `cro_app.py` — Flask app (port 5060)
- `scripts/test_connection.py` — Phase-1 smoke test
