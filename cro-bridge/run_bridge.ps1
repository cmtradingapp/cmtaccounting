cd "c:\Projects\cmtaccounting\cro-bridge"

& "C:\Windows\Microsoft.NET\Framework64\v4.0.30319\csc.exe" `
    /out:MT5Bridge.exe `
    /reference:"C:\MetaTrader5SDK\Libs\MetaQuotes.MT5CommonAPI64.dll" `
    /reference:"C:\MetaTrader5SDK\Libs\MetaQuotes.MT5ManagerAPI64.dll" `
    MT5Bridge.cs

# .NET Framework resolves assemblies from the exe directory at runtime
Copy-Item "C:\MetaTrader5SDK\Libs\MetaQuotes.MT5CommonAPI64.dll" . -Force
Copy-Item "C:\MetaTrader5SDK\Libs\MetaQuotes.MT5ManagerAPI64.dll" . -Force
Copy-Item "C:\MetaTrader5SDK\Libs\MT5APIManager64.dll"            . -Force

$env:MT5_SERVER    = "176.126.66.18:1950"
$env:MT5_LOGIN     = "1111"
$env:MT5_PASSWORD  = "Zt*pE5AkZ_SkEgH5"
$env:MT5_SDK_LIBS  = "C:\MetaTrader5SDK\Libs"
$env:CRO_GROUP     = "CMV*"
$env:CRO_DAY_START = "0"
$env:CRO_NOW       = "0"

$raw = .\MT5Bridge.exe
# Print stderr is handled by the exe itself; $raw captures stdout only.
$json = $raw | Where-Object { $_.TrimStart().StartsWith("{") } | Select-Object -Last 1
if (-not $json) { Write-Host "No JSON output (see stderr above)"; exit 1 }
$d = $json | ConvertFrom-Json
Write-Host ""
Write-Host "=== MT5Bridge snapshot ==="
Write-Host ("  Floating PnL (USD) : {0,18:N2}" -f $d.floating_pnl_usd)
Write-Host ("  Closed PnL   (USD) : {0,18:N2}" -f $d.closed_pnl_usd)
Write-Host ("  Open positions     : {0,18:N0}" -f $d.n_positions)
Write-Host ("  Traders today      : {0,18:N0}" -f $d.n_traders)
Write-Host ("  Net deposits (USD) : {0,18:N2}" -f $d.net_deposits)
Write-Host ("  Pushed at          : {0}" -f $d.pushed_at)
Write-Host "=========================="
