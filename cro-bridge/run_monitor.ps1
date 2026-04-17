cd "c:\Projects\cmtaccounting\cro-bridge"

& "C:\Windows\Microsoft.NET\Framework64\v4.0.30319\csc.exe" `
    /out:MT5Monitor.exe `
    /reference:"C:\MetaTrader5SDK\Libs\MetaQuotes.MT5CommonAPI64.dll" `
    /reference:"C:\MetaTrader5SDK\Libs\MetaQuotes.MT5ManagerAPI64.dll" `
    MT5Monitor.cs

Copy-Item "C:\MetaTrader5SDK\Libs\MetaQuotes.MT5CommonAPI64.dll" . -Force
Copy-Item "C:\MetaTrader5SDK\Libs\MetaQuotes.MT5ManagerAPI64.dll" . -Force
Copy-Item "C:\MetaTrader5SDK\Libs\MT5APIManager64.dll"            . -Force

$env:MT5_SERVER    = "176.126.66.18:1950"
$env:MT5_LOGIN     = "1111"
$env:MT5_PASSWORD  = "Zt*pE5AkZ_SkEgH5"
$env:MT5_SDK_LIBS  = "C:\MetaTrader5SDK\Libs"
$env:CRO_GROUP     = "CMV*"
$env:MT5_INTERVAL  = "5"   # seconds between polls

.\MT5Monitor.exe
