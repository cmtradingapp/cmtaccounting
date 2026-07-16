@echo off
echo Starting SSH tunnels...
echo   15432 ^> dealio replica DB
echo   15433 ^> backoffice postgres
echo   15435 ^> fees postgres (FEES_MODE=live only)
echo.
echo NOTE: Praxis now connects DIRECTLY to the reporting replica
echo       13.140.163.221:5434 (reporting_ro) -- no tunnel needed.
echo       (The old 15434 ^> 161.97.162.143:5433 forward was firewalled.)
echo NOTE: Antelope CRM (Azure SQL) connects DIRECTLY to
echo       cmtmainserver.database.windows.net:1433 -- no tunnel needed.
echo.
echo Keep this window open while developing. Close it to disconnect.
echo.

"C:\Program Files\PuTTY\plink.exe" -batch -pw "lWFUo272cxA0" root@213.199.45.213 ^
  -L 15432:cmtrading-replicadb.dealio.ai:5106 ^
  -L 15433:172.18.0.2:5432 ^
  -L 15435:fees_postgres:5432 ^
  -N

pause
