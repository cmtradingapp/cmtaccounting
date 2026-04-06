@echo off
echo Starting SSH tunnels...
echo   15432 ^> dealio replica DB
echo   15433 ^> backoffice postgres
echo.
echo Keep this window open while developing. Close it to disconnect.
echo.

"C:\Program Files\PuTTY\plink.exe" -batch -pw "lWFUo272cxA0" root@213.199.45.213 ^
  -L 15432:cmtrading-replicadb.dealio.ai:5106 ^
  -L 15433:172.18.0.2:5432 ^
  -N

pause
