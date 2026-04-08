@echo off
echo Starting SSH tunnels...
echo   15432 ^> dealio replica DB
echo   15433 ^> backoffice postgres
echo   15434 ^> praxis operations DB
echo   15435 ^> fees postgres (FEES_MODE=live only)
echo.
echo Keep this window open while developing. Close it to disconnect.
echo.

"C:\Program Files\PuTTY\plink.exe" -batch -pw "lWFUo272cxA0" root@213.199.45.213 ^
  -L 15432:cmtrading-replicadb.dealio.ai:5106 ^
  -L 15433:172.18.0.2:5432 ^
  -L 15434:161.97.162.143:5433 ^
  -L 15435:fees_postgres:5432 ^
  -N

pause
