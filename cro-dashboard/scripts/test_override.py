"""Try loading mscoree with DLL overrides + init Wine-Mono properly."""
from pathlib import Path
import paramiko


def _creds():
    for parent in Path(__file__).resolve().parents:
        f = parent / "putty-creds.txt"
        if f.exists():
            kv = {}
            for line in f.read_text().splitlines():
                if ":" in line:
                    k, v = line.split(":", 1)
                    kv[k.strip().lower()] = v.strip()
            return kv["host"], kv["username"], kv["password"]


host, user, pw = _creds()
c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect(host, username=user, password=pw, timeout=15)

script = r"""
docker run --rm cro-bridge-cro-bridge:latest bash -c '
echo "=== mscoree file date ==="
ls -la /opt/wineprefix/drive_c/windows/system32/mscoree.dll
echo "=== Wine-Mono contents ==="
ls /opt/wineprefix/drive_c/windows/mono/mono-2.0/bin/ 2>/dev/null | head -5
echo "=== registry dll overrides (Wine-Mono sets these) ==="
grep -A3 "mscoree" /opt/wineprefix/user.reg 2>/dev/null | head -10
grep -A3 "mscoree" /opt/wineprefix/system.reg 2>/dev/null | head -10

echo "=== try with WINEDLLOVERRIDES ==="
WINEDLLOVERRIDES="mscoree=n,b;fusion=n,b" wine /app/MT5Bridge.exe 2>&1 | tail -10
echo "exit=$?"

echo "=== fresh wineboot + run ==="
wineboot -u 2>&1 > /dev/null && wineserver -w
wine /app/MT5Bridge.exe 2>&1 | tail -10
echo "exit=$?"
' 2>&1 | tail -50
"""

_, so, _ = c.exec_command(script, timeout=180)
print(so.read().decode())
c.close()
