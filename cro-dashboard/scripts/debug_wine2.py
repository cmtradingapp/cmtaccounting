"""Diagnose mscoree / Wine-Mono state in the current cro-bridge image."""
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
echo "=== mscoree files ==="
ls /opt/wineprefix/drive_c/windows/system32/mscoree.dll 2>/dev/null
ls /opt/wineprefix/drive_c/windows/syswow64/mscoree.dll 2>/dev/null
echo
echo "=== mono files ==="
ls -la /opt/wineprefix/drive_c/windows/mono/ 2>/dev/null
echo
echo "=== dotnet framework files ==="
ls /opt/wineprefix/drive_c/windows/Microsoft.NET/ 2>/dev/null
echo
echo "=== WINEDEBUG loaddll errors for MT5Bridge.exe ==="
# only show lines AFTER h.exe/MT5Bridge.exe is loaded, or errors
WINEDEBUG=+loaddll,+module wine /app/MT5Bridge.exe 2>&1 | grep -iE "MT5Bridge|mscoree|fail|err:|fixup|ilonly" | head -30
echo "=== exit: ==="
wine /app/MT5Bridge.exe 2>&1
echo "code=$?"
' 2>&1 | tail -50
"""

_, so, _ = c.exec_command(script, timeout=180)
print(so.read().decode())
c.close()
