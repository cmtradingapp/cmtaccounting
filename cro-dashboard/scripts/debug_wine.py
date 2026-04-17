"""Deep diagnostic: what .NET runtime is in Wine?"""
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
docker run --rm tobix/pywine:3.11 bash -c '
echo "=== wine version ==="
wine --version
echo
echo "=== wine mono status ==="
ls -la /opt/wineprefix/drive_c/windows/mono/ 2>/dev/null | head -5
echo
echo "=== registered runtimes ==="
ls /opt/wineprefix/drive_c/windows/Microsoft.NET/ 2>/dev/null
echo
echo "=== wine with loaddll log ==="
apt-get update -qq && apt-get install -y -qq mono-mcs 2>&1 > /dev/null
cat > /tmp/h.cs <<EOC
using System;
public class H { public static int Main() { Console.WriteLine("OK"); return 0; } }
EOC
mcs -out:/tmp/h.exe /tmp/h.cs
echo "size of h.exe:"
ls -la /tmp/h.exe
echo "=== first 50 lines of wine+WINEDEBUG=+loaddll ==="
WINEDEBUG=+loaddll wine /tmp/h.exe 2>&1 | grep -iE "mono|\.exe|\.net|mscoree|failed|error" | head -20
echo "=== exit code: ==="
WINEDEBUG=-all wine /tmp/h.exe
echo "exit=$?"
' 2>&1 | tail -50
"""

_, so, _ = c.exec_command(script, timeout=300)
print(so.read().decode())
c.close()
