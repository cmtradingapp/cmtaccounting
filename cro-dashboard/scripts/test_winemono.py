"""Test: can tobix/pywine's default Wine-Mono run our hello.exe and MT5Bridge.exe?"""
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
# Use the base pywine image (no dotnet48 override), install mono-mcs to compile
docker run --rm tobix/pywine:3.11 bash -c '
apt-get update -qq && apt-get install -y -qq mono-mcs 2>&1 | tail -2
cat > /tmp/hello.cs <<EOC
using System;
public class Hello {
    public static int Main() {
        Console.WriteLine("HELLO FROM WINE-MONO");
        return 42;
    }
}
EOC
mcs -out:/tmp/hello.exe /tmp/hello.cs
echo "=== run hello.exe under wine (uses Wine-Mono) ==="
wine /tmp/hello.exe
echo "exit=$?"
' 2>&1 | tail -15
"""

_, so, _ = c.exec_command(script, timeout=300)
print(so.read().decode())
c.close()
