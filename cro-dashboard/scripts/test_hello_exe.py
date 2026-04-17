"""Minimal test: compile + run a hello-world C# exe under Wine."""
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
docker run --rm --entrypoint bash cro-bridge-cro-bridge:latest -c '
cat > /tmp/hello.cs <<EOC
using System;
public class Hello {
    public static int Main() {
        Console.WriteLine("HELLO FROM DOTNET");
        Console.Error.WriteLine("stderr also");
        return 42;
    }
}
EOC
mcs -out:/tmp/hello.exe /tmp/hello.cs
ls -la /tmp/hello.exe
echo "=== run under wine ==="
wine /tmp/hello.exe
echo "exit=$?"
echo "=== run under mono (native linux) ==="
mono /tmp/hello.exe
echo "exit=$?"
' 2>&1 | tail -20
"""

_, so, _ = c.exec_command(script, timeout=180)
print(so.read().decode())
c.close()
