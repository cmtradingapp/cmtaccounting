"""Run a verbose test of the MT5Bridge.exe inside the cro-bridge image."""
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

remote_script = r"""
docker run --rm \
  --env-file /root/recon-app/.env \
  --network recon-app_recon_net \
  --entrypoint bash cro-bridge-cro-bridge:latest -c '
echo "=== wine env ==="
echo WINEPREFIX=$WINEPREFIX WINEARCH=$WINEARCH
echo "=== wine version ==="
wine --version 2>&1 | head
echo "=== simple wine test ==="
wine cmd /c echo HELLO 2>&1
echo "=== run helper (no pipe) ==="
wine /app/MT5Bridge.exe
EX=$?
echo "=== exit=$EX ==="
'
"""
_, so, se = c.exec_command(remote_script, timeout=180)
print(so.read().decode())
err = se.read().decode()
if err.strip():
    print("--- stderr ---")
    print(err)
c.close()
