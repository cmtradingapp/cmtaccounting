"""Run MT5Bridge.exe with proper env + DLL overrides."""
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
docker run --rm \
  --env-file /root/recon-app/.env \
  --network recon-app_recon_net \
  -e WINEDLLOVERRIDES='mscoree=n,b;fusion=n,b' \
  --entrypoint bash cro-bridge-cro-bridge:latest -c '
# Compute today Europe/Nicosia day start as unix seconds
export CRO_DAY_START=$(date -d "$(TZ=Europe/Nicosia date +%Y-%m-%dT00:00:00)" +%s)
export CRO_NOW=$(date +%s)
echo "day_start=$CRO_DAY_START now=$CRO_NOW"
wine /app/MT5Bridge.exe 2>&1 | tail -15
echo "exit=$?"
'
"""

_, so, _ = c.exec_command(script, timeout=180)
print(so.read().decode())
c.close()
