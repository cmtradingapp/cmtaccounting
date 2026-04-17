"""Run MT5Bridge.exe directly and inspect raw JSON output."""
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
  -e WINEDLLOVERRIDES='mscoree=n,b;fusion=n,b' \
  --entrypoint bash cro-bridge-cro-bridge:latest -c '
export CRO_DAY_START=$(date -d "$(TZ=Europe/Nicosia date +%Y-%m-%dT00:00:00)" +%s)
export CRO_NOW=$(date +%s)
wine /app/MT5Bridge.exe 2>/dev/null | tail -1
'
"""

_, so, _ = c.exec_command(script, timeout=120)
raw = so.read().decode()
c.close()

# Save raw to file for inspection
import json
print("Raw output length:", len(raw))
print("First 500 chars:", raw[:500])
try:
    d = json.loads(raw.strip())
    print("\nParsed keys:", list(d.keys()))
    print("volume_usd =", d.get("volume_usd"))
    print("n_closing_deals =", d.get("n_closing_deals"))
    bs = d.get("by_symbol", [])
    print(f"by_symbol count = {len(bs)}")
    for r in bs[:3]:
        print("  ", r)
except Exception as e:
    print("Parse error:", e)
