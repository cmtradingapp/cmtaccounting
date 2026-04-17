"""Upload MT5Bridge.cs, recompile via docker build overlay, run for diagnostics."""
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

sftp = c.open_sftp()
sftp.put(r"c:/Projects/cmtaccounting/cro-bridge/MT5Bridge.cs",
         "/root/cro-bridge/MT5Bridge.cs")
sftp.close()

script = r"""
cat > /tmp/Dockerfile.rc <<EOF
FROM cro-bridge-cro-bridge:latest
COPY MT5Bridge.cs /app/
RUN mcs -out:/app/MT5Bridge.exe \
  -reference:/sdk-libs/MetaQuotes.MT5CommonAPI64.dll \
  -reference:/sdk-libs/MetaQuotes.MT5ManagerAPI64.dll \
  /app/MT5Bridge.cs  # AnyCPU
EOF
cd /root/cro-bridge
docker build -t cro-bridge-cro-bridge:latest -f /tmp/Dockerfile.rc . 2>&1 | tail -6
echo "--- run test ---"
docker run --rm \
  --env-file /root/recon-app/.env \
  --network recon-app_recon_net \
  --entrypoint bash cro-bridge-cro-bridge:latest -c '
wine /app/MT5Bridge.exe
echo "exit=$?"
' 2>&1 | tail -30
"""

_, so, _ = c.exec_command(script, timeout=300)
print(so.read().decode())
c.close()
