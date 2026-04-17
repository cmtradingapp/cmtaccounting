"""Upload updated cro-bridge/ + recompile C# + restart service."""
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
src = Path(r"c:/Projects/cmtaccounting/cro-bridge")
for name in ["MT5Bridge.cs", "cro_bridge_pusher.py", "docker-compose.yml", "Dockerfile"]:
    sftp.put(str(src / name), f"/root/cro-bridge/{name}")
    print(f"  uploaded {name}")
sftp.close()

# Do a fast overlay rebuild — only recompile + re-COPY (dotnet48 layer cached)
script = r"""
cat > /tmp/Dockerfile.rebuild <<'EOF'
FROM cro-bridge-cro-bridge:latest
COPY MT5Bridge.cs /app/
RUN mcs -out:/app/MT5Bridge.exe \
        -reference:/sdk-libs/MetaQuotes.MT5CommonAPI64.dll \
        -reference:/sdk-libs/MetaQuotes.MT5ManagerAPI64.dll \
        /app/MT5Bridge.cs
COPY cro_bridge_pusher.py /app/
EOF
cd /root/cro-bridge
docker build -t cro-bridge-cro-bridge:latest -f /tmp/Dockerfile.rebuild . 2>&1 | tail -10
echo '--- restart service ---'
docker compose up -d --force-recreate 2>&1 | tail -5
echo '--- wait then show logs ---'
sleep 40
docker compose logs --tail 20
"""

_, so, _ = c.exec_command(script, timeout=300)
print(so.read().decode())
c.close()
