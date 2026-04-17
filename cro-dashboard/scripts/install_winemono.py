"""Build a fresh cro-bridge image with Wine-Mono and test."""
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

# Build a FRESH image from tobix/pywine + wine-mono (no dotnet48!)
script = r"""
cat > /tmp/Dockerfile.winemono <<'DOCKER_EOF'
FROM tobix/pywine:3.11

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates wget \
        mono-mcs mono-runtime libmono-system-core4.0-cil \
        python3 python3-urllib3 \
 && rm -rf /var/lib/apt/lists/*

# Install Wine-Mono (the open-source .NET Framework replacement bundled with Wine)
RUN mkdir -p /opt/wineprefix/drive_c/windows/mono \
 && wget -qO /tmp/wine-mono.msi https://dl.winehq.org/wine/wine-mono/9.4.0/wine-mono-9.4.0-x86.msi \
 && wine msiexec /i /tmp/wine-mono.msi /quiet 2>&1 | tail -5 \
 && wineserver -w \
 && rm /tmp/wine-mono.msi

WORKDIR /app
COPY sdk-libs /sdk-libs
COPY MT5Bridge.cs /app/
RUN mcs -out:/app/MT5Bridge.exe \
        -reference:/sdk-libs/MetaQuotes.MT5CommonAPI64.dll \
        -reference:/sdk-libs/MetaQuotes.MT5ManagerAPI64.dll \
        /app/MT5Bridge.cs \
 && cp /sdk-libs/*.dll /app/

COPY cro_bridge_pusher.py /app/
ENV MT5_SDK_LIBS=Z:/app
CMD ["python3", "/app/cro_bridge_pusher.py"]
DOCKER_EOF

cd /root/cro-bridge
docker build -t cro-bridge-cro-bridge:latest -f /tmp/Dockerfile.winemono . 2>&1 | tail -15
echo === test ===
docker run --rm --env-file /root/recon-app/.env --network recon-app_recon_net \
  --entrypoint bash cro-bridge-cro-bridge:latest -c '
wine /app/MT5Bridge.exe
echo "exit=$?"
' 2>&1 | tail -30
"""

_, so, _ = c.exec_command(script, timeout=900)
print(so.read().decode())
c.close()
