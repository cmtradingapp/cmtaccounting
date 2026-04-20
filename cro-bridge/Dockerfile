# cro-bridge: Wine-hosted MT5 Manager bridge, driven by a C# helper.
#
# Architecture:
#   - Linux Python 3 (cro_live_pusher.py) starts MT5LivePusher.exe once and
#     reads its stdout indefinitely. The exe holds a persistent PUMP_MODE_POSITIONS
#     connection and emits one JSON line every CRO_INTERVAL seconds.
#   - The .exe runs under Wine's bundled Wine-Mono .NET runtime, which loads
#     the native MT5APIManager64.dll (a Windows PE binary) via Wine's loader.
#   - MT5Bridge.exe (one-shot legacy) is also compiled but not used in production.
#
# Critical:
#   Run-time MUST set  WINEDLLOVERRIDES=mscoree=n,b;fusion=n,b  (see compose).
#   Without this Wine fails to load mscoree and the .NET loader can't start.
FROM tobix/pywine:3.11

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    WINEDEBUG=-all

# Mono (for compiling C#), native Linux Python 3, basic utils
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates wget \
        mono-mcs mono-runtime libmono-system-core4.0-cil \
        libmono-system-runtime-serialization4.0-cil \
        libmono-system-xml4.0-cil \
        python3 python3-urllib3 tzdata \
 && rm -rf /var/lib/apt/lists/*

# Install Wine-Mono (open-source .NET Framework 4.8-compatible runtime for Wine)
RUN wget -qO /tmp/wine-mono.msi \
        https://dl.winehq.org/wine/wine-mono/9.4.0/wine-mono-9.4.0-x86.msi \
 && wine msiexec /i /tmp/wine-mono.msi /quiet 2>&1 | tail -3 \
 && wineserver -w \
 && rm /tmp/wine-mono.msi

# Compile the C# helpers against the MT5 .NET wrappers
WORKDIR /app
COPY sdk-libs /sdk-libs
COPY MT5Bridge.cs MT5LivePusher.cs MT5Reporter.cs /app/
COPY reusable-api/Mt5MonitorApiBundle.cs /app/
RUN mcs -out:/app/MT5Bridge.exe \
        -reference:/sdk-libs/MetaQuotes.MT5CommonAPI64.dll \
        -reference:/sdk-libs/MetaQuotes.MT5ManagerAPI64.dll \
        /app/MT5Bridge.cs \
 && mcs -out:/app/MT5LivePusher.exe \
        -reference:/sdk-libs/MetaQuotes.MT5CommonAPI64.dll \
        -reference:/sdk-libs/MetaQuotes.MT5ManagerAPI64.dll \
        -r:System.Runtime.Serialization \
        -r:System.Xml \
        /app/Mt5MonitorApiBundle.cs /app/MT5LivePusher.cs \
 && mcs -out:/app/MT5Reporter.exe \
        -reference:/sdk-libs/MetaQuotes.MT5CommonAPI64.dll \
        -reference:/sdk-libs/MetaQuotes.MT5ManagerAPI64.dll \
        -r:System.Runtime.Serialization \
        -r:System.Xml \
        /app/Mt5MonitorApiBundle.cs /app/MT5Reporter.cs \
 && cp /sdk-libs/*.dll /app/

COPY cro_bridge_pusher.py cro_live_pusher.py cro_report_server.py /app/

# Startup script: run report server in background, then start live pusher
RUN printf '#!/bin/sh\npython3 /app/cro_report_server.py &\nexec python3 /app/cro_live_pusher.py\n' \
    > /app/start.sh && chmod +x /app/start.sh

ENV MT5_SDK_LIBS=Z:/app

CMD ["/app/start.sh"]
