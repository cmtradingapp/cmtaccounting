# cro-bridge: Wine-hosted MT5 Manager bridge, driven by a C# helper.
#
# Architecture:
#   - Linux Python 3 runs the pusher loop.
#   - Every 30s it execs `wine /app/MT5Bridge.exe` -- our tiny C# helper
#     compiled with Mono's mcs against the MT5 Manager .NET wrapper.
#   - The .exe runs under Wine's bundled Wine-Mono .NET runtime, which loads
#     the native MT5APIManager64.dll (a Windows PE binary) via Wine's loader.
#   - The .exe prints one JSON line on stdout; Python POSTs it to /cro/feed.
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
COPY MT5Bridge.cs MT5LivePusher.cs /app/
RUN mcs -out:/app/MT5Bridge.exe \
        -reference:/sdk-libs/MetaQuotes.MT5CommonAPI64.dll \
        -reference:/sdk-libs/MetaQuotes.MT5ManagerAPI64.dll \
        /app/MT5Bridge.cs \
 && mcs -out:/app/MT5LivePusher.exe \
        -reference:/sdk-libs/MetaQuotes.MT5CommonAPI64.dll \
        -reference:/sdk-libs/MetaQuotes.MT5ManagerAPI64.dll \
        /app/MT5LivePusher.cs \
 && cp /sdk-libs/*.dll /app/

COPY cro_bridge_pusher.py cro_live_pusher.py /app/

ENV MT5_SDK_LIBS=Z:/app

CMD ["python3", "/app/cro_live_pusher.py"]
