"""cro-report-server — HTTP server that runs on-demand MT5 reports.

Listens on :5051; accepts POST /report with JSON body:
  {"type": "deposit-withdrawal"|"positions-history"|"trading-accounts",
   "from_date": "YYYY-MM-DD",  (omit for trading-accounts)
   "to_date":   "YYYY-MM-DD",  (omit for trading-accounts)
   "format":    "json"|"csv"}

Invokes wine /app/MT5Reporter.exe with the appropriate args and streams
the output back as the HTTP response body.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

EXE_PATH = os.environ.get("CRO_REPORTER_EXE", "/app/MT5Reporter.exe")
PORT = int(os.environ.get("CRO_REPORT_PORT", "5051"))

CONTENT_TYPES = {
    "json": "application/json",
    "csv":  "text/csv",
}

REPORT_TYPES = {"deposit-withdrawal", "positions-history", "trading-accounts"}


class ReportHandler(BaseHTTPRequestHandler):
    timeout = 660  # override default 30s — must outlive the subprocess timeout (600s)
    def log_message(self, fmt, *args):
        print(f"[report-server] {fmt % args}", file=sys.stderr, flush=True)

    def do_POST(self):
        if self.path != "/report":
            self._reply(404, "text/plain", b"Not found")
            return

        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else b"{}"
        try:
            req = json.loads(body)
        except Exception:
            self._reply(400, "text/plain", b"Invalid JSON")
            return

        report_type = (req.get("type") or "").strip().lower()
        fmt         = (req.get("format") or "json").strip().lower()
        from_date   = (req.get("from_date") or "").strip()
        to_date     = (req.get("to_date") or "").strip()

        if report_type not in REPORT_TYPES:
            self._reply(400, "text/plain",
                        f"Unknown type: {report_type}. Valid: {', '.join(sorted(REPORT_TYPES))}".encode())
            return

        if fmt not in CONTENT_TYPES:
            self._reply(400, "text/plain", b"format must be json or csv")
            return

        if report_type == "trading-accounts":
            cmd_args = [fmt]
        else:
            if not from_date or not to_date:
                self._reply(400, "text/plain", b"from_date and to_date required")
                return
            cmd_args = [from_date, to_date, fmt]

        cmd = ["wine", EXE_PATH, report_type] + cmd_args
        env = os.environ.copy()
        try:
            result = subprocess.run(
                cmd, env=env,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                timeout=600, text=True,
            )
        except subprocess.TimeoutExpired:
            self._reply(504, "text/plain", b"Report generation timed out (600s)")
            return
        except Exception as e:
            self._reply(500, "text/plain", str(e).encode())
            return

        if result.returncode != 0:
            err = result.stderr.strip()[-1000:]
            print(f"[report-server] exe error: {err}", file=sys.stderr, flush=True)
            self._reply(500, "text/plain", f"Reporter failed: {err}".encode())
            return

        data = result.stdout.encode("utf-8")
        self._reply(200, CONTENT_TYPES[fmt], data)

    def _reply(self, code: int, content_type: str, body: bytes):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main():
    server = ThreadingHTTPServer(("0.0.0.0", PORT), ReportHandler)
    print(f"[report-server] listening on :{PORT}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
