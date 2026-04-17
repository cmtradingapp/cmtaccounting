"""Flask app for the CRO 'All in One' dashboard.

Run from the project venv:

    .venv\\Scripts\\python.exe cro_app.py          # port 5060
    set FLASK_RUN_PORT=5070 && .venv\\Scripts\\python.exe cro_app.py

Env vars:
    MT5_SERVER    default 176.126.66.18:1950
    MT5_LOGIN     default 1111
    MT5_PASSWORD  default (hardcoded in mt5-an100-credentials.md)
    CRO_GROUP     default real\\*
    CRO_AUTH      optional basic-auth password (if set, HTTP basic auth required)
"""
from __future__ import annotations

import os
import threading
from dataclasses import asdict
from datetime import date, datetime
from functools import wraps

import hmac
import secrets
import sys

from flask import (
    Flask, jsonify, redirect, render_template, request, session, url_for,
)

from mt5_bridge import MT5Bridge, MT5Error
import cro_cache
import cro_metrics

# ── configuration ──────────────────────────────────────────────────────────
SERVER   = os.environ.get("MT5_SERVER",   "176.126.66.18:1950")
LOGIN    = int(os.environ.get("MT5_LOGIN", "1111"))
PASSWORD = os.environ.get("MT5_PASSWORD", "Zt*pE5AkZ_SkEgH5")
GROUP    = os.environ.get("CRO_GROUP",    "CMV*")
# Fail-closed: never start without an explicit password.
CRO_PASSWORD = os.environ.get("CRO_PASSWORD")
if not CRO_PASSWORD:
    print("[cro] ERROR: CRO_PASSWORD env var is required.", file=sys.stderr)
    raise SystemExit(2)
CRO_SECRET   = os.environ.get("CRO_SECRET") or secrets.token_urlsafe(32)
PORT         = int(os.environ.get("FLASK_RUN_PORT", "5060"))

# ── shared MT5 bridge (thread-safe accessor) ───────────────────────────────
_bridge: MT5Bridge | None = None
_bridge_lock = threading.Lock()


def _get_bridge() -> MT5Bridge:
    global _bridge
    with _bridge_lock:
        if _bridge is None:
            b = MT5Bridge()
            b.connect(SERVER, LOGIN, PASSWORD)
            _bridge = b
        return _bridge


def _drop_bridge() -> None:
    global _bridge
    with _bridge_lock:
        if _bridge is not None:
            try:
                _bridge.disconnect()
            except Exception:
                pass
            _bridge = None


# ── Flask app ──────────────────────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = CRO_SECRET
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    PERMANENT_SESSION_LIFETIME=60 * 60 * 12,  # 12 h
)

PUBLIC_PATHS = {"/login", "/api/health", "/static"}


def _is_authed() -> bool:
    return bool(session.get("cro_ok"))


@app.before_request
def _gate():
    p = request.path
    if p.startswith("/static/") or p in PUBLIC_PATHS:
        return None
    if _is_authed():
        return None
    if p.startswith("/api/"):
        return jsonify({"ok": False, "error": "auth required"}), 401
    return redirect(url_for("login", next=p))


@app.get("/login")
def login():
    err = request.args.get("err")
    return render_template("cro_login.html", err=err)


@app.post("/login")
def login_post():
    pw = request.form.get("password", "")
    if not hmac.compare_digest(pw, CRO_PASSWORD):
        return redirect(url_for("login", err="1"))
    session.permanent = True
    session["cro_ok"] = True
    nxt = request.args.get("next") or "/"
    if not nxt.startswith("/"):
        nxt = "/"
    return redirect(nxt)


@app.get("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.get("/")
def index():
    today = date.today().isoformat()
    return render_template(
        "cro_dashboard.html",
        default_date=today,
        default_group=GROUP,
        server=SERVER,
    )


def _parse_date(s: str | None, default: date) -> date:
    if not s:
        return default
    return datetime.strptime(s, "%Y-%m-%d").date()


def _compute_day(day: date, group: str) -> cro_metrics.Snapshot:
    try:
        bridge = _get_bridge()
        return cro_metrics.compute_snapshot(bridge, day, group)
    except MT5Error:
        _drop_bridge()
        raise


def _compute_month(year: int, month: int, group: str) -> cro_metrics.Snapshot:
    try:
        bridge = _get_bridge()
        return cro_metrics.compute_month_snapshot(bridge, year, month, group)
    except MT5Error:
        _drop_bridge()
        raise


@app.get("/api/day")
def api_day():
    day = _parse_date(request.args.get("date"), date.today())
    group = request.args.get("group", GROUP)
    force = request.args.get("force") == "1"
    snap = cro_cache.or_compute(
        "day", day.isoformat(), group,
        lambda: _compute_day(day, group),
        force=force,
    )
    return jsonify(asdict(snap))


@app.get("/api/month")
def api_month():
    ym = request.args.get("month")  # YYYY-MM
    today = date.today()
    if ym:
        y, m = map(int, ym.split("-"))
    else:
        y, m = today.year, today.month
    group = request.args.get("group", GROUP)
    force = request.args.get("force") == "1"
    snap = cro_cache.or_compute(
        "month", f"{y:04d}-{m:02d}", group,
        lambda: _compute_month(y, m, group),
        force=force,
    )
    return jsonify(asdict(snap))


@app.post("/api/refresh")
def api_refresh():
    day = _parse_date(request.args.get("date"), date.today())
    group = request.args.get("group", GROUP)
    cro_cache.invalidate("day", day.isoformat(), group)
    snap = cro_cache.or_compute(
        "day", day.isoformat(), group,
        lambda: _compute_day(day, group),
        force=True,
    )
    return jsonify({"ok": True, "label": snap.label})


@app.get("/api/health")
def health():
    try:
        b = _get_bridge()
        return jsonify({"ok": True, "api_version": b.get_version(), "server": SERVER})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=PORT, debug=True, use_reloader=False)
