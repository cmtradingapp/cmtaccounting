import io
import os
import functools
from flask import Flask, render_template, request, jsonify, send_file, abort, Response
from dotenv import load_dotenv

load_dotenv()

import queries
import openpyxl

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "dev")

_AUTH_USER = os.environ.get("AUTH_USER", "")
_AUTH_PASS = os.environ.get("AUTH_PASS", "")


def _unauthorized():
    return Response(
        "Authentication required.",
        401,
        {"WWW-Authenticate": 'Basic realm="CMT Reconciliation"'},
    )


def require_auth(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not _AUTH_USER or not _AUTH_PASS:
            abort(500, "AUTH_USER and AUTH_PASS env vars not set.")
        auth = request.authorization
        if not auth or auth.username != _AUTH_USER or auth.password != _AUTH_PASS:
            return _unauthorized()
        return f(*args, **kwargs)
    return wrapper


@app.route("/")
@require_auth
def index():
    try:
        months = queries.available_months()
    except Exception as e:
        months = []
    return render_template("index.html", months=months)


@app.route("/recon/<month>")
@require_auth
def recon(month):
    try:
        year, mon = int(month[:4]), int(month[5:7])
    except (ValueError, IndexError):
        abort(400)
    try:
        rows = queries.reconcile(year, mon)
        stats = queries.summary_stats(rows)
        months = queries.available_months()
    except Exception as e:
        return render_template("index.html", months=[], error=str(e))

    status_filter = request.args.get("status", "all")
    if status_filter != "all":
        rows = [r for r in rows if r["status"] == status_filter]

    return render_template(
        "recon.html",
        month=month,
        rows=rows,
        stats=stats,
        months=months,
        status_filter=status_filter,
    )


@app.route("/recon/<month>/export")
@require_auth
def export(month):
    try:
        year, mon = int(month[:4]), int(month[5:7])
    except (ValueError, IndexError):
        abort(400)

    rows = queries.reconcile(year, mon)
    stats = queries.summary_stats(rows)

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = f"Recon {month}"

    headers = ["Login", "MT4 Net (USD)", "CRM Net (USD)", "CRM Deposits",
               "CRM Withdrawals", "Difference", "Status", "Payment Methods", "Tx Count", "Currency"]
    ws.append(headers)

    for r in rows:
        ws.append([
            r["login"], r["mt4_net"], r["crm_net"],
            r["crm_deposits"], r["crm_withdrawals"],
            r["difference"], r["status"],
            r["payment_methods"], r["tx_count"], r["currency"],
        ])

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return send_file(
        buf,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=f"recon_{month}.xlsx",
    )


@app.route("/recon/<month>/<int:login>")
@require_auth
def login_detail(month, login):
    try:
        year, mon = int(month[:4]), int(month[5:7])
    except (ValueError, IndexError):
        abort(400)
    crm_rows  = queries.login_detail(year, mon, login)
    mt4_rows  = queries.login_mt4_detail(year, mon, login)
    months    = queries.available_months()
    return render_template("detail.html", month=month, login=login,
                           crm_rows=crm_rows, mt4_rows=mt4_rows, months=months)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050, debug=False)
