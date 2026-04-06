import io
import os
import functools
from datetime import date
from flask import Flask, render_template, request, jsonify, send_file, abort, Response, redirect, url_for
from dotenv import load_dotenv

load_dotenv()

import queries
import ai_parse
import openpyxl

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET", "dev")

@app.context_processor
def inject_request():
    return {"request": request}

try:
    queries.ensure_fee_tables()
except Exception as e:
    print(f"WARNING: Could not create fee tables: {e}")

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


# ---------------------------------------------------------------------------
# PSP Fee Management
# ---------------------------------------------------------------------------

FEE_TYPES = [
    "Deposit", "Withdrawal", "Settlement", "Chargeback", "Refund",
    "Rolling Reserve", "Holdback", "Setup", "Registration", "Minimum Monthly",
]

PAYMENT_METHODS = [
    "Credit Cards", "Bank Wire", "Mobile Money", "Electronic Payment",
    "Crypto", "MOMO", "E-Wallet",
]

CURRENCIES = [
    "USD", "EUR", "GBP", "ZAR", "NGN", "KES", "GHS", "UGX", "TZS",
    "RWF", "XOF", "XAF", "AED", "BRL", "CLP", "COP", "MXN", "PEN",
]

@app.route("/fees")
@require_auth
def fees_list():
    agreements = queries.get_all_agreements()
    terminated = queries.get_terminated_agreements()
    entities = queries.get_entities()
    return render_template("fees.html", agreements=agreements, terminated=terminated, entities=entities)


@app.route("/fees/entities/add", methods=["POST"])
@require_auth
def fees_entity_add():
    name = request.form.get("name", "").strip().upper()
    if name:
        queries.add_entity(name)
    return redirect(url_for("fees_list"))


@app.route("/fees/entities/delete", methods=["POST"])
@require_auth
def fees_entity_delete():
    name = request.form.get("name", "")
    queries.delete_entity(name)
    return redirect(url_for("fees_list"))


@app.route("/fees/new", methods=["GET", "POST"])
@require_auth
def fees_new():
    if request.method == "POST":
        data = {
            "psp_name":         request.form["psp_name"].strip(),
            "provider_name":    request.form.get("provider_name", "").strip() or None,
            "agreement_entity": request.form.get("agreement_entity", "").strip() or None,
            "agreement_date":   request.form.get("agreement_date") or None,
            "addendum_date":    request.form.get("addendum_date") or None,
            "auto_settlement":  request.form.get("auto_settlement") == "on",
            "settlement_bank":  request.form.get("settlement_bank", "").strip() or None,
        }
        if not data["psp_name"]:
            abort(400, "PSP name is required")
        psp_id = queries.create_agreement(data)
        return redirect(url_for("fees_detail", psp_id=psp_id))
    return render_template("fee_form.html", agreement=None, entities=queries.get_entities(),
                           today=date.today().isoformat())


@app.route("/fees/<int:psp_id>")
@require_auth
def fees_detail(psp_id):
    agreement = queries.get_agreement(psp_id)
    if not agreement:
        abort(404)
    rules = queries.get_fee_rules(psp_id)
    return render_template("fee_detail.html", agreement=agreement, rules=rules,
                           fee_types=FEE_TYPES, payment_methods=PAYMENT_METHODS,
                           currencies=CURRENCIES, entities=queries.get_entities())


@app.route("/fees/<int:psp_id>/edit", methods=["POST"])
@require_auth
def fees_edit(psp_id):
    data = {
        "psp_name":         request.form["psp_name"].strip(),
        "provider_name":    request.form.get("provider_name", "").strip() or None,
        "agreement_entity": request.form.get("agreement_entity", "").strip() or None,
        "agreement_date":   request.form.get("agreement_date") or None,
        "addendum_date":    request.form.get("addendum_date") or None,
        "auto_settlement":  request.form.get("auto_settlement") == "on",
        "settlement_bank":  request.form.get("settlement_bank", "").strip() or None,
    }
    queries.update_agreement(psp_id, data)
    return redirect(url_for("fees_detail", psp_id=psp_id))


@app.route("/fees/<int:psp_id>/delete", methods=["POST"])
@require_auth
def fees_delete(psp_id):
    queries.delete_agreement(psp_id)
    return redirect(url_for("fees_list"))


@app.route("/fees/<int:psp_id>/rules/add", methods=["POST"])
@require_auth
def fees_add_rule(psp_id):
    kind = request.form["fee_kind"]
    data = {
        "payment_method": request.form.get("payment_method", "").strip() or None,
        "fee_type":       request.form["fee_type"],
        "country":        request.form.get("country", "GLOBAL").strip() or "GLOBAL",
        "sub_provider":   request.form.get("sub_provider", "").strip() or None,
        "fee_kind":       kind,
        "pct_rate":       None,
        "fixed_amount":   None,
        "fixed_currency": None,
        "description":    request.form.get("description", "").strip() or None,
        "tiers":          [],
    }

    if kind in ("percentage", "fixed_plus_pct"):
        raw = request.form.get("pct_rate", "")
        if raw:
            data["pct_rate"] = float(raw) / 100.0

    if kind in ("fixed", "fixed_plus_pct"):
        raw = request.form.get("fixed_amount", "")
        if raw:
            data["fixed_amount"] = float(raw)
        data["fixed_currency"] = request.form.get("fixed_currency", "USD")

    if kind == "tiered":
        froms = request.form.getlist("tier_from")
        tos   = request.form.getlist("tier_to")
        rates = request.form.getlist("tier_rate")
        for f, t, r in zip(froms, tos, rates):
            if r:
                data["tiers"].append({
                    "volume_from": float(f) if f else 0,
                    "volume_to":   float(t) if t else None,
                    "pct_rate":    float(r) / 100.0,
                })

    queries.create_fee_rule(psp_id, data)
    return redirect(url_for("fees_detail", psp_id=psp_id))


@app.route("/fees/rules/<int:rule_id>/edit", methods=["POST"])
@require_auth
def fees_edit_rule(rule_id):
    psp_id = request.form.get("psp_id", type=int)
    kind = request.form["fee_kind"]
    data = {
        "payment_method": request.form.get("payment_method", "").strip() or None,
        "fee_type":       request.form["fee_type"],
        "country":        request.form.get("country", "GLOBAL").strip() or "GLOBAL",
        "sub_provider":   request.form.get("sub_provider", "").strip() or None,
        "fee_kind":       kind,
        "pct_rate":       None,
        "fixed_amount":   None,
        "fixed_currency": None,
        "description":    request.form.get("description", "").strip() or None,
        "tiers":          [],
    }
    if kind in ("percentage", "fixed_plus_pct"):
        raw = request.form.get("pct_rate", "")
        if raw:
            data["pct_rate"] = float(raw) / 100.0
    if kind in ("fixed", "fixed_plus_pct"):
        raw = request.form.get("fixed_amount", "")
        if raw:
            data["fixed_amount"] = float(raw)
        data["fixed_currency"] = request.form.get("fixed_currency", "USD")
    if kind == "tiered":
        froms = request.form.getlist("tier_from")
        tos   = request.form.getlist("tier_to")
        rates = request.form.getlist("tier_rate")
        for f, t, r in zip(froms, tos, rates):
            if r:
                data["tiers"].append({
                    "volume_from": float(f) if f else 0,
                    "volume_to":   float(t) if t else None,
                    "pct_rate":    float(r) / 100.0,
                })
    queries.update_fee_rule(rule_id, data)
    return redirect(url_for("fees_detail", psp_id=psp_id))


@app.route("/fees/rules/<int:rule_id>/delete", methods=["POST"])
@require_auth
def fees_delete_rule(rule_id):
    psp_id = request.form.get("psp_id", type=int)
    queries.delete_fee_rule(rule_id)
    return redirect(url_for("fees_detail", psp_id=psp_id))


# ---------------------------------------------------------------------------
# AI Agreement Upload
# ---------------------------------------------------------------------------

ALLOWED_UPLOAD_EXTS = {"pdf", "docx", "doc"}


@app.route("/fees/upload", methods=["GET", "POST"])
@require_auth
def fees_upload():
    if request.method == "GET":
        return render_template("fee_upload.html")

    f = request.files.get("agreement_file")
    if not f or not f.filename:
        return render_template("fee_upload.html", error="No file selected.")

    ext = f.filename.rsplit(".", 1)[-1].lower() if "." in f.filename else ""
    if ext not in ALLOWED_UPLOAD_EXTS:
        return render_template("fee_upload.html",
                               error="Only PDF and DOCX files are supported.")

    try:
        file_bytes = f.read()
        text = ai_parse.extract_text(file_bytes, f.filename)
    except Exception as e:
        return render_template("fee_upload.html",
                               error=f"Could not read file: {e}")

    if not text.strip():
        return render_template("fee_upload.html",
                               error="No text could be extracted from this file. "
                                     "It may be a scanned image PDF — try a DOCX version instead.")

    try:
        result = ai_parse.analyze_agreement(text)
    except Exception as e:
        return render_template("fee_upload.html",
                               error=f"AI analysis failed: {e}")

    return render_template(
        "fee_confirm.html",
        agreement=result.get("agreement", {}),
        fee_rules=result.get("fee_rules", []),
        entities=queries.get_entities(),
        fee_types=FEE_TYPES,
        payment_methods=PAYMENT_METHODS,
        currencies=CURRENCIES,
        filename=f.filename,
    )


@app.route("/fees/upload/confirm", methods=["POST"])
@require_auth
def fees_upload_confirm():
    data = {
        "psp_name":         request.form["psp_name"].strip(),
        "provider_name":    request.form.get("provider_name", "").strip() or None,
        "agreement_entity": request.form.get("agreement_entity", "").strip() or None,
        "agreement_date":   request.form.get("agreement_date") or None,
        "addendum_date":    request.form.get("addendum_date") or None,
        "auto_settlement":  request.form.get("auto_settlement") == "on",
        "settlement_bank":  request.form.get("settlement_bank", "").strip() or None,
    }
    if not data["psp_name"]:
        abort(400, "PSP name is required")

    psp_id = queries.create_agreement(data)

    rule_count = int(request.form.get("rule_count", 0))
    for i in range(rule_count):
        kind = request.form.get(f"fee_kind_{i}", "percentage")
        rule = {
            "payment_method": request.form.get(f"payment_method_{i}", "").strip() or None,
            "fee_type":       request.form.get(f"fee_type_{i}", "Deposit"),
            "country":        request.form.get(f"country_{i}", "GLOBAL").strip() or "GLOBAL",
            "sub_provider":   request.form.get(f"sub_provider_{i}", "").strip() or None,
            "fee_kind":       kind,
            "pct_rate":       None,
            "fixed_amount":   None,
            "fixed_currency": None,
            "description":    request.form.get(f"description_{i}", "").strip() or None,
            "tiers":          [],
        }
        if kind in ("percentage", "fixed_plus_pct"):
            raw = request.form.get(f"pct_rate_{i}", "")
            if raw:
                rule["pct_rate"] = float(raw) / 100.0
        if kind in ("fixed", "fixed_plus_pct"):
            raw = request.form.get(f"fixed_amount_{i}", "")
            if raw:
                rule["fixed_amount"] = float(raw)
            rule["fixed_currency"] = request.form.get(f"fixed_currency_{i}", "USD")
        if kind == "tiered":
            froms = request.form.getlist(f"tier_from_{i}")
            tos   = request.form.getlist(f"tier_to_{i}")
            rates = request.form.getlist(f"tier_rate_{i}")
            for f_v, t_v, r_v in zip(froms, tos, rates):
                if r_v:
                    rule["tiers"].append({
                        "volume_from": float(f_v) if f_v else 0,
                        "volume_to":   float(t_v) if t_v else None,
                        "pct_rate":    float(r_v) / 100.0,
                    })
        queries.create_fee_rule(psp_id, rule)

    return redirect(url_for("fees_detail", psp_id=psp_id))


if __name__ == "__main__":
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=5050, debug=debug)
