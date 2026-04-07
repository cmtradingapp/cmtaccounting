import io
import os
import uuid
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

_RECON_USER = os.environ.get("RECON_USER", "")
_RECON_PASS = os.environ.get("RECON_PASS", "")
_FEES_USER  = os.environ.get("FEES_USER",  "")
_FEES_PASS  = os.environ.get("FEES_PASS",  "")


def _unauthorized(realm):
    return Response(
        "Authentication required.",
        401,
        {"WWW-Authenticate": f'Basic realm="{realm}"'},
    )


def require_recon_auth(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not _RECON_USER or not _RECON_PASS:
            abort(500, "RECON_USER and RECON_PASS env vars not set.")
        auth = request.authorization
        if not auth or auth.username != _RECON_USER or auth.password != _RECON_PASS:
            return _unauthorized("CMT Reconciliation")
        return f(*args, **kwargs)
    return wrapper


def require_fees_auth(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not _FEES_USER or not _FEES_PASS:
            abort(500, "FEES_USER and FEES_PASS env vars not set.")
        auth = request.authorization
        if not auth or auth.username != _FEES_USER or auth.password != _FEES_PASS:
            return _unauthorized("CMT Fee Processor")
        return f(*args, **kwargs)
    return wrapper


@app.route("/")
@require_recon_auth
def index():
    try:
        months = queries.available_months()
    except Exception as e:
        months = []
    return render_template("index.html", months=months)


@app.route("/recon/<month>")
@require_recon_auth
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
@require_recon_auth
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
@require_recon_auth
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
@require_fees_auth
def fees_list():
    agreements = queries.get_all_agreements()
    terminated = queries.get_terminated_agreements()
    entities = queries.get_entities()
    return render_template("fees.html", agreements=agreements, terminated=terminated, entities=entities)


@app.route("/fees/entities/add", methods=["POST"])
@require_fees_auth
def fees_entity_add():
    name = request.form.get("name", "").strip().upper()
    if name:
        queries.add_entity(name)
    return redirect(url_for("fees_list"))


@app.route("/fees/entities/delete", methods=["POST"])
@require_fees_auth
def fees_entity_delete():
    name = request.form.get("name", "")
    queries.delete_entity(name)
    return redirect(url_for("fees_list"))


@app.route("/fees/new", methods=["GET", "POST"])
@require_fees_auth
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
@require_fees_auth
def fees_detail(psp_id):
    agreement = queries.get_agreement(psp_id)
    if not agreement:
        abort(404)
    rules = queries.get_fee_rules(psp_id)
    return render_template("fee_detail.html", agreement=agreement, rules=rules,
                           fee_types=FEE_TYPES, payment_methods=PAYMENT_METHODS,
                           currencies=CURRENCIES, entities=queries.get_entities())


@app.route("/fees/<int:psp_id>/edit", methods=["POST"])
@require_fees_auth
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
@require_fees_auth
def fees_delete(psp_id):
    queries.delete_agreement(psp_id)
    return redirect(url_for("fees_list"))


@app.route("/fees/<int:psp_id>/rules/add", methods=["POST"])
@require_fees_auth
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
@require_fees_auth
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
@require_fees_auth
def fees_delete_rule(rule_id):
    psp_id = request.form.get("psp_id", type=int)
    queries.delete_fee_rule(rule_id)
    return redirect(url_for("fees_detail", psp_id=psp_id))


# ---------------------------------------------------------------------------
# AI Agreement Upload
# ---------------------------------------------------------------------------

ALLOWED_UPLOAD_EXTS = {"pdf", "docx", "doc"}


@app.route("/fees/upload", methods=["GET", "POST"])
@require_fees_auth
def fees_upload():
    templates     = queries.get_prompt_templates()
    context_notes = queries.get_context_notes()

    # Amendment mode: psp_id present → amending an existing agreement
    psp_id_get  = request.args.get("psp_id", type=int)
    amend_agr   = queries.get_agreement(psp_id_get) if psp_id_get else None

    # Choose the default template for the current mode (each type has its own default)
    if amend_agr:
        default_tpl = next((t for t in templates if t.get("prompt_type") == "amendment" and t["is_default"]), None)
        if not default_tpl:
            default_tpl = next((t for t in templates if t.get("prompt_type") == "amendment"), None)
    else:
        default_tpl = next((t for t in templates if t.get("prompt_type", "agreement") == "agreement" and t["is_default"]), None)
        if not default_tpl:
            default_tpl = next((t for t in templates if t["is_default"]), None)
    if not default_tpl and templates:
        default_tpl = templates[0]

    if request.method == "GET":
        return render_template("fee_upload.html", templates=templates,
                               default_tpl=default_tpl, context_notes=context_notes,
                               amendment_agreement=amend_agr)

    def _render_error(msg):
        psp_id_post = request.form.get("psp_id", type=int)
        agr = queries.get_agreement(psp_id_post) if psp_id_post else None
        return render_template("fee_upload.html", templates=templates,
                               default_tpl=default_tpl, context_notes=context_notes,
                               amendment_agreement=agr, error=msg)

    f = request.files.get("agreement_file")
    if not f or not f.filename:
        return _render_error("No file selected.")

    ext = f.filename.rsplit(".", 1)[-1].lower() if "." in f.filename else ""
    if ext not in ALLOWED_UPLOAD_EXTS:
        return _render_error("Only PDF and DOCX files are supported.")

    # Resolve selected prompt template
    tpl_id = request.form.get("prompt_template_id", type=int)
    tpl    = queries.get_prompt_template(tpl_id) if tpl_id else default_tpl
    system_prompt = tpl["system_prompt"] if tpl else None

    # Collect all extra context: checked saved notes + newly typed boxes
    extra_sections = []

    checked_ids = request.form.getlist("saved_note_ids", type=int)
    if checked_ids:
        queries.increment_context_note_usage(checked_ids)
        saved = {n["id"]: n for n in context_notes}
        for nid in checked_ids:
            if nid in saved:
                extra_sections.append((saved[nid]["label"], saved[nid]["text"]))

    ctx_labels     = request.form.getlist("ctx_label")
    ctx_texts      = request.form.getlist("ctx_text")
    ctx_saves      = request.form.getlist("ctx_save")       # "1" or "" per box
    ctx_note_types = request.form.getlist("ctx_note_type")  # "agreement" or "amendment" per box
    for i, (label, text) in enumerate(zip(ctx_labels, ctx_texts)):
        text = text.strip()
        if not text:
            continue
        label     = label.strip() or "Additional Note"
        note_type = ctx_note_types[i] if i < len(ctx_note_types) else "agreement"
        extra_sections.append((label, text))
        if i < len(ctx_saves) and ctx_saves[i] == "1":
            queries.save_context_note(label, text, note_type=note_type)

    if extra_sections:
        divider = "\n" + "═" * 55
        addon = divider + "\n  ADDITIONAL CONTEXT (provided before analysis)\n" + divider
        for label, text in extra_sections:
            addon += f"\n\n── {label} ──\n{text}"
        system_prompt = (system_prompt or "") + "\n\n" + addon

    # Amendment mode: psp_id in form → amend an existing agreement
    psp_id_post = request.form.get("psp_id", type=int)

    try:
        file_bytes = f.read()
        text = ai_parse.extract_text(file_bytes, f.filename)
    except Exception as e:
        return _render_error(f"Could not read file: {e}")

    if not text.strip():
        return _render_error("No text could be extracted from this file. "
                             "It may be a scanned image PDF — try a DOCX version instead.")

    # ── New agreement flow: save file alongside the agreement ───────────────
    if not psp_id_post:
        # file_bytes available; will be saved after agreement is created in confirm step
        # cache now so confirm can retrieve it
        agr_cache_token = str(uuid.uuid4())
        queries.cache_upload(agr_cache_token, f.filename, file_bytes)
    else:
        agr_cache_token = None

    # ── Amendment flow ──────────────────────────────────────────────────────
    if psp_id_post:
        amendment_agreement = queries.get_agreement(psp_id_post)
        if not amendment_agreement:
            return _render_error("PSP not found.")

        try:
            result = ai_parse.analyze_amendment(text, system_prompt=system_prompt)
        except Exception as e:
            return _render_error(f"AI analysis failed: {e}")

        raw_changes   = result.get("rule_changes", [])
        amend_meta    = result.get("amendment", {})
        addendum_date = amend_meta.get("addendum_date") or ""
        notes         = amend_meta.get("notes") or ""

        # Cache file so confirm route can persist it
        cache_token = str(uuid.uuid4())
        queries.cache_upload(cache_token, f.filename, file_bytes)

        existing_rules = queries.get_fee_rules(psp_id_post)

        def _find_match(match_on):
            pm = (match_on.get("payment_method") or "").lower()
            co = (match_on.get("country") or "").lower()
            ft = (match_on.get("fee_type") or "").lower()
            for r in existing_rules:
                if (
                    (r.get("payment_method") or "").lower() == pm
                    and (r.get("country") or "").lower() == co
                    and (r.get("fee_type") or "").lower() == ft
                ):
                    return r
            return None

        changes = []
        for rc in raw_changes:
            action   = rc.get("action", "add")
            match_on = rc.get("match_on") or {}
            existing = None
            if action in ("replace", "remove"):
                existing = _find_match(match_on)
                if existing is None:
                    action = "add"
            changes.append({"action": action, "new_rule": rc, "existing_rule": existing})

        return render_template(
            "fee_amendment_confirm.html",
            agreement=amendment_agreement,
            changes=changes,
            addendum_date=addendum_date,
            notes=notes,
            cache_token=cache_token,
            fee_types=FEE_TYPES,
            payment_methods=PAYMENT_METHODS,
            currencies=CURRENCIES,
            filename=f.filename,
        )

    # ── New agreement flow ─────────────────────────────────────────────────
    try:
        result = ai_parse.analyze_agreement(text, system_prompt=system_prompt)
    except Exception as e:
        return _render_error(f"AI analysis failed: {e}")

    return render_template(
        "fee_confirm.html",
        agreement=result.get("agreement", {}),
        fee_rules=result.get("fee_rules", []),
        entities=queries.get_entities(),
        fee_types=FEE_TYPES,
        payment_methods=PAYMENT_METHODS,
        currencies=CURRENCIES,
        filename=f.filename,
        cache_token=agr_cache_token,
    )


@app.route("/fees/upload/confirm", methods=["POST"])
@require_fees_auth
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

    # Save initial agreement file if a cache token was passed
    cache_token = request.form.get("cache_token", "").strip()
    if cache_token:
        fn, fd = queries.pop_upload_cache(cache_token)
        if fn and fd:
            queries.save_agreement_file(psp_id, fn, fd)

    return redirect(url_for("fees_detail", psp_id=psp_id))


@app.route("/fees/<int:psp_id>/amendment/confirm", methods=["POST"])
@require_fees_auth
def fees_amendment_confirm(psp_id):
    """Apply AI-extracted amendment changes and record them in amendment history."""
    if not queries.get_agreement(psp_id):
        abort(404)

    cache_token   = request.form.get("cache_token", "").strip()
    addendum_date = (request.form.get("addendum_date") or "").strip()
    notes         = (request.form.get("notes") or "").strip()
    change_count  = int(request.form.get("change_count", 0))

    # Retrieve file from cache
    filename, file_data = queries.pop_upload_cache(cache_token) if cache_token else (None, None)

    applied_changes = []  # list of (action, fee_rule_id, old_rule, new_rule)

    for i in range(change_count):
        if request.form.get(f"skip_{i}"):
            continue

        action = request.form.get(f"action_{i}", "add")

        if action == "remove":
            rule_id = request.form.get(f"existing_rule_id_{i}", type=int)
            old_rule = queries.get_fee_rule(rule_id) if rule_id else None
            if rule_id:
                queries.delete_fee_rule(rule_id)
            applied_changes.append((action, rule_id, old_rule, None))
            continue

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

        if action == "replace":
            rule_id = request.form.get(f"existing_rule_id_{i}", type=int)
            old_rule = queries.get_fee_rule(rule_id) if rule_id else None
            if rule_id:
                queries.update_fee_rule(rule_id, rule)
                result_id = rule_id
            else:
                result_id = queries.create_fee_rule(psp_id, rule)
            applied_changes.append((action, result_id, old_rule, rule))
        else:
            result_id = queries.create_fee_rule(psp_id, rule)
            applied_changes.append((action, result_id, None, rule))

    if addendum_date:
        queries.update_addendum_date(psp_id, addendum_date)

    # Record amendment in history
    amend_id = queries.create_amendment_record(
        psp_id, addendum_date or None, filename, file_data,
        notes or None, len(applied_changes)
    )
    for action, fee_rule_id, old_rule, new_rule in applied_changes:
        queries.add_amendment_change(amend_id, action, fee_rule_id, old_rule, new_rule)

    return redirect(url_for("fees_detail", psp_id=psp_id))


# ---------------------------------------------------------------------------
# Amendment History & File Downloads
# ---------------------------------------------------------------------------

@app.route("/fees/<int:psp_id>/amendments")
@require_fees_auth
def fees_amendments(psp_id):
    agreement  = queries.get_agreement(psp_id)
    if not agreement:
        abort(404)
    amendments = queries.get_amendments(psp_id)
    has_agr_file = bool(queries.get_agreement_file(psp_id)[0])
    return render_template("fee_amendment_history.html",
                           agreement=agreement, amendments=amendments,
                           has_agr_file=has_agr_file)


@app.route("/fees/<int:psp_id>/amendments/<int:amend_id>")
@require_fees_auth
def fees_amendment_detail(psp_id, amend_id):
    agreement = queries.get_agreement(psp_id)
    if not agreement:
        abort(404)
    amend = queries.get_amendment(amend_id)
    if not amend or amend["agreement_id"] != psp_id:
        abort(404)
    return render_template("fee_amendment_history.html",
                           agreement=agreement,
                           amendments=queries.get_amendments(psp_id),
                           selected=amend,
                           has_agr_file=bool(queries.get_agreement_file(psp_id)[0]))


@app.route("/fees/<int:psp_id>/download-agreement")
@require_fees_auth
def fees_download_agreement(psp_id):
    filename, file_data = queries.get_agreement_file(psp_id)
    if not file_data:
        abort(404)
    mime = "application/pdf" if (filename or "").lower().endswith(".pdf") else "application/octet-stream"
    return send_file(io.BytesIO(file_data), download_name=filename, mimetype=mime, as_attachment=True)


@app.route("/fees/amendments/<int:amend_id>/download")
@require_fees_auth
def fees_download_amendment(amend_id):
    filename, file_data = queries.get_amendment_file(amend_id)
    if not file_data:
        abort(404)
    mime = "application/pdf" if (filename or "").lower().endswith(".pdf") else "application/octet-stream"
    return send_file(io.BytesIO(file_data), download_name=filename, mimetype=mime, as_attachment=True)


# ---------------------------------------------------------------------------
# Prompt Template Management
# ---------------------------------------------------------------------------

@app.route("/fees/context-notes/save", methods=["POST"])
@require_fees_auth
def context_note_save():
    data      = request.get_json(force=True)
    label     = (data.get("label")     or "").strip()
    text      = (data.get("text")      or "").strip()
    note_type = (data.get("note_type") or "agreement").strip()
    if note_type not in ("agreement", "amendment"):
        note_type = "agreement"
    if not text:
        return jsonify({"error": "Text is required"}), 400
    label = label or "Additional Note"
    note_id = queries.save_context_note(label, text, note_type=note_type)
    return jsonify({"id": note_id, "label": label, "text": text,
                    "note_type": note_type, "use_count": 0, "usage_pct": 0})


@app.route("/fees/context-notes/<int:note_id>/delete", methods=["POST"])
@require_fees_auth
def context_note_delete(note_id):
    queries.delete_context_note(note_id)
    return redirect(url_for("fees_upload"))


@app.route("/fees/prompts")
@require_fees_auth
def prompt_list():
    templates = queries.get_prompt_templates()
    return render_template("prompt_list.html", templates=templates)


@app.route("/fees/prompts/new", methods=["GET", "POST"])
@require_fees_auth
def prompt_new():
    templates = queries.get_prompt_templates()
    if request.method == "POST":
        name   = request.form.get("name", "").strip()
        prompt = request.form.get("system_prompt", "").strip()
        if not name or not prompt:
            return render_template("prompt_form.html", template=None, templates=templates,
                                   error="Name and prompt text are required.")
        prompt_type = request.form.get("prompt_type", "agreement")
        tpl_id = queries.create_prompt_template(name, prompt, prompt_type=prompt_type)
        if request.form.get("set_default"):
            queries.set_default_prompt_template(tpl_id)
        return redirect(url_for("prompt_list"))
    base_id = request.args.get("base", type=int)
    return render_template("prompt_form.html", template=None, templates=templates,
                           base_id=base_id)


@app.route("/fees/prompts/<int:tpl_id>/edit", methods=["GET", "POST"])
@require_fees_auth
def prompt_edit(tpl_id):
    tpl = queries.get_prompt_template(tpl_id)
    if not tpl:
        abort(404)
    if tpl.get("is_builtin"):
        return redirect(url_for("prompt_list"))
    if request.method == "POST":
        name   = request.form.get("name", "").strip()
        prompt = request.form.get("system_prompt", "").strip()
        if not name or not prompt:
            return render_template("prompt_form.html", template=tpl,
                                   error="Name and prompt text are required.")
        prompt_type = request.form.get("prompt_type", "agreement")
        queries.update_prompt_template(tpl_id, name, prompt, prompt_type=prompt_type)
        if request.form.get("set_default"):
            queries.set_default_prompt_template(tpl_id)
        return redirect(url_for("prompt_list"))
    return render_template("prompt_form.html", template=tpl)


@app.route("/fees/prompts/<int:tpl_id>/content")
@require_fees_auth
def prompt_content(tpl_id):
    tpl = queries.get_prompt_template(tpl_id)
    if not tpl:
        abort(404)
    return jsonify({"system_prompt": tpl["system_prompt"]})


@app.route("/fees/prompts/<int:tpl_id>/set-default", methods=["POST"])
@require_fees_auth
def prompt_set_default(tpl_id):
    queries.set_default_prompt_template(tpl_id)
    return redirect(url_for("prompt_list"))


@app.route("/fees/prompts/<int:tpl_id>/delete", methods=["POST"])
@require_fees_auth
def prompt_delete(tpl_id):
    tpl = queries.get_prompt_template(tpl_id)
    if tpl and tpl.get("is_builtin"):
        abort(403)
    queries.delete_prompt_template(tpl_id)
    return redirect(url_for("prompt_list"))


if __name__ == "__main__":
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=5050, debug=debug)
