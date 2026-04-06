"""Simplified Flask dashboard — reads from the reconciliation database.

No file upload logic. No in-memory pandas reconciliation.
Just SQL queries against the DB and result display.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask, render_template, jsonify
from sqlalchemy import text

from db.engine import get_session
from db.models import ReconciliationRun, Reconciliation, CleanCRMTransaction, CleanPSPTransaction
from config import WEB_HOST, WEB_PORT

app = Flask(__name__)


@app.route("/")
def index():
    """Dashboard home page."""
    return render_template("index.html")


@app.route("/api/runs")
def api_runs():
    """List all reconciliation runs."""
    session = get_session()
    try:
        runs = session.query(ReconciliationRun).order_by(
            ReconciliationRun.run_at.desc()
        ).all()
        return jsonify([{
            "id": r.id,
            "report_month": r.report_month,
            "run_at": r.run_at.isoformat() if r.run_at else None,
            "match_rate": r.match_rate,
            "matched": r.matched,
            "unmatched_crm": r.unmatched_crm,
            "unmatched_psp": r.unmatched_psp,
            "unrecon_amount": r.unrecon_amount,
            "triggered_by": r.triggered_by,
        } for r in runs])
    finally:
        session.close()


@app.route("/api/runs/<int:run_id>/summary")
def api_run_summary(run_id):
    """Detailed summary for a specific reconciliation run."""
    session = get_session()
    try:
        run = session.query(ReconciliationRun).get(run_id)
        if not run:
            return jsonify({"error": "Run not found"}), 404

        # Count by match status
        status_counts = {}
        results = session.query(Reconciliation).filter_by(run_id=run_id).all()
        for r in results:
            status_counts[r.match_status] = status_counts.get(r.match_status, 0) + 1

        # PSP breakdown
        psp_stats = {}
        psp_matched = session.execute(text("""
            SELECT p.psp_name, COUNT(*) as cnt
            FROM reconciliation r
            JOIN clean_psp_transactions p ON r.psp_tx_id = p.id
            WHERE r.run_id = :run_id AND r.match_status = 'matched'
            GROUP BY p.psp_name
        """), {"run_id": run_id}).fetchall()
        for psp_name, cnt in psp_matched:
            psp_stats[psp_name] = {"matched": cnt}

        return jsonify({
            "run": {
                "id": run.id,
                "report_month": run.report_month,
                "run_at": run.run_at.isoformat() if run.run_at else None,
                "match_rate": run.match_rate,
                "matched": run.matched,
                "unmatched_crm": run.unmatched_crm,
                "unmatched_psp": run.unmatched_psp,
                "unrecon_amount": run.unrecon_amount,
            },
            "status_breakdown": status_counts,
            "psp_breakdown": psp_stats,
        })
    finally:
        session.close()


@app.route("/api/runs/<int:run_id>/unmatched")
def api_unmatched(run_id):
    """List unmatched CRM rows for a specific run."""
    session = get_session()
    try:
        rows = session.execute(text("""
            SELECT c.id, c.psp_transaction_id, c.transactionid,
                   c.amount, c.currency, c.payment_method, c.payment_processor,
                   c.transaction_type, c.login
            FROM reconciliation r
            JOIN clean_crm_transactions c ON r.crm_id = c.id
            WHERE r.run_id = :run_id AND r.match_status = 'unmatched_crm'
            ORDER BY c.amount DESC
            LIMIT 200
        """), {"run_id": run_id}).fetchall()

        return jsonify([{
            "id": r[0],
            "psp_transaction_id": r[1],
            "transactionid": r[2],
            "amount": r[3],
            "currency": r[4],
            "payment_method": r[5],
            "payment_processor": r[6],
            "transaction_type": r[7],
            "login": r[8],
        } for r in rows])
    finally:
        session.close()


@app.route("/api/db-stats")
def api_db_stats():
    """Overall database statistics."""
    session = get_session()
    try:
        crm_count = session.execute(text("SELECT COUNT(*) FROM clean_crm_transactions")).scalar()
        psp_count = session.execute(text("SELECT COUNT(*) FROM clean_psp_transactions")).scalar()
        bank_count = session.execute(text("SELECT COUNT(*) FROM clean_bank_transactions")).scalar()

        psp_by_name = session.execute(text("""
            SELECT psp_name, COUNT(*) FROM clean_psp_transactions GROUP BY psp_name ORDER BY COUNT(*) DESC
        """)).fetchall()

        bank_by_name = session.execute(text("""
            SELECT bank_name, COUNT(*) FROM clean_bank_transactions GROUP BY bank_name ORDER BY COUNT(*) DESC
        """)).fetchall()

        return jsonify({
            "crm_rows": crm_count,
            "psp_rows": psp_count,
            "bank_rows": bank_count,
            "psp_breakdown": {name: cnt for name, cnt in psp_by_name},
            "bank_breakdown": {name: cnt for name, cnt in bank_by_name},
        })
    finally:
        session.close()


if __name__ == "__main__":
    app.run(host=WEB_HOST, port=WEB_PORT, debug=True)
