"""CLI entry point for the reconciliation pipeline.

Usage (file-based — current):
    python scripts/run_pipeline.py --month 2023-01 --data-dir "../relevant-data/MRS/2023/01. Jan. 2023/"

Usage (Praxis API — once credentials are available):
    python scripts/run_pipeline.py --month 2023-01 --data-dir <crm-dir> --praxis

Steps:
    1. Init DB (create tables, seed registry)
    2. Auto-discover transformers
    3. Load CRM transactions
    4. Load PSP statements (file-based or via Praxis API)
    5. Load Bank statements (always file-based for now)
    6. Run SQL reconciliation
    7. Print summary
"""

import os
import sys
import argparse
from datetime import date, datetime

# Add parent dir to path so imports work when running from scripts/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.init_db import init_db
from db.engine import engine
from db.models import Base
from transformers.registry import registry
from loaders.crm_loader import load_crm
from loaders.file_loader import load_directory
from reconciliation.engine import run_reconciliation


def find_crm_file(data_dir: str) -> str | None:
    """Find the CRM transactions file in the data directory."""
    platform_dir = os.path.join(data_dir, "platform")
    if os.path.isdir(platform_dir):
        for fname in os.listdir(platform_dir):
            if "CRM" in fname and fname.endswith(('.xlsx', '.xls')):
                return os.path.join(platform_dir, fname)
            if "transaction" in fname.lower() and fname.endswith(('.xlsx', '.xls')):
                return os.path.join(platform_dir, fname)

    # Fallback: search top-level
    for fname in os.listdir(data_dir):
        if "CRM" in fname and fname.endswith(('.xlsx', '.xls')):
            return os.path.join(data_dir, fname)

    return None


def main():
    parser = argparse.ArgumentParser(description="Run the ETL reconciliation pipeline")
    parser.add_argument("--month", required=True, help="Report month (e.g. 2023-01)")
    parser.add_argument("--data-dir", required=True, help="Path to monthly data directory (must contain platform/ for CRM)")
    parser.add_argument("--fresh", action="store_true", help="Drop and recreate all tables")
    parser.add_argument("--praxis", action="store_true",
                        help="Fetch PSP data from Praxis API instead of local files "
                             "(requires PRAXIS_API_URL and PRAXIS_API_KEY env vars)")
    args = parser.parse_args()

    data_dir = os.path.abspath(args.data_dir)
    if not os.path.isdir(data_dir):
        print(f"ERROR: Data directory not found: {data_dir}")
        sys.exit(1)

    # ── Step 1: Init DB ─────────────────────────────────────────────────────
    print("=" * 60)
    print(f"Reconciliation Pipeline — {args.month}")
    print("=" * 60)

    if args.fresh:
        print("\n[1/5] Dropping and recreating database...")
        Base.metadata.drop_all(engine)
    else:
        print("\n[1/5] Initializing database...")

    init_db()
    print("  Database ready.")

    # ── Step 2: Auto-discover transformers ──────────────────────────────────
    print("\n[2/5] Discovering transformers...")
    registry.auto_discover()
    print(f"  PSP transformers: {', '.join(registry.psp_names)}")
    print(f"  Bank transformers: {', '.join(registry.bank_names)}")

    # ── Step 3: Load CRM ────────────────────────────────────────────────────
    print("\n[3/5] Loading CRM transactions...")
    crm_file = find_crm_file(data_dir)
    if crm_file:
        crm_count = load_crm(crm_file, report_month=args.month)
        print(f"  CRM: {crm_count} rows from {os.path.basename(crm_file)}")
    else:
        print("  WARNING: No CRM file found in platform/ directory")
        crm_count = 0

    # ── Step 4: Load PSP statements ─────────────────────────────────────────
    if args.praxis:
        print("\n[4/5] Fetching PSP statements via Praxis API...")
        from loaders.praxis_loader import load_all_gateways_via_praxis, PraxisAPIError
        try:
            # Derive date range from the report month (full calendar month)
            year, month = map(int, args.month.split("-"))
            import calendar
            last_day = calendar.monthrange(year, month)[1]
            date_from = date(year, month, 1)
            date_to = date(year, month, last_day)
            praxis_stats = load_all_gateways_via_praxis(date_from=date_from, date_to=date_to)
            total_praxis_rows = sum(praxis_stats.values())
            print(f"\n  Praxis summary: {len(praxis_stats)} PSPs, {total_praxis_rows} total rows")
        except (PraxisAPIError, NotImplementedError) as e:
            print(f"\n  ERROR: Praxis API unavailable — {e}")
            print("  Falling back to file-based loading...")
            args.praxis = False

    if not args.praxis:
        print("\n[4/5] Loading PSP and Bank statements from files...")
        stats = load_directory(data_dir)
        print(f"\n  Summary: {stats['psp_files']} PSP files ({stats['psp_rows']} rows), "
              f"{stats['bank_files']} bank files ({stats['bank_rows']} rows)")
        if stats["unrecognized"]:
            print(f"  Unrecognized files: {', '.join(stats['unrecognized'][:10])}")
            if len(stats["unrecognized"]) > 10:
                print(f"    ... and {len(stats['unrecognized']) - 10} more")

    # ── Step 5: Reconcile ───────────────────────────────────────────────────
    print("\n[5/5] Running reconciliation...")
    summary = run_reconciliation(report_month=args.month)

    print("\n" + "=" * 60)
    print("RECONCILIATION RESULTS")
    print("=" * 60)
    print(f"  Report Month:     {summary['report_month']}")
    print(f"  Total CRM:        {summary['total_crm']}")
    print(f"  Total PSP:        {summary['total_psp']}")
    print(f"  Total Bank:       {summary['total_bank']}")
    print(f"  ────────────────────────────")
    print(f"  Matched:          {summary['matched']} ({summary['match_rate']}%)")
    print(f"    via psp_id:     {summary['matched_via_psp_id']}")
    print(f"    via txn_id:     {summary['matched_via_txn_id']}")
    print(f"    via bank:       {summary['matched_via_bank']}")
    print(f"  Unmatched CRM:    {summary['unmatched_crm']}")
    print(f"  Unmatched PSP:    {summary['unmatched_psp']}")
    print(f"  Unrecon Amount:   ${summary['unrecon_amount']:,.2f}")
    print("=" * 60)


if __name__ == "__main__":
    main()
