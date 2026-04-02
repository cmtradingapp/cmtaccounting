"""File loader — scans directories and routes files to the correct transformer.

Handles both PSP and bank statement files. Inserts into raw + clean DB tables.
"""

import os
import json

import pandas as pd

from db.engine import get_session
from db.models import (
    RawPSPTransaction, RawBankTransaction,
    CleanPSPTransaction, CleanBankTransaction, PSPSchemaRegistry,
)
from transformers.registry import registry
from transformers.base import PSPTransformer, BankTransformer
from transformers.psp.generic import GenericPSPTransformer


def _load_generic_transformers_from_db():
    """Load PSP transformers from the schema registry for PSPs without custom code."""
    session = get_session()
    try:
        rows = session.query(PSPSchemaRegistry).all()
        for row in rows:
            # Skip if a custom transformer is already registered for this PSP
            if registry.get_psp(row.psp_name) is not None:
                continue
            transformer = GenericPSPTransformer.from_registry_row(row)
            registry.register_psp(transformer)
    finally:
        session.close()


def load_psp_file(filepath: str, transformer: PSPTransformer) -> int:
    """Load a single PSP file using the given transformer. Returns row count."""
    clean_df = transformer.load(filepath)
    if clean_df is None or clean_df.empty:
        return 0

    session = get_session()
    source_file = os.path.basename(filepath)
    count = 0

    try:
        for i, row in clean_df.iterrows():
            # Raw layer
            raw = RawPSPTransaction(
                psp_name=transformer.psp_name,
                source_file=source_file,
                row_number=i,
                raw_data=json.loads(row.to_json(default_handler=str)),
            )
            session.add(raw)
            session.flush()

            # Clean layer
            ref_id = row.get("reference_id")
            if ref_id is None or pd.isna(ref_id):
                continue

            clean = CleanPSPTransaction(
                psp_name=transformer.psp_name,
                reference_id=str(ref_id),
                amount=row.get("amount"),
                currency=row.get("currency"),
                fee=row.get("fee"),
                status=row.get("status"),
                date=row.get("date") if pd.notna(row.get("date")) else None,
                source_file=source_file,
                raw_id=raw.id,
            )
            session.add(clean)
            count += 1

        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    return count


def load_bank_file(filepath: str, transformer: BankTransformer) -> int:
    """Load a single bank statement file using the given transformer. Returns row count."""
    clean_df = transformer.load(filepath)
    if clean_df is None or clean_df.empty:
        return 0

    session = get_session()
    source_file = os.path.basename(filepath)
    count = 0

    try:
        for i, row in clean_df.iterrows():
            raw = RawBankTransaction(
                bank_name=transformer.bank_name,
                source_file=source_file,
                row_number=i,
                raw_data=json.loads(row.to_json(default_handler=str)),
            )
            session.add(raw)
            session.flush()

            clean = CleanBankTransaction(
                bank_name=transformer.bank_name,
                reference=row.get("reference"),
                client_id=str(row["client_id"]) if pd.notna(row.get("client_id")) else None,
                amount=row.get("amount"),
                currency=row.get("currency"),
                date=row.get("date") if pd.notna(row.get("date")) else None,
                description=row.get("description"),
                source_file=source_file,
                raw_id=raw.id,
            )
            session.add(clean)
            count += 1

        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    return count


def load_directory(data_dir: str) -> dict:
    """Scan a data directory and load all recognized PSP and bank files.

    Expected structure:
        data_dir/
            PSPs/          # PSP statement files
            Banks/         # Bank statement files
            platform/      # CRM file (handled separately by crm_loader)

    Returns dict with load stats.
    """
    # Ensure generic transformers from DB registry are available
    _load_generic_transformers_from_db()

    stats = {"psp_files": 0, "psp_rows": 0, "bank_files": 0, "bank_rows": 0,
             "unrecognized": []}

    # Load PSP files
    psp_dir = os.path.join(data_dir, "PSPs")
    if os.path.isdir(psp_dir):
        for fname in sorted(os.listdir(psp_dir)):
            fpath = os.path.join(psp_dir, fname)
            if not os.path.isfile(fpath):
                continue

            transformer = registry.get_psp_for_file(fname)
            if transformer:
                rows = load_psp_file(fpath, transformer)
                stats["psp_files"] += 1
                stats["psp_rows"] += rows
                print(f"  PSP [{transformer.psp_name}] {fname}: {rows} rows")
            else:
                stats["unrecognized"].append(fname)

    # Load bank files
    bank_dir = os.path.join(data_dir, "Banks")
    if not os.path.isdir(bank_dir):
        # Some data layouts use flat directory with banks mixed in
        bank_dir = data_dir

    if os.path.isdir(bank_dir):
        for fname in sorted(os.listdir(bank_dir)):
            fpath = os.path.join(bank_dir, fname)
            if not os.path.isfile(fpath):
                continue

            transformer = registry.get_bank_for_file(fname)
            if transformer:
                rows = load_bank_file(fpath, transformer)
                stats["bank_files"] += 1
                stats["bank_rows"] += rows
                print(f"  Bank [{transformer.bank_name}] {fname}: {rows} rows")

    return stats
