"""CRM Transaction loader.

Loads the CRM Transactions Additional info.xlsx file, resolves currency_id → ISO,
normalizes join keys, and inserts into both raw and clean tables.
"""

import os
import json
from datetime import date

import pandas as pd

from db.engine import get_session
from db.models import RawCRMTransaction, CleanCRMTransaction
from transformers.normalize import normalize_key, resolve_currency, clean_amount


def _detect_column(df, candidates):
    """Find the first matching column name (case-insensitive)."""
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols_lower:
            return cols_lower[cand.lower()]
    return None


def load_crm(filepath: str, report_month: str = None):
    """Load CRM transactions from Excel into raw + clean tables.

    Args:
        filepath: Path to CRM Transactions Additional info.xlsx
        report_month: e.g. "2023-01" — defaults to extraction from filename/date

    Returns:
        Number of clean rows inserted.
    """
    df = pd.read_excel(filepath, dtype=str)

    # Detect key columns
    psp_id_col = _detect_column(df, ['psp_transaction_id', 'psp_id'])
    txn_id_col = _detect_column(df, ['transactionid', 'transaction_id'])
    amount_col = _detect_column(df, ['amount'])
    currency_col = _detect_column(df, ['currency_id', 'currency'])
    usd_col = _detect_column(df, ['usdamount', 'usd_amount'])
    pm_col = _detect_column(df, ['payment_method'])
    pp_col = _detect_column(df, ['payment_processor'])
    tt_col = _detect_column(df, ['transactiontype', 'transaction_type'])
    login_col = _detect_column(df, ['login'])
    deal_col = _detect_column(df, ['mtorder_id', 'deal_no', 'dealid', 'order_id'])
    fname_col = _detect_column(df, ['first_name', 'firstname'])
    lname_col = _detect_column(df, ['last_name', 'lastname'])
    comment_col = _detect_column(df, ['comment', 'comments'])

    # Detect date columns — CRM may store as Month/Day/Year separate columns
    month_col = _detect_column(df, ['Month of confirmation_time', 'month'])
    day_col = _detect_column(df, ['Day of confirmation_time', 'day'])
    year_col = _detect_column(df, ['Year of confirmation_time', 'year'])
    date_col = _detect_column(df, ['confirmation_time', 'date', 'created_at'])

    # Resolve currencies
    if currency_col:
        currencies = resolve_currency(df[currency_col])
    else:
        currencies = pd.Series([None] * len(df))

    # Normalize join keys
    psp_keys = normalize_key(df[psp_id_col]) if psp_id_col else pd.Series([None] * len(df))
    txn_keys = normalize_key(df[txn_id_col]) if txn_id_col else pd.Series([None] * len(df))

    session = get_session()
    source_file = os.path.basename(filepath)
    clean_count = 0

    try:
        for i, row in df.iterrows():
            # Raw layer — store entire row as JSON
            raw = RawCRMTransaction(
                source_file=source_file,
                row_number=i,
                raw_data=json.loads(row.to_json()),
            )
            session.add(raw)
            session.flush()  # get raw.id

            # Parse date
            txn_date = None
            if month_col and day_col and year_col:
                try:
                    m = int(float(row.get(month_col, 0) or 0))
                    d = int(float(row.get(day_col, 0) or 0))
                    y = int(float(row.get(year_col, 0) or 0))
                    if m and d and y:
                        txn_date = date(y, m, d)
                except (ValueError, TypeError):
                    pass
            elif date_col and pd.notna(row.get(date_col)):
                try:
                    txn_date = pd.to_datetime(row[date_col], errors='coerce')
                    if pd.notna(txn_date):
                        txn_date = txn_date.date()
                    else:
                        txn_date = None
                except Exception:
                    txn_date = None

            # Clean layer
            clean = CleanCRMTransaction(
                crm_transaction_id=row.get(txn_id_col) if txn_id_col else None,
                psp_transaction_id=psp_keys.iloc[i],
                transactionid=txn_keys.iloc[i],
                login=row.get(login_col) if login_col else None,
                amount=clean_amount(row.get(amount_col) if amount_col else None),
                currency=currencies.iloc[i],
                usd_amount=clean_amount(row.get(usd_col) if usd_col else None),
                payment_method=row.get(pm_col) if pm_col else None,
                payment_processor=row.get(pp_col) if pp_col else None,
                transaction_type=row.get(tt_col) if tt_col else None,
                date=txn_date,
                mtorder_id=row.get(deal_col) if deal_col else None,
                report_month=report_month,
                first_name=row.get(fname_col) if fname_col else None,
                last_name=row.get(lname_col) if lname_col else None,
                comment=row.get(comment_col) if comment_col else None,
                raw_id=raw.id,
            )
            session.add(clean)
            clean_count += 1

        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

    return clean_count
