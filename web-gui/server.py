import os
import csv
import re
import io
import json
import pickle
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_file
import requests as http_requests
import pandas as pd

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

STATE_FILE = os.path.join(app.config['UPLOAD_FOLDER'], '_recon_state.pkl')

# 52-column spec used by List.xlsx ("Our" tab) — kept for test compatibility
OUTPUT_COLUMNS = [
    'Tran.Date', 'Reference', 'Deal No', 'Amount', 'Commission', 'Total',
    'Currency', 'AmntBC', 'CommissionBC', 'TotalBC', 'Reason Code',
    'Payment Method', 'Bank', 'Institution', 'Details1', 'Details2',
    'Comment', 'Remarks', 'Exch.Diff. %', 'Asked Initial Amount',
    'Asked Commission', 'Asked Amount', 'Asked Currency', 'Withdraw ID',
    'Withdraw Acc', 'Withdraw DealNo', 'Take To Profit', 'Unrecon. Fees',
    'AgeGroup', 'Match No', 'Recon.Reason Group', 'Recon Reason',
    'IsTiming', 'MatchCount', 'EOD', 'IsEODTran', 'Type', 'ClientCode',
    'ClientAccount', 'ReasonCodeD', 'CategoryCode', 'Reason Code Group',
    'MRS Notes', 'Country', 'IsSeggr', 'PlatformName', 'Country.1',
    'ClientType', 'ClientGroup', 'Matched By', 'ReasonCodeGroupName', 'Matched On'
]

# 44-column spec matching MT4-Transactions tab in Life Cycle Report-final.xlsx
MT4_TRX_COLUMNS = [
    'Tran.Date', 'Reference', 'Deal No', 'Amount', 'Commission', 'Total',
    'Currency', 'AmntBC', 'CommissionBC', 'TotalBC', 'Reason Code',
    'Payment Method', 'Bank', 'Institution', 'Details1', 'Details2',
    'Comment', 'Remarks', 'Exch.Diff. %', 'Take To Profit', 'Unrecon. Fees',
    'Match No', 'Recon.Reason Group', 'Recon Reason', 'IsTiming', 'MatchCount',
    'EOD', 'IsEODTran', 'ClientAccount', 'ReasonCodeD', 'CategoryCode',
    'Reason Code Group', 'MRS Notes', 'Country', 'IsSeggr', 'PlatformName',
    'Country_1', 'ClientType', 'ClientGroup', 'Matched By', 'ReasonCodeGroupName',
    'Matched On', 'Index', 'TRX Type'
]

# 31-column spec matching PM-Transactions tab
PM_TRX_COLUMNS = [
    'Index', 'Tran.Date', 'Reference', 'Amount', 'Currency', 'AmntBC',
    'Payment Method', 'Bank', 'Details1', 'Details2', 'Comment', 'Remarks',
    'ExcRate', 'Exch.Diff. %', 'ToEmail', 'TranStatus', 'Reference2',
    'Take To Profit', 'ReasonCodeD', 'MRS Notes', 'Recon.Reason Group',
    'Recon Reason', 'Match No', 'ReasonCodeGroupName', 'TRX Type',
    'PM Name', 'PM-Cur', 'Is Balance Currency', 'Balance Currency',
    'Amount in Bal Curr', 'Amount USD'
]


# ── Currency FK lookup ───────────────────────────────────────────────────────
# Maps CRM currency_id integer → ISO 4217 currency code.
# Verified via join of CRM mtorder_id against reference MT4-Transactions.
CURRENCY_ID_MAP = {
    1:   'USD',
    2:   'EUR',
    3:   'GBP',
    4:   'AED',
    5:   'ZAR',
    6:   'NGN',
    7:   'IDR',
    8:   'MXN',
    9:   'COP',
    10:  'TZS',
    11:  'USDT',
    12:  'KES',
    13:  'UGX',
    14:  'GHS',
    15:  'ZMW',
    16:  'BWP',
    17:  'PHP',
    18:  'MYR',
    19:  'VND',
}


def _resolve_currency(series):
    """Convert CRM currency_id integers to ISO currency codes."""
    def _conv(v):
        if pd.isna(v):
            return None
        try:
            return CURRENCY_ID_MAP.get(int(float(v)), str(v))
        except (ValueError, TypeError):
            return str(v)
    return series.apply(_conv)


# ── TRX Type mapping ────────────────────────────────────────────────────────
# Maps (CRM payment_method, transactiontype) → MT4 TRX Type attribute.
# Derived by joining CRM mtorder_id against reference MT4-Transactions (3,548 rows).
_PM_TEXT_TO_TRX_TYPE = {
    # ── fixed-attribute non-PSP methods ─────────────────────────────────────
    'transfer':         '4. Transfer',     # TRF entries
    'internal transfer':'4. Transfer',
    'bonus':            '5. Bonuses',      # BN entries
    'frf commission':   '5. Bonuses',      # ADJ, maps to Bonuses per reference
    'processing fees':  '5. Fees/Charges', # PRF entries
    'commission':       '5. Realised Commissions',
    'ib commission':    '5. IB Payment',
    'adjustment':       '4. Transfer',     # ADJ entries (most common sub-type)
    'fee compensation': '5. Fee Compensation',
    'chargeback':       '5. Fee Compensation',
}
# Payment method text values that represent PSP flows (→ 2. DP / 2. WD based on transactiontype)
_PSP_PAYMENT_METHODS = {
    'credit card',        # Maps to individual PSP via payment_processor
    'electronic payment', # Maps to individual PSP via payment_processor
    'cryptowallet',       # FRX
    'cash',
    'wire transfer',      # BT (Banks) — verified as PSP flow in reference
}


# ── Non-PSP method → PM code (fixed mapping) ────────────────────────────────
_PM_TEXT_TO_CODE = {
    'transfer':         'TRF',
    'internal transfer':'TRF',
    'processing fees':  'PRF',
    'commission':       'PRF',
    'frf commission':   'ADJ',
    'ib commission':    'ADJ',
    'bonus':            'BN',
    'adjustment':       'ADJ',
    'fee compensation': 'ADJ',
    'chargeback':       'ADJ',
    'cash':             'ADJ',
    'wire transfer':    'BT',   # Wire transfers go through Banks (BT)
}


def _map_trx_type(payment_method, transactiontype, is_timing=False):
    """Derive the MT4 TRX Type attribute from CRM payment_method and transactiontype."""
    pm = str(payment_method).strip().lower() if payment_method else ''
    tt = str(transactiontype).strip().lower() if transactiontype else ''

    # Timing rows override the base category
    if is_timing:
        return '3. Timing Deposit' if 'deposit' in tt else '3. Timing Withdrawal'

    # Fixed-attribute methods
    if pm in _PM_TEXT_TO_TRX_TYPE:
        return _PM_TEXT_TO_TRX_TYPE[pm]

    # PSP flow methods — use deposit/withdrawal direction
    if pm in _PSP_PAYMENT_METHODS or not pm:
        if 'deposit' in tt:
            return '2. DP'
        if 'withdraw' in tt:
            return '2. WD'

    # Fallback: use transaction type direction if available
    if 'deposit' in tt:
        return '2. DP'
    if 'withdraw' in tt:
        return '2. WD'
    return '4. Transfer'


# ── PM Code mapping ──────────────────────────────────────────────────────────
# Maps lowercased/stripped CRM payment_processor strings → 2-letter PM code.
_PROCESSOR_TO_PM_CODE = {
    'zotapaymg':             'ZP',
    'zotapay':               'ZP',
    'safecharges2s3dv2':     'SC',
    'safecharges2s3dv2_ver2':'SC',
    'safecharge':            'SC',
    'safecharges2s':         'SC',
    'korapayapm':            'KP',
    'korapayhpp':            'KP',
    'korapay':               'KP',
    'solidpayments3dsv2':    'SLP',
    'solidpayments':         'SLP',
    'finrax':                'FRX',
    'ozow':                  'OZ',
    'eftpay':                'EFT',
    'virtualpays2s':         'VP',
    'virtualpay':            'VP',
    'skrill':                'SKR',
    'neteller':              'NT',
    'directa24rest':         'DRC',
    'directa24':             'DRC',
    'inatec':                'INA',
    'powercash':             'INA',
    'swiffyeft':             'SW',
    'swiffy':                'SW',
    'astropay':              'ASP',
    'letknow':               'LKP',
    'letknowpay':            'LKP',
    'trustpayments':         'TP',
    'nuvei':                 'SC',
}


def _map_pm_code(payment_processor):
    """Map CRM payment_processor string to 2-letter MT4 Payment Method code."""
    if not payment_processor or str(payment_processor).lower() in ('nan', 'none', ''):
        return None
    key = re.sub(r'[^a-z0-9]', '', str(payment_processor).lower())
    return _PROCESSOR_TO_PM_CODE.get(key)


def extract_headers(filepath):
    """Extract column headers from CSV or Excel files (handles messy legacy formats)."""
    ext = os.path.splitext(filepath)[1].lower()
    try:
        if ext in ('.xlsx', '.xls'):
            df = pd.read_excel(filepath, nrows=0)
        else:
            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                sample = f.read(4096)
                try:
                    dialect = csv.Sniffer().sniff(sample)
                    df = pd.read_csv(filepath, dialect=dialect, nrows=0)
                except csv.Error:
                    df = pd.read_csv(filepath, nrows=0)
        cols = [str(c).strip() for c in df.columns if not str(c).startswith('Unnamed')]
        return cols
    except Exception as e:
        return [f"(parse error: {str(e)[:60]})"]


def normalize_key(series):
    """Sanitize join keys for robust matching.

    Handles: trailing whitespace, float .0 suffixes (39120162.0 -> 39120162),
    leading zeros (0012345 -> 12345), case differences, and nulls.
    Null-like values become NA so they never accidentally match each other.
    """
    s = series.astype(str).str.strip()
    s = s.str.replace(r'\.0+$', '', regex=True)
    s = s.str.replace(r'^0+(\d)', r'\1', regex=True)
    s = s.str.upper()
    s = s.replace({'NAN': None, 'NONE': None, 'NAT': None, '': None})
    return s


def _get_merged_col(merged, *candidates):
    """Return the first found column from merged df, preferring _crm suffix."""
    for name in candidates:
        for variant in [f"{name}_crm", name, f"{name}_bank"]:
            if variant in merged.columns:
                return merged[variant]
    return pd.Series([None] * len(merged), index=merged.index)


def _detect_bank_ref_col(df):
    """Detect the transaction reference column for a single PSP file.

    Uses a priority-ordered list derived from all known PSP schemas.
    Returns None if no plausible reference column is found.
    """
    # Normalize each column name for matching: lowercase, strip all spaces/punctuation
    def _norm(s):
        return re.sub(r'[^a-z0-9]', '', s.lower())

    normed = {col: _norm(col) for col in df.columns}

    # Columns whose normalized name starts with 'external' are counterparty IDs
    # (e.g. Zotapay's external_transaction_id = the bank/card network's own ref,
    # not the Zotapay order ID stored in CRM). Skip them in all priority levels.
    def _is_external(n):
        return n.startswith('external')

    # Priority 1 — exact normalized matches (most reliable)
    exact = [
        'transactionreference',   # TrustPayments
        'merchantreference',      # EFTpay, Swiffy
        'transactionid',          # Finrax, Solidpayments
        'txid',                   # generic
        'referenceno',            # Finrax all.xlsx
        'paymentreference',       # Korapay pay-ins — must come before settlementreference
        'settlementreference',    # Korapay settlements (batch-level, lower priority)
        'refno',                  # Solidpayment fees
        'refid',                  # VP Refunds
        'transactionnumber',      # VP Deposits
    ]
    for keyword in exact:
        for col, n in normed.items():
            if n == keyword and not _is_external(n):
                return col

    # Priority 2 — contains a specific reference substring
    contains = [
        'transactionreference',
        'transactionref',
        'merchantreference',
        'transactionid',
        'settlementreference',
        'paymentreference',
        'referenceno',
        'txnid',
        'transactionnumber',
    ]
    for keyword in contains:
        for col, n in normed.items():
            if keyword in n and not _is_external(n):
                return col

    # Priority 3 — generic 'reference' substring (last resort before bare id)
    for col, n in normed.items():
        if 'reference' in n and not _is_external(n):
            return col

    # Priority 4 — bare 'id' column (Neteller, Skrill, Ozow, Zotapay)
    for col in df.columns:
        if col.strip().lower() in ('id', 'transaction details'):
            return col

    return None


def _detect_bank_ref_cols(df):
    """Return ALL plausible reference columns for a PSP file, in priority order.

    Used by the per-PSP loop to try multiple candidates and pick the one with
    the best overlap against CRM keys (handles cases like SolidPayments where
    the priority-1 column has low overlap but a lower-priority one has high overlap).
    """
    def _norm(s):
        return re.sub(r'[^a-z0-9]', '', s.lower())

    normed = {col: _norm(col) for col in df.columns}

    def _is_external(n):
        return n.startswith('external')

    seen = []

    def _add(col):
        if col and col not in seen:
            seen.append(col)

    exact = [
        'transactionreference', 'merchantreference', 'transactionid', 'txid',
        'referenceno', 'paymentreference', 'settlementreference',
        'uniqueid', 'refno', 'refid', 'transactionnumber',
    ]
    for keyword in exact:
        for col, n in normed.items():
            if n == keyword and not _is_external(n):
                _add(col)

    contains = [
        'transactionreference', 'transactionref', 'merchantreference',
        'transactionid', 'uniqueid', 'settlementreference', 'paymentreference',
        'referenceno', 'txnid', 'transactionnumber',
    ]
    for keyword in contains:
        for col, n in normed.items():
            if keyword in n and not _is_external(n):
                _add(col)

    for col, n in normed.items():
        if 'reference' in n and not _is_external(n):
            _add(col)

    for col in df.columns:
        if col.strip().lower() in ('id', 'transaction details'):
            _add(col)

    return seen


def _detect_bank_amount_col(df):
    """Detect the amount column for a single PSP file."""
    priority = ['baseamount', 'settlebaseamount', 'settledamount', 'netamount',
                'amount', 'gross', 'net', 'total']
    normed = {col: re.sub(r'[^a-z0-9]', '', col.lower()) for col in df.columns}
    for keyword in priority:
        for col, n in normed.items():
            if keyword in n:
                return col
    return None


def _load_psp_file(path):
    """Load a single PSP CSV or Excel file, returning a DataFrame or None on failure."""
    ext = os.path.splitext(path)[1].lower()

    # Keywords that indicate a row is a real column header (not metadata)
    _HEADER_KEYWORDS = {'date', 'amount', 'id', 'reference', 'transaction',
                        'currency', 'type', 'status', 'payment', 'method',
                        'name', 'result', 'email', 'order', 'fee'}

    def _looks_like_header(row_values):
        """True if 3+ values contain header keywords (suggesting a column-name row)."""
        hits = sum(1 for v in row_values
                   if any(k in str(v).lower() for k in _HEADER_KEYWORDS))
        return hits >= 3

    def _has_metadata_header(df):
        """True if the loaded DataFrame looks like it used a metadata row as header."""
        unnamed = sum(1 for c in df.columns if str(c).startswith('Unnamed'))
        first_col = str(df.columns[0]) if len(df.columns) else ''
        return (unnamed > len(df.columns) * 0.5
                or any(k in first_col.lower() for k in ('report', 'cpanel', 'generated')))

    try:
        if ext in ('.xlsx', '.xls'):
            # dtype=str preserves large integer IDs (e.g. 19-digit SafeCharge IDs)
            # that float64 would round, corrupting join keys.
            df = pd.read_excel(path, dtype=str)
            if _has_metadata_header(df):
                # Scan first 20 rows for the real header row
                raw = pd.read_excel(path, header=None, dtype=str, nrows=20)
                for i in range(len(raw)):
                    vals = [v for v in raw.iloc[i].tolist() if pd.notna(v)]
                    if _looks_like_header(vals):
                        df = pd.read_excel(path, skiprows=i, dtype=str)
                        break
        else:
            try:
                with open(path, 'r', encoding='utf-8', errors='replace') as f:
                    dialect = csv.Sniffer().sniff(f.read(4096))
                df = pd.read_csv(path, dialect=dialect, encoding='utf-8',
                                 encoding_errors='replace', dtype=str)
            except Exception:
                df = pd.read_csv(path, encoding='utf-8', encoding_errors='replace',
                                 dtype=str)
        # Skip files with no usable columns
        real_cols = [c for c in df.columns if not str(c).startswith('Unnamed')
                     and not str(c).startswith('CPanel')]
        return df if real_cols else None
    except Exception:
        return None


def build_lifecycle_df(merged):
    """Build the 52-column Lifecycle List from the outer-joined merged DataFrame."""
    g = lambda *names: _get_merged_col(merged, *names)
    out = pd.DataFrame(index=merged.index)

    # Parse CRM date strings ("January 1, 2023") → proper datetime
    raw_date = g('Month, Day, Year of confirmation_time', 'Month, Day, Year of created_time')
    out['Tran.Date'] = pd.to_datetime(raw_date, errors='coerce')
    out['Reference'] = g('psp_transaction_id', 'receipt', 'transactionreference')
    # Deal No: integer → string (no float .0 suffix); UUIDs from MT4 P&L not available
    deal_no_raw = g('mtorder_id')
    out['Deal No'] = deal_no_raw.apply(
        lambda v: str(int(v)) if pd.notna(v) and str(v) not in ('nan', 'None', '') else None
    )
    # CRM stores all amounts as positive; apply sign (withdrawals are negative)
    _sign = g('transactiontype').fillna('').str.lower().apply(
        lambda t: -1 if 'withdraw' in t else 1
    )
    out['Amount'] = pd.to_numeric(g('amount'), errors='coerce') * _sign
    out['Commission'] = 0
    out['Total'] = out['Amount']
    out['Currency'] = _resolve_currency(g('currency_id'))
    out['AmntBC'] = pd.to_numeric(g('usdamount'), errors='coerce') * _sign
    out['CommissionBC'] = 0
    out['TotalBC'] = out['AmntBC']
    out['Reason Code'] = None
    out['Payment Method'] = g('payment_method')
    out['Bank'] = g('bank_name')
    out['Institution'] = g('payment_processor')
    out['Details1'] = g('mtorder_id').astype(str).replace('None', None).replace('nan', None)
    out['Details2'] = None
    out['Comment'] = g('comment')
    fname = g('first_name').fillna('').astype(str).str.strip()
    lname = g('last_name').fillna('').astype(str).str.strip()
    out['Remarks'] = (fname + ' ' + lname).str.strip().replace('', None)
    out['Exch.Diff. %'] = None
    out['Asked Initial Amount'] = out['Amount']
    out['Asked Commission'] = 0
    out['Asked Amount'] = out['Amount']
    out['Asked Currency'] = out['Currency']
    out['Withdraw ID'] = None
    out['Withdraw Acc'] = g('ewalletid', 'bank_acccount_number')
    out['Withdraw DealNo'] = None
    out['Take To Profit'] = False

    unrecon = pd.Series([None] * len(merged), dtype=object, index=merged.index)
    if '_merge' in merged.columns:
        both_mask = merged['_merge'] == 'both'
        bank_amt_col = next((c for c in merged.columns if 'amount' in c.lower() and '_bank' in c), None)
        crm_amt_col = next((c for c in merged.columns if 'amount' in c.lower() and '_crm' in c), None)
        if bank_amt_col and crm_amt_col and both_mask.any():
            b = pd.to_numeric(merged.loc[both_mask, bank_amt_col], errors='coerce')
            c = pd.to_numeric(merged.loc[both_mask, crm_amt_col], errors='coerce')
            diff = (b - c).abs()
            unrecon[both_mask] = diff.where(diff > 0.001, None)
    out['Unrecon. Fees'] = unrecon

    out['AgeGroup'] = None

    if '_merge' in merged.columns:
        is_matched = merged['_merge'] == 'both'
        match_nos = pd.Series([None] * len(merged), dtype=object, index=merged.index)
        for seq, idx in enumerate(merged.index[is_matched], start=1):
            match_nos[idx] = seq
        out['Match No'] = match_nos
        out['Recon.Reason Group'] = merged['_merge'].map({
            'both': 'Matched', 'left_only': 'Unmatched', 'right_only': 'Bank Only'
        })
        out['Recon Reason'] = merged['_merge'].map({
            'both': 'Ref/Ref-Curr-Amnt>nn', 'left_only': 'No Match', 'right_only': 'No CRM Entry'
        })
        # IsTiming = True only for actual cross-period timing rows.
        # We don't have timing-match logic yet, so default to False.
        # (Reference has only 3 timing rows out of 10,255 — they are rare.)
        out['IsTiming'] = False
        out['MatchCount'] = is_matched.astype(int)
        out['Matched By'] = merged['_merge'].map({'both': 'MRS', 'left_only': None, 'right_only': None})
        now = datetime.now()
        out['Matched On'] = merged['_merge'].apply(lambda x: now if x == 'both' else None)
    else:
        out['Match No'] = None
        out['Recon.Reason Group'] = None
        out['Recon Reason'] = None
        out['IsTiming'] = False  # no timing match logic yet
        out['MatchCount'] = 0
        out['Matched By'] = None
        out['Matched On'] = None

    out['EOD'] = None
    out['IsEODTran'] = None
    out['Type'] = g('transactiontype')
    out['ClientCode'] = g('vtigeraccountid')
    # login is the MT4 account ID — matches ClientAccount in the reference output
    out['ClientAccount'] = g('login', 'tradingaccountsid')
    out['ReasonCodeD'] = None
    trans_type = g('transactiontype').fillna('').astype(str).str.lower()
    out['CategoryCode'] = trans_type.apply(
        lambda x: 'Deposit' if 'deposit' in x else ('Withdrawal' if 'withdraw' in x else (x.title() or None))
    )
    out['Reason Code Group'] = None
    out['MRS Notes'] = None
    out['Country'] = None
    out['IsSeggr'] = None
    out['PlatformName'] = None
    out['Country.1'] = None
    out['ClientType'] = None
    out['ClientGroup'] = None
    out['ReasonCodeGroupName'] = None

    return out[[c for c in OUTPUT_COLUMNS if c in out.columns]]


def build_balances_df(merged):
    """Build a Currency/Equity summary from the reconciled data."""
    g = lambda *names: _get_merged_col(merged, *names)

    currency = _resolve_currency(g('currency_id')).fillna('USD')
    amount = pd.to_numeric(g('amount'), errors='coerce').fillna(0)
    usd_amount = pd.to_numeric(g('usdamount'), errors='coerce').fillna(0)

    df_src = pd.DataFrame({'Currency': currency, 'Amount': amount, 'USD': usd_amount})
    summary = df_src.groupby('Currency').agg(
        Equity=('Amount', 'sum'),
        USD_Total=('USD', 'sum')
    ).reset_index()

    eur_per_usd = 0.92
    try:
        r = http_requests.get('https://api.frankfurter.app/latest?from=USD&to=EUR', timeout=3)
        eur_per_usd = r.json().get('rates', {}).get('EUR', 0.92)
    except Exception:
        pass

    summary['Equity EUR'] = (summary['USD_Total'] * eur_per_usd).round(2)
    summary['Equity USD'] = summary['USD_Total'].round(2)
    total_usd = summary['USD_Total'].abs().sum()
    summary['Perc'] = ((summary['USD_Total'].abs() / total_usd * 100) if total_usd > 0 else 0).round(2)
    summary['Equity'] = summary['Equity'].round(2)

    return summary[['Currency', 'Equity', 'Equity EUR', 'Equity USD', 'Perc']]


def build_mt4_transactions_df(merged):
    """Build the MT4-Transactions tab (44 cols) matching Life Cycle Report-final.xlsx.

    Only CRM-side rows are included (left_only + both). Bank-only rows belong in
    PM-Transactions, not here.
    """
    # Filter to CRM-side rows only
    if '_merge' in merged.columns:
        crm_merged = merged[merged['_merge'] != 'right_only'].copy()
    else:
        crm_merged = merged.copy()

    lc = build_lifecycle_df(crm_merged)
    out = pd.DataFrame(index=lc.index)

    shared = [
        'Tran.Date', 'Reference', 'Deal No', 'Amount', 'Commission', 'Total',
        'Currency', 'AmntBC', 'CommissionBC', 'TotalBC', 'Reason Code',
        'Bank', 'Institution', 'Details1', 'Details2',
        'Comment', 'Remarks', 'Exch.Diff. %', 'Take To Profit', 'Unrecon. Fees',
        'Match No', 'Recon.Reason Group', 'Recon Reason', 'IsTiming', 'MatchCount',
        'EOD', 'IsEODTran', 'ClientAccount', 'ReasonCodeD', 'CategoryCode',
        'Reason Code Group', 'MRS Notes', 'Country', 'IsSeggr', 'PlatformName',
        'ClientType', 'ClientGroup', 'Matched By', 'ReasonCodeGroupName', 'Matched On'
    ]
    for col in shared:
        out[col] = lc[col] if col in lc.columns else None

    out['Country_1'] = lc['Country.1'] if 'Country.1' in lc.columns else None
    out['Index'] = range(1, len(out) + 1)

    # Payment Method: 2-letter PM code
    # 1. Try payment_processor → PM code (for PSP entries)
    proc = _get_merged_col(crm_merged, 'payment_processor').fillna('')
    pm_from_proc = proc.apply(_map_pm_code)

    # 2. Fall back to payment_method text → fixed PM code (for non-PSP entries)
    pay_method_text = _get_merged_col(crm_merged, 'payment_method').fillna('').str.lower().str.strip()
    pm_from_text = pay_method_text.map(_PM_TEXT_TO_CODE)

    # Prefer processor-derived code; then text-derived; then None
    out['Payment Method'] = pm_from_proc.where(pm_from_proc.notna(), pm_from_text)

    # TRX Type
    trx_type_raw = _get_merged_col(crm_merged, 'transactiontype').fillna('')
    is_timing = lc['IsTiming'].fillna(False) if 'IsTiming' in lc.columns else pd.Series([False]*len(lc), index=lc.index)
    out['TRX Type'] = [
        _map_trx_type(pm, tt, it)
        for pm, tt, it in zip(pay_method_text, trx_type_raw, is_timing)
    ]

    return out[MT4_TRX_COLUMNS]


def _psp_source_to_pm_name(psp_source_series):
    """Map _psp_source filenames (e.g. 'bankFile_3.csv') to real PSP names
    using the Mapping Rules table from the reference file.
    Falls back to the raw filename if no match found.
    """
    mapping_rules = _load_mapping_rules()
    # Build a code → name lookup
    if not mapping_rules.empty and 'PM Code' in mapping_rules.columns and 'PM Name' in mapping_rules.columns:
        code_to_name = mapping_rules.drop_duplicates('PM Code').set_index('PM Code')['PM Name'].to_dict()
    else:
        code_to_name = {}

    def _convert(src):
        if pd.isna(src):
            return None
        # Strip upload prefix and extension to get a filename-based PM code guess
        clean = str(src).replace('bankFile_', '').rsplit('.', 1)[0].strip()
        # Try direct PM code lookup
        code = _map_pm_code(clean)
        if code and code in code_to_name:
            return code_to_name[code]
        # Try matching the filename against known processor names
        clean_lower = re.sub(r'[^a-z0-9]', '', clean.lower())
        for proc, pm_code in _PROCESSOR_TO_PM_CODE.items():
            if proc in clean_lower or clean_lower in proc:
                return code_to_name.get(pm_code, pm_code)
        return src  # fallback to raw filename

    return psp_source_series.apply(_convert)


def build_pm_transactions_df(merged):
    """Build the PM-Transactions tab (31 cols) from PSP-side rows only.

    Only bank-side rows are included (right_only = bank-only, both = matched pair).
    CRM-only rows (left_only) belong in MT4-Transactions, not here.
    """
    # Filter to PSP-side rows only
    if '_merge' in merged.columns:
        psp_merged = merged[merged['_merge'].isin(['right_only', 'both'])].copy()
    else:
        psp_merged = merged.copy()

    g = lambda *names: _get_merged_col(psp_merged, *names)
    out = pd.DataFrame(index=psp_merged.index)

    out['Index'] = range(1, len(psp_merged) + 1)

    out['Tran.Date'] = g('tran.date', 'date', 'created_at', 'Month, Day, Year of confirmation_time',
                         'Month, Day, Year of created_time')

    # Reference from bank side
    bank_ref = None
    for col in psp_merged.columns:
        low = col.lower()
        if ('reference' in low or 'transactionid' in low.replace('_', '').replace(' ', '')) \
                and '_crm' not in low and col not in ('_join_key', '_merge', '_psp_source'):
            bank_ref = col
            break
    out['Reference'] = psp_merged[bank_ref] if bank_ref else g('psp_transaction_id')

    # Amount from bank side
    bank_amt = next((c for c in psp_merged.columns
                     if 'amount' in c.lower() and '_crm' not in c and not c.startswith('_')), None)
    amount_numeric = pd.to_numeric(psp_merged[bank_amt], errors='coerce') if bank_amt else None
    out['Amount'] = amount_numeric

    # Currency: prefer resolved CRM currency, fall back to bank-side column
    crm_ccy = _resolve_currency(g('currency_id'))
    bank_ccy_col = next(
        (c for c in psp_merged.columns
         if 'currency' in c.lower() and '_crm' not in c and not c.startswith('_')),
        None
    )
    bank_ccy = psp_merged[bank_ccy_col].fillna('') if bank_ccy_col else pd.Series([''] * len(psp_merged), index=psp_merged.index)
    out['Currency'] = crm_ccy.where(crm_ccy.notna() & (crm_ccy.astype(str) != 'nan'), bank_ccy)

    out['AmntBC'] = g('usdamount')

    out['Payment Method'] = g('payment_method')
    out['Bank']           = g('bank_name', 'payment_processor')
    out['Details1']       = g('mtorder_id').astype(str).replace('None', None).replace('nan', None)
    out['Details2']       = None
    out['Comment']        = g('comment')
    fname = g('first_name').fillna('').astype(str).str.strip()
    lname = g('last_name').fillna('').astype(str).str.strip()
    out['Remarks'] = (fname + ' ' + lname).str.strip().replace('', None)

    for col in ['ExcRate', 'Exch.Diff. %', 'ToEmail', 'TranStatus', 'Reference2']:
        out[col] = None
    out['Take To Profit'] = False
    out['ReasonCodeD']    = None
    out['MRS Notes']      = None

    merge_col = psp_merged.get('_merge', pd.Series(['right_only'] * len(psp_merged), index=psp_merged.index))
    out['Recon.Reason Group'] = merge_col.map({'both': 'Matched', 'right_only': 'Bank Only'})
    out['Recon Reason']       = merge_col.map({'both': 'Ref/Ref-Curr-Amnt>nn', 'right_only': 'No CRM Entry'})

    # Match No: use same sequential numbering as MT4-Transactions for matched rows
    lc = build_lifecycle_df(merged)
    lc_psp = lc[lc.index.isin(psp_merged.index)]
    out['Match No']            = lc_psp['Match No']
    out['ReasonCodeGroupName'] = None

    # TRX Type from amount sign: positive = deposit, negative = withdrawal
    if amount_numeric is not None:
        out['TRX Type'] = amount_numeric.apply(
            lambda v: '2. DP' if pd.notna(v) and v > 0 else ('2. WD' if pd.notna(v) and v < 0 else '4. Transfer')
        )
    else:
        out['TRX Type'] = '4. Transfer'

    # PM Name: map upload filename → real PSP name via Mapping Rules
    psp_source = psp_merged['_psp_source'] if '_psp_source' in psp_merged.columns \
                 else pd.Series([None]*len(psp_merged), index=psp_merged.index)
    out['PM Name'] = _psp_source_to_pm_name(psp_source)

    # PM-Cur: "{pm_code}-{currency}"
    currency_str = out['Currency'].fillna('').astype(str).str.upper().replace({'NAN': '', 'NONE': ''})
    pm_code_s = psp_source.apply(
        lambda s: _map_pm_code(str(s).replace('.csv','').replace('.xlsx','').replace('bankFile_','').strip()) or ''
        if pd.notna(s) else ''
    )
    out['PM-Cur'] = pm_code_s.str.cat(currency_str.where(currency_str != '', other=''), sep='-').str.strip('-').replace('', None)

    out['Is Balance Currency'] = None
    out['Balance Currency']    = None
    out['Amount in Bal Curr']  = None
    out['Amount USD']          = pd.to_numeric(g('usdamount'), errors='coerce')

    return out[PM_TRX_COLUMNS]


def _load_opening_balances(equity_path):
    """Parse an equity report file (e.g. Unrealised.xlsx) into a
    {(login, currency): real_equity} dict for use as opening balances.
    """
    if not equity_path or not os.path.exists(equity_path):
        return {}
    try:
        df = pd.read_excel(equity_path, header=1)
        # Expected columns: Login, currency, Real Equity
        login_col = next((c for c in df.columns if c.strip().lower() == 'login'), None)
        ccy_col   = next((c for c in df.columns if c.strip().lower() == 'currency'), None)
        eq_col    = next((c for c in df.columns if 'real equity' in c.strip().lower()), None)
        if not (login_col and ccy_col and eq_col):
            return {}
        result = {}
        for _, row in df[[login_col, ccy_col, eq_col]].dropna().iterrows():
            try:
                key = (int(row[login_col]), str(row[ccy_col]).strip().upper())
                result[key] = float(row[eq_col])
            except (ValueError, TypeError):
                continue
        return result
    except Exception:
        return {}


def build_ccy_lifecycle_df(merged, use_usd=False, opening_balances=None):
    """Build the CCY or USD per-account lifecycle pivot from transaction data.

    Uses the proper MT4 TRX Type attribute numbering derived from payment_method.
    If opening_balances dict {(login, currency): equity} is provided, adds
    '1. Opening Balance' and computes '6. Closing Balance'.
    """
    g = lambda *names: _get_merged_col(merged, *names)

    crm_mask = merged['_merge'] != 'right_only' if '_merge' in merged.columns \
               else pd.Series([True] * len(merged), index=merged.index)

    # Use login (MT4 account ID) — matches ClientAccount in the reference output
    account  = g('login', 'tradingaccountsid').where(crm_mask)
    currency = _resolve_currency(g('currency_id').where(crm_mask))

    # CRM stores amounts as positive for both deposits and withdrawals.
    # Apply sign: withdrawals and fee-type transactions are negative in the output.
    trx_direction = g('transactiontype').where(crm_mask).fillna('').str.lower()
    sign = trx_direction.apply(lambda t: -1 if 'withdraw' in t else 1)
    raw_amt_unsigned = pd.to_numeric(g('usdamount' if use_usd else 'amount'), errors='coerce').where(crm_mask)
    raw_amt = raw_amt_unsigned * sign

    # Build TRX Type from payment_method + transactiontype using attribute mapping
    pay_method = g('payment_method').where(crm_mask).fillna('')
    trx_type_raw = g('transactiontype').where(crm_mask).fillna('')
    is_timing_col = merged.get('IsTiming', pd.Series([False] * len(merged), index=merged.index))

    trx_type_mapped = pd.Series([
        _map_trx_type(pm, tt, it)
        for pm, tt, it in zip(pay_method, trx_type_raw, is_timing_col)
    ], index=merged.index)

    df = pd.DataFrame({
        'Client Account': account,
        'Currency': currency,
        'Amount': raw_amt,
        'TRX Type': trx_type_mapped,
    }).dropna(subset=['Client Account', 'Currency', 'Amount'])
    df['Client Account'] = pd.to_numeric(df['Client Account'], errors='coerce')
    df = df.dropna(subset=['Client Account'])
    df['Client Account'] = df['Client Account'].astype(int)

    rows = []
    attr_col = 'Amount USD' if use_usd else 'Amount'
    open_attr = '1. Opening Before (USD)' if use_usd else '1. Opening Balance'
    ob = opening_balances or {}

    for (acct, ccy), grp in df.groupby(['Client Account', 'Currency']):
        account_rows = []

        def add(attr, vals):
            val = round(float(vals.sum()), 2)
            if val != 0:
                account_rows.append({
                    'Client Account': acct, 'Currency': ccy,
                    'Attribute': attr, attr_col: val
                })

        # Opening balance from equity file (keyed by (login, currency))
        opening = ob.get((acct, str(ccy).upper()), None)
        if opening is not None and opening != 0:
            account_rows.insert(0, {
                'Client Account': acct, 'Currency': ccy,
                'Attribute': open_attr, attr_col: round(opening, 2)
            })

        # Aggregate all TRX Type groups we have data for
        for attr_val, grp_attr in grp.groupby('TRX Type'):
            add(attr_val, grp_attr['Amount'])

        # Closing Balance = Opening + sum of all movements
        if account_rows:
            total_movement = sum(r[attr_col] for r in account_rows
                                 if not r['Attribute'].startswith('1.'))
            if opening is not None:
                account_rows.append({
                    'Client Account': acct, 'Currency': ccy,
                    'Attribute': '6. Closing Balance',
                    attr_col: round(opening + total_movement, 2)
                })

        rows.extend(account_rows)

    result = pd.DataFrame(rows, columns=['Client Account', 'Currency', 'Attribute', attr_col]) \
             if rows else pd.DataFrame(columns=['Client Account', 'Currency', 'Attribute', attr_col])

    return result.sort_values(['Client Account', 'Currency', 'Attribute']).reset_index(drop=True)


def build_pm_lifecycle_df(merged, use_usd=False):
    """Aggregate PM-Transactions by PM Name → lifecycle pivot (PM CCY or USD Life Cycle)."""
    pm_trx = build_pm_transactions_df(merged)

    attr_col = 'Amount USD' if use_usd else 'Value'
    amount_src = 'Amount USD' if use_usd else 'Amount'

    rows = []
    group_cols = ['PM Name', 'Currency'] if not use_usd else ['PM Name']

    for keys, grp in pm_trx.groupby(group_cols):
        pm_name = keys[0] if isinstance(keys, tuple) else keys
        ccy = keys[1] if isinstance(keys, tuple) and len(keys) > 1 else 'USD'

        for attr, sub in grp.groupby('TRX Type'):
            val = round(float(pd.to_numeric(sub[amount_src], errors='coerce').sum()), 2)
            if val != 0:
                row = {'Payment Method': pm_name, 'Attribute': attr, attr_col: val}
                if not use_usd:
                    row['Currency'] = ccy
                rows.append(row)

    if rows:
        result = pd.DataFrame(rows)
        if use_usd:
            result = result[['Payment Method', 'Attribute', attr_col]]
        else:
            result = result[['Payment Method', 'Currency', 'Attribute', attr_col]]
        return result.sort_values(list(result.columns[:-1])).reset_index(drop=True)
    else:
        if use_usd:
            return pd.DataFrame(columns=['Payment Method', 'Attribute', attr_col])
        return pd.DataFrame(columns=['Payment Method', 'Currency', 'Attribute', attr_col])


def _load_mapping_rules():
    """Load the PM Code → PM Name mapping table from the bundled CSV.
    Falls back to an empty DataFrame if the file is not available.
    """
    csv_path = os.path.join(os.path.dirname(__file__), 'data', 'mapping_rules.csv')
    if not os.path.exists(csv_path):
        return pd.DataFrame(columns=['PM Code', 'PM Name', 'PM-Cur', 'Is Balance Currency',
                                     'Balance Currency', 'PM-Bal-Cur', 'Processing Currency',
                                     'Amount factor'])
    try:
        return pd.read_csv(csv_path)
    except Exception:
        return pd.DataFrame()


def build_lifecycle_excel(merged, opening_balance_path=None):
    """Build the multi-tab Life Cycle Report Excel matching the historical format."""
    opening_balances = _load_opening_balances(opening_balance_path)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:

        def _write(df, sheet):
            df.to_excel(writer, sheet_name=sheet, index=False)
            ws = writer.sheets[sheet]
            for col_cells in ws.columns:
                max_len = max((len(str(c.value)) if c.value is not None else 0) for c in col_cells)
                ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 2, 35)

        _write(build_mt4_transactions_df(merged), 'MT4-Transactions')
        _write(build_ccy_lifecycle_df(merged, use_usd=False, opening_balances=opening_balances),
               'MT4 CCY Life Cycle')
        _write(build_ccy_lifecycle_df(merged, use_usd=True, opening_balances=opening_balances),
               'MT4 USD per acc Life Cycle')
        _write(build_pm_lifecycle_df(merged, use_usd=True),  'PM USD Life Cycle')
        _write(build_pm_lifecycle_df(merged, use_usd=False), 'PM CCY Life Cycle')
        _write(build_pm_transactions_df(merged), 'PM-Transactions')
        _write(_load_mapping_rules(), 'Mapping Rules')

    buf.seek(0)
    return buf


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/upload', methods=['POST'])
def upload_files():
    single_files = ['platformFile', 'equityFile', 'transactionsFile']
    labels = {
        'platformFile': 'Platform (CRM/MT4)',
        'equityFile': 'Client Equity Report',
        'transactionsFile': 'Transactions'
    }

    bank_files = request.files.getlist('bankFile')
    if not bank_files or not bank_files[0].filename:
        return jsonify({"error": "Missing Bank/PSP statements."}), 400

    if not all(k in request.files for k in single_files):
        return jsonify({"error": "Missing expected data files."}), 400

    all_headers = {}

    bank_columns_all = []
    bank_filenames = []
    filename_map = {}  # bankFile_N.ext → original filename
    for i, f in enumerate(bank_files):
        ext = os.path.splitext(f.filename)[1] or '.csv'
        saved_name = f"bankFile_{i}{ext}"
        path = os.path.join(app.config['UPLOAD_FOLDER'], saved_name)
        f.save(path)
        headers = extract_headers(path)
        bank_columns_all.extend(headers)
        bank_filenames.append(f.filename)
        filename_map[saved_name] = f.filename

    # Persist the original filename mapping for use in PM Name resolution
    with open(os.path.join(app.config['UPLOAD_FOLDER'], '_filename_map.json'), 'w') as _fmf:
        json.dump(filename_map, _fmf)

    unique_bank_cols = list(dict.fromkeys(bank_columns_all))
    all_headers['Bank/PSP Statements'] = {
        "filename": f"{len(bank_files)} files: {', '.join(bank_filenames[:5])}{'...' if len(bank_filenames) > 5 else ''}",
        "columns": unique_bank_cols
    }

    for k in single_files:
        f = request.files[k]
        ext = os.path.splitext(f.filename)[1] or '.csv'
        path = os.path.join(app.config['UPLOAD_FOLDER'], f"{k}{ext}")
        f.save(path)
        headers = extract_headers(path)
        all_headers[labels[k]] = {
            "filename": f.filename,
            "columns": headers
        }

    # Optional: opening balance file (previous month's equity report)
    ob_file = request.files.get('openingBalanceFile')
    if ob_file and ob_file.filename:
        ext = os.path.splitext(ob_file.filename)[1] or '.xlsx'
        ob_path = os.path.join(app.config['UPLOAD_FOLDER'], f"openingBalanceFile{ext}")
        ob_file.save(ob_path)

    total_cols = sum(len(v['columns']) for v in all_headers.values())
    return jsonify({
        "status": "success",
        "message": f"Parsed {total_cols} unique columns across {len(bank_files) + len(single_files)} files ({len(bank_files)} Bank/PSP statements).",
        "sources": all_headers
    })


@app.route('/api/map-columns', methods=['POST'])
def map_columns():
    """Use an LLM via OpenRouter to analyze detected columns and suggest join key / output mappings."""
    api_key = os.environ.get('OPENROUTER_API_KEY')
    if not api_key:
        try:
            with open(r'C:\Users\aleh.c\Documents\openrouter-key.txt', 'r') as _f:
                api_key = _f.read().strip()
        except FileNotFoundError:
            return jsonify({"status": "error", "message": "No API key found. Set OPENROUTER_API_KEY or create C:\\Users\\aleh.c\\Documents\\openrouter-key.txt"}), 500

    try:
        data = request.json or {}
        sources = data.get('sources', {})

        sources_text = ""
        for source_name, info in sources.items():
            cols = info.get('columns', [])
            sources_text += f"\n{source_name} ({info.get('filename', '')}):\n  {', '.join(cols)}\n"

        prompt = f"""You are a financial reconciliation expert. Analyze these uploaded file schemas and identify column mappings.

TARGET OUTPUT SCHEMA (52 columns):
{', '.join(OUTPUT_COLUMNS)}

UPLOADED SOURCE FILES:
{sources_text}

For each source, identify:
1. The transaction reference/ID column (used for bank-to-CRM matching)
2. The amount column
3. Which source columns map to the output schema columns

Return ONLY valid JSON in this exact format:
{{
  "sources": {{
    "Bank/PSP Statements": {{
      "ref_col": "exact_column_name_or_null",
      "amount_col": "exact_column_name_or_null",
      "mappings": {{"source_column": "Output Column"}}
    }},
    "Platform (CRM/MT4)": {{
      "ref_col": "exact_column_name_or_null",
      "amount_col": "exact_column_name_or_null",
      "mappings": {{"source_column": "Output Column"}}
    }}
  }},
  "join_explanation": "brief explanation of the join strategy"
}}"""

        response = http_requests.post(
            'https://openrouter.ai/api/v1/chat/completions',
            headers={'Authorization': f'Bearer {api_key}', 'Content-Type': 'application/json'},
            json={
                'model': 'anthropic/claude-haiku-4-5',
                'max_tokens': 1024,
                'messages': [{'role': 'user', 'content': prompt}],
            },
            timeout=30
        )
        response.raise_for_status()
        result_text = response.json()['choices'][0]['message']['content'].strip()

        result_text = re.sub(r'^```(?:json)?\s*', '', result_text)
        result_text = re.sub(r'\s*```$', '', result_text)

        try:
            result = json.loads(result_text)
        except json.JSONDecodeError:
            json_match = re.search(r'\{.*\}', result_text, re.DOTALL)
            result = json.loads(json_match.group()) if json_match else {"join_explanation": result_text, "sources": {}}

        return jsonify({"status": "success", "mapping": result})

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/rates')
def get_rates():
    target_currencies = 'EUR,GBP,MXN,ZAR,TRY,NGN,KES,TZS,UGX,GHS,ZMW,BWP,CDF,NAD,PHP,IDR,MYR,VND,AED,COP,EGP,CLP,XOF,XAF,PEN,INR,RWF,BRL'
    apis = [
        f'https://api.frankfurter.app/latest?from=USD&to={target_currencies}',
        'https://open.er-api.com/v6/latest/USD',
    ]
    for url in apis:
        try:
            r = http_requests.get(url, timeout=5)
            data = r.json()
            if 'rates' in data:
                wanted = set(target_currencies.split(','))
                rates = {k: v for k, v in data['rates'].items() if k in wanted}
                return jsonify({"status": "success", "base": "USD", "rates": rates,
                                "crypto_note": "BTC, ETH, USDT, LTC, XRP also used historically"})
        except Exception:
            continue
    return jsonify({"status": "error", "message": "All FX providers unreachable."}), 502


@app.route('/api/rates/crypto')
def get_crypto_rates():
    try:
        r = http_requests.get(
            'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,tether,litecoin,ripple&vs_currencies=usd',
            timeout=5
        )
        data = r.json()
        rates = {
            'BTC': data.get('bitcoin', {}).get('usd', 0),
            'ETH': data.get('ethereum', {}).get('usd', 0),
            'USDT': data.get('tether', {}).get('usd', 1.0),
            'LTC': data.get('litecoin', {}).get('usd', 0),
            'XRP': data.get('ripple', {}).get('usd', 0),
        }
        return jsonify({"status": "success", "rates": rates})
    except Exception:
        return jsonify({"status": "error", "message": "Crypto provider unreachable."}), 502


@app.route('/api/reconcile', methods=['POST'])
def reconcile():
    """Per-PSP reconciliation engine.

    Each PSP file is processed independently so its own reference column is
    detected without interference from other PSPs that use different column names.
    CRM columns are pre-renamed to the _crm suffix before any merge so matched
    and unmatched rows have a consistent column layout in the output DataFrame.
    """
    upload = app.config['UPLOAD_FOLDER']

    try:
        bank_paths = []
        crm_path = None
        for f in sorted(os.listdir(upload)):
            if f.startswith('bankFile_'): bank_paths.append(os.path.join(upload, f))
            if f.startswith('platformFile'): crm_path = os.path.join(upload, f)

        if not bank_paths or not crm_path:
            return jsonify({"status": "error", "summary": {"error": "Missing uploaded files. Please re-upload."}}), 400

        # ── Load CRM ────────────────────────────────────────────────────────
        ext = os.path.splitext(crm_path)[1].lower()
        if ext in ('.xlsx', '.xls'):
            crm_df = pd.read_excel(crm_path)
        else:
            try:
                with open(crm_path, 'r', encoding='utf-8', errors='replace') as f:
                    dialect = csv.Sniffer().sniff(f.read(4096))
                crm_df = pd.read_csv(crm_path, dialect=dialect)
            except Exception:
                crm_df = pd.read_csv(crm_path, encoding='utf-8', encoding_errors='replace')

        crm_cols_lower = {c: c.lower().strip() for c in crm_df.columns}

        crm_ref_col = next((c for c, l in crm_cols_lower.items()
                            if any(k in l for k in ['psp_transaction_id', 'psp_id'])), None)
        # Secondary CRM join key: some PSPs store transactionid instead of psp_transaction_id
        crm_txid_col = next((c for c, l in crm_cols_lower.items()
                             if l == 'transactionid'), None)
        crm_deal_col = next((c for c, l in crm_cols_lower.items()
                             if any(k in l for k in ['mtorder_id', 'deal_no', 'dealid', 'order_id'])), None)
        crm_amount_col = next((c for c, l in crm_cols_lower.items()
                               if 'amount' in l), None)

        if not crm_ref_col:
            return jsonify({"status": "error",
                            "summary": {"error": "Could not detect CRM reference column (psp_transaction_id)."}}), 400

        # Pre-rename all CRM columns to _crm suffix so matched + unmatched
        # rows have consistent column names after concat.
        crm_rename = {c: f"{c}_crm" for c in crm_df.columns if not c.startswith('_')}
        crm_renamed = crm_df.rename(columns=crm_rename).copy()
        crm_ref_col_crm = f"{crm_ref_col}_crm"
        crm_txid_col_crm = f"{crm_txid_col}_crm" if crm_txid_col else None

        crm_renamed['_join_key'] = normalize_key(crm_renamed[crm_ref_col_crm])
        if crm_txid_col_crm and crm_txid_col_crm in crm_renamed.columns:
            crm_renamed['_join_key_txid'] = normalize_key(crm_renamed[crm_txid_col_crm])

        # Assign a stable row ID so claimed rows can be tracked across PSPs
        # regardless of which CRM key column was used for that match.
        crm_renamed['_crm_row_id'] = range(len(crm_renamed))

        # ── Per-PSP loop ─────────────────────────────────────────────────────
        matched_frames = []
        bank_only_frames = []
        all_matched_crm_row_ids = set()   # row IDs of CRM rows already claimed
        psp_stats = []
        total_bank_rows = 0

        # Load original filename mapping (set by upload_files or test_load)
        _fmap_path = os.path.join(upload, '_filename_map.json')
        _filename_map = {}
        if os.path.exists(_fmap_path):
            with open(_fmap_path) as _fmf:
                _filename_map = json.load(_fmf)

        for bp in bank_paths:
            bank_df = _load_psp_file(bp)
            if bank_df is None:
                continue

            # Use original filename for PM Name resolution; fall back to internal name
            _src_name = _filename_map.get(os.path.basename(bp), os.path.basename(bp))

            total_bank_rows += len(bank_df)

            # Collect all candidate ref columns and pick the one with best CRM overlap.
            # This handles cases where the priority-1 column has low overlap (e.g.
            # SolidPayments TransactionId=12 vs UniqueId=79) by trying all candidates.
            ref_candidates = _detect_bank_ref_cols(bank_df)
            if not ref_candidates:
                # Can't match this PSP — all rows go to bank_only
                bonly = bank_df.copy()
                bonly['_join_key'] = None
                bonly['_merge'] = 'right_only'
                bonly['_psp_source'] = _src_name
                bank_only_frames.append(bonly)
                continue

            # CRM keys (will compare against each bank candidate)
            crm_avail_base = crm_renamed[
                ~crm_renamed['_crm_row_id'].isin(all_matched_crm_row_ids)
            ].copy()
            crm_pspid_keys = set(crm_avail_base['_join_key'].dropna())
            crm_txid_keys  = (set(crm_avail_base['_join_key_txid'].dropna())
                              if '_join_key_txid' in crm_avail_base.columns else set())

            best_ref_col = ref_candidates[0]
            best_overlap = 0
            best_crm_key = 'pspid'
            for cand in ref_candidates:
                cand_keys = set(normalize_key(bank_df[cand]).dropna())
                ov_psp  = len(cand_keys & crm_pspid_keys)
                ov_txid = len(cand_keys & crm_txid_keys)
                ov = max(ov_psp, ov_txid)
                if ov > best_overlap:
                    best_overlap = ov
                    best_ref_col = cand
                    best_crm_key = 'txid' if ov_txid > ov_psp else 'pspid'

            bank_ref_col = best_ref_col
            bank_df = bank_df.copy()
            bank_df['_join_key'] = normalize_key(bank_df[bank_ref_col])
            bank_df['_psp_source'] = _src_name

            # Detect and normalize per-PSP amount into a unified column
            _bamt_col = _detect_bank_amount_col(bank_df)
            if _bamt_col:
                bank_df['_bank_amount'] = pd.to_numeric(
                    bank_df[_bamt_col].astype(str).str.replace(',', '', regex=False),
                    errors='coerce')

            # Only attempt to match CRM rows not already claimed by a prior PSP
            crm_available = crm_avail_base.copy()

            # Apply the CRM key chosen during the overlap comparison above
            crm_key_label = crm_ref_col
            if best_crm_key == 'txid' and '_join_key_txid' in crm_available.columns:
                crm_available['_join_key'] = crm_available['_join_key_txid']
                crm_key_label = crm_txid_col

            crm_available = crm_available[crm_available['_join_key'].notna()]
            inner = crm_available.merge(bank_df, on='_join_key', how='inner')

            if len(inner) > 0:
                # Deduplicate: multiple bank rows can share the same reference
                # (e.g. TrustPayments settlement batches). Keep only the first
                # bank match per CRM row to prevent double-counting CRM amounts.
                crm_id_col = next(
                    (c for c in inner.columns
                     if c in ('mtorder_id_crm', 'transactionid_crm', 'mttransactionsid_crm')),
                    None
                )
                if crm_id_col:
                    inner = inner.drop_duplicates(subset=[crm_id_col])

                inner['_merge'] = 'both'
                matched_frames.append(inner)
                all_matched_crm_row_ids.update(inner['_crm_row_id'].tolist())
                psp_stats.append(f"{os.path.basename(bp)}[{bank_ref_col}→{crm_key_label}]:{len(inner)}")

            # Collect bank rows that didn't match any CRM row
            matched_bank_keys = set(inner['_join_key'].dropna().tolist()) if len(inner) > 0 else set()
            bonly = bank_df[bank_df['_join_key'].notna() &
                            ~bank_df['_join_key'].isin(matched_bank_keys)].copy()
            if len(bonly) > 0:
                bonly['_merge'] = 'right_only'
                bank_only_frames.append(bonly)

        # ── Build final merged DataFrame ─────────────────────────────────────
        crm_unmatched = crm_renamed[
            ~crm_renamed['_crm_row_id'].isin(all_matched_crm_row_ids)
        ].copy()
        crm_unmatched['_merge'] = 'left_only'

        parts = [crm_unmatched] + matched_frames + bank_only_frames
        merged = pd.concat(parts, ignore_index=True, sort=False)

        # ── Stats ────────────────────────────────────────────────────────────
        total_crm  = len(crm_df)
        matched    = int((merged['_merge'] == 'both').sum())
        crm_only   = int((merged['_merge'] == 'left_only').sum())
        bank_only  = int((merged['_merge'] == 'right_only').sum())

        unrecon_fees = 0.0
        if crm_amount_col and matched > 0:
            both = merged[merged['_merge'] == 'both'].copy()
            crm_amt = f"{crm_amount_col}_crm"
            if crm_amt in both.columns and '_bank_amount' in both.columns:
                a = pd.to_numeric(both[crm_amt], errors='coerce').abs()
                b = both['_bank_amount'].abs()   # already numeric from per-PSP loop
                valid = a.notna() & b.notna()
                unrecon_fees = float((a[valid] - b[valid]).abs().sum())

        if psp_stats:
            n = len(psp_stats)
            preview = ', '.join(psp_stats[:3]) + (f' (+{n-3} more)' if n > 3 else '')
            join_info = f"CRM[{crm_ref_col}] matched {n} PSP(s): {preview}"
        else:
            join_info = f"CRM[{crm_ref_col}] — no PSP reference columns detected"

        # ── Unmatched CRM breakdown by TRX Type ──────────────────────────────
        # Derive TRX type for unmatched CRM rows so the UI can show how many
        # are non-PSP internal entries (PRF/ADJ/BN/TRF) vs genuine gaps.
        crm_pm_col  = next((c for c, l in crm_cols_lower.items() if l == 'payment_method'), None)
        crm_tt_col  = next((c for c, l in crm_cols_lower.items() if l == 'transactiontype'), None)
        crm_pm_col_r = f"{crm_pm_col}_crm" if crm_pm_col else None
        crm_tt_col_r = f"{crm_tt_col}_crm" if crm_tt_col else None

        PSP_TYPES = {'2. DP', '2. WD'}

        # TRX types for ALL CRM rows — needed for crm_psp_total denominator
        crm_psp_total = total_crm
        if crm_pm_col and crm_tt_col:
            all_pm = crm_df[crm_pm_col].fillna('')
            all_tt = crm_df[crm_tt_col].fillna('')
            all_trx = [_map_trx_type(pm, tt) for pm, tt in zip(all_pm, all_tt)]
            crm_psp_total = sum(1 for t in all_trx if t in PSP_TYPES)

        unmatched_trx_breakdown: dict = {}
        internal_trx_breakdown: dict = {}
        if crm_pm_col_r and crm_pm_col_r in crm_unmatched.columns:
            pm_series = crm_unmatched[crm_pm_col_r].fillna('')
            tt_series = crm_unmatched[crm_tt_col_r].fillna('') if (crm_tt_col_r and crm_tt_col_r in crm_unmatched.columns) else pd.Series([''] * len(crm_unmatched))
            trx_types = [_map_trx_type(pm, tt) for pm, tt in zip(pm_series, tt_series)]
            from collections import Counter
            counts = Counter(trx_types)
            # Split PSP (2. DP / 2. WD) from internal — sorted alphabetically within each group
            unmatched_trx_breakdown = {
                k: v for k, v in sorted(counts.items(), key=lambda x: x[0])
                if k in PSP_TYPES
            }
            internal_trx_breakdown = {
                k: v for k, v in sorted(counts.items(), key=lambda x: x[0])
                if k not in PSP_TYPES
            }

        crm_psp_unmatched = sum(unmatched_trx_breakdown.values())

        # Check if an opening balance file was uploaded (optional)
        ob_path = None
        for fname in os.listdir(upload):
            if fname.startswith('openingBalanceFile'):
                ob_path = os.path.join(upload, fname)
                break

        with open(STATE_FILE, 'wb') as f:
            pickle.dump({'merged': merged, 'crm_df': crm_df,
                         'opening_balance_path': ob_path}, f)

        return jsonify({
            "status": "success",
            "summary": {
                "total_crm_rows":      total_crm,
                "crm_psp_total":       crm_psp_total,
                "crm_psp_unmatched":   crm_psp_unmatched,
                "total_bank_rows":     total_bank_rows,
                "total_matched":       matched,
                "total_orphaned":      crm_only + bank_only,
                "crm_unmatched":       crm_only,
                "bank_unmatched":      bank_only,
                "unrecon_fees":        round(unrecon_fees, 2),
                "join_keys_used":      join_info,
                "deal_no_column":      crm_deal_col or "not detected",
                "unmatched_trx_breakdown":  unmatched_trx_breakdown,
                "internal_trx_breakdown":   internal_trx_breakdown,
            }
        })

    except Exception as e:
        return jsonify({"status": "error", "summary": {"error": str(e)}}), 500


@app.route('/api/unmatched-crm')
def unmatched_crm_rows():
    """Return unmatched CRM rows for a given TRX type, for manual inspection."""
    trx_type_filter = request.args.get('trx_type', '').strip()
    try:
        with open(STATE_FILE, 'rb') as f:
            state = pickle.load(f)
    except FileNotFoundError:
        return jsonify({"error": "No reconciliation data found."}), 400

    try:
        merged = state['merged']
        crm_df = state['crm_df']
        unmatched = merged[merged['_merge'] == 'left_only'].copy()

        # Re-detect CRM column names (strip _crm suffix for lookup)
        crm_cols_lower = {c: c.lower().strip() for c in crm_df.columns}
        crm_pm_col  = next((c for c, l in crm_cols_lower.items() if l == 'payment_method'), None)
        crm_tt_col  = next((c for c, l in crm_cols_lower.items() if l == 'transactiontype'), None)
        crm_pm_col_r = f"{crm_pm_col}_crm" if crm_pm_col else None
        crm_tt_col_r = f"{crm_tt_col}_crm" if crm_tt_col else None

        # Derive TRX type for each unmatched row and filter
        if crm_pm_col_r and crm_pm_col_r in unmatched.columns:
            pm_s = unmatched[crm_pm_col_r].fillna('')
            tt_s = unmatched[crm_tt_col_r].fillna('') if (crm_tt_col_r and crm_tt_col_r in unmatched.columns) else pd.Series([''] * len(unmatched), index=unmatched.index)
            unmatched['_trx_type'] = [_map_trx_type(pm, tt) for pm, tt in zip(pm_s, tt_s)]
        else:
            unmatched['_trx_type'] = 'Unknown'

        if trx_type_filter:
            unmatched = unmatched[unmatched['_trx_type'] == trx_type_filter]

        # Pick display columns: prefer the most useful CRM fields, strip _crm suffix for headers
        WANT = ['login', 'tradingaccountsid', 'tran.date', 'date', 'createdate',
                'amount', 'currency', 'payment_method', 'transactiontype',
                'payment_processor', 'psp_transaction_id', 'transactionid', 'bank_name']
        display_cols = {}
        for want in WANT:
            col_r = f"{want}_crm"
            if col_r in unmatched.columns:
                display_cols[col_r] = want

        rows_df = unmatched[list(display_cols.keys())].rename(columns=display_cols)

        # Use pandas to_json to safely handle numpy types / NaN, then parse back
        import json as _json
        rows_list = _json.loads(rows_df.head(500).to_json(orient='values'))

        return jsonify({
            "trx_type": trx_type_filter,
            "count": len(rows_df),
            "columns": list(rows_df.columns),
            "rows": rows_list
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/download/lifecycle')
def download_lifecycle():
    """Generate and serve the multi-tab Life Cycle Report matching the historical format."""
    try:
        with open(STATE_FILE, 'rb') as f:
            state = pickle.load(f)
    except FileNotFoundError:
        return jsonify({"error": "No reconciliation data found. Run reconciliation first."}), 400

    try:
        buf = build_lifecycle_excel(state['merged'],
                                    opening_balance_path=state.get('opening_balance_path'))
        filename = f"Life Cycle Report {datetime.now().strftime('%Y-%m-%d')}.xlsx"
        return send_file(buf, as_attachment=True, download_name=filename,
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/download/balances')
def download_balances():
    """Generate and serve Balances.xlsx with currency equity summary."""
    try:
        with open(STATE_FILE, 'rb') as f:
            state = pickle.load(f)
    except FileNotFoundError:
        return jsonify({"error": "No reconciliation data found. Run reconciliation first."}), 400

    try:
        df = build_balances_df(state['merged'])
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Balances', index=False)
            ws = writer.sheets['Balances']
            for col_cells in ws.columns:
                max_len = max((len(str(cell.value)) if cell.value is not None else 0) for cell in col_cells)
                ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 2, 25)
        buf.seek(0)
        filename = f"Balances {datetime.now().strftime('%Y-%m-%d')}.xlsx"
        return send_file(buf, as_attachment=True, download_name=filename,
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _find_dir_icase(parent, *candidates):
    """Find the first existing subdirectory of parent matching any candidate name (case-insensitive)."""
    try:
        entries = {e.lower(): e for e in os.listdir(parent)}
    except OSError:
        return None
    for name in candidates:
        if name.lower() in entries:
            path = os.path.join(parent, entries[name.lower()])
            if os.path.isdir(path):
                return path
    return None


def _find_file_pattern(directory, *patterns):
    """Return path of the best-matching file in directory whose lowercase name starts
    with any of the given patterns. When multiple files match, the shortest filename
    wins (most canonical name, avoids DEC/JAN suffix copies and numbered duplicates).
    """
    if not directory or not os.path.isdir(directory):
        return None
    matches = []
    for fname in os.listdir(directory):
        fpath = os.path.join(directory, fname)
        if not os.path.isfile(fpath):
            continue
        fl = fname.lower()
        for pat in patterns:
            if fl.startswith(pat.lower()):
                matches.append(fpath)
                break
    if not matches:
        return None
    return min(matches, key=lambda p: len(os.path.basename(p)))


def _detect_month_files(month_dir):
    """Locate platform + PSP files for a month, tolerating folder/filename variations.
    Returns a dict with keys: plat_dir, psp_dir, crm, equity, transactions, opening_balance.
    Values are absolute paths or None if not found.
    """
    plat_dir = _find_dir_icase(month_dir, 'platform')
    psp_dir  = _find_dir_icase(month_dir, 'PSPs', 'PSP')

    crm          = _find_file_pattern(plat_dir, 'CRM Transactions')
    transactions = _find_file_pattern(plat_dir, 'Deposit and Withdrawal Report')
    equity       = _find_file_pattern(plat_dir, 'Client Balance', 'Equity Check', 'Equity Report')
    opening_bal  = _find_file_pattern(plat_dir, 'Unrealised')

    return {
        'plat_dir':        plat_dir,
        'psp_dir':         psp_dir,
        'crm':             crm,
        'equity':          equity,
        'transactions':    transactions,
        'opening_balance': opening_bal,
    }


def _copy_month_to_uploads(month_dir):
    """Copy a month's PSP + platform files into the uploads folder.
    Returns all_headers dict for the /api/upload-style response.
    Tolerates folder/filename variations across months (case-insensitive dirs,
    pattern-matched filenames).
    """
    import shutil
    upload = app.config['UPLOAD_FOLDER']
    files  = _detect_month_files(month_dir)

    if not files['crm']:
        raise FileNotFoundError(f"No CRM Transactions file found in {month_dir}")
    if not files['psp_dir']:
        raise FileNotFoundError(f"No PSP/PSPs folder found in {month_dir}")

    # Clear any old bank files
    for f in os.listdir(upload):
        if f.startswith('bankFile_'):
            os.remove(os.path.join(upload, f))

    labels = {
        'platformFile':     'Platform (CRM/MT4)',
        'equityFile':       'Client Equity Report',
        'transactionsFile': 'Transactions',
    }
    all_headers = {}

    # Required: CRM platform file
    ext = os.path.splitext(files['crm'])[1]
    dst = os.path.join(upload, f'platformFile{ext}')
    shutil.copy(files['crm'], dst)
    all_headers[labels['platformFile']] = {
        'filename': os.path.basename(files['crm']),
        'columns': extract_headers(dst)
    }

    # Optional: equity file
    if files['equity']:
        ext = os.path.splitext(files['equity'])[1]
        dst = os.path.join(upload, f'equityFile{ext}')
        shutil.copy(files['equity'], dst)
        all_headers[labels['equityFile']] = {
            'filename': os.path.basename(files['equity']),
            'columns': extract_headers(dst)
        }

    # Optional: transactions file
    if files['transactions']:
        ext = os.path.splitext(files['transactions'])[1]
        dst = os.path.join(upload, f'transactionsFile{ext}')
        shutil.copy(files['transactions'], dst)
        all_headers[labels['transactionsFile']] = {
            'filename': os.path.basename(files['transactions']),
            'columns': extract_headers(dst)
        }

    # Copy PSP files
    idx = 0
    filename_map = {}
    bank_columns_all = []
    bank_filenames = []
    for fname in sorted(os.listdir(files['psp_dir'])):
        fpath = os.path.join(files['psp_dir'], fname)
        if not os.path.isfile(fpath):
            continue
        ext = os.path.splitext(fname)[1].lower()
        if ext not in ('.csv', '.xlsx', '.xls'):
            continue
        saved_name = f'bankFile_{idx}{ext}'
        shutil.copy(fpath, os.path.join(upload, saved_name))
        filename_map[saved_name] = fname
        headers = extract_headers(os.path.join(upload, saved_name))
        bank_columns_all.extend(headers)
        bank_filenames.append(fname)
        idx += 1

    with open(os.path.join(upload, '_filename_map.json'), 'w') as _f:
        json.dump(filename_map, _f)

    unique_bank_cols = list(dict.fromkeys(bank_columns_all))
    all_headers['Bank/PSP Statements'] = {
        'filename': f"{idx} files: {', '.join(bank_filenames[:5])}{'...' if len(bank_filenames) > 5 else ''}",
        'columns': unique_bank_cols
    }

    # Optional opening balance
    if files['opening_balance']:
        dst = os.path.join(upload, 'openingBalanceFile.xlsx')
        shutil.copy(files['opening_balance'], dst)

    # Build a manifest so the browser can fetch each file back and populate inputs
    file_manifest = {}
    for key, fname in [
        ('platformFile',     files['crm']),
        ('equityFile',       files['equity']),
        ('transactionsFile', files['transactions']),
        ('openingBalanceFile', files['opening_balance']),
    ]:
        if fname:
            ext = os.path.splitext(fname)[1]
            saved = f'{key}{ext}'
            file_manifest[key] = {
                'saved': saved,
                'original': os.path.basename(fname),
            }
    file_manifest['bankFiles'] = [
        {'saved': sn, 'original': on}
        for sn, on in filename_map.items()
    ]

    return all_headers, file_manifest


@app.route('/api/uploads/<path:filename>', methods=['GET'])
def serve_upload(filename):
    """Serve a previously-uploaded file back to the browser (used by test prefill)."""
    # Prevent path traversal — only serve flat filenames from the uploads dir
    safe_name = os.path.basename(filename)
    path = os.path.join(os.path.abspath(app.config['UPLOAD_FOLDER']), safe_name)
    if not path.startswith(os.path.abspath(app.config['UPLOAD_FOLDER'])):
        return jsonify({'error': 'Forbidden'}), 403
    if not os.path.isfile(path):
        return jsonify({'error': 'Not found'}), 404
    return send_file(path)


@app.route('/api/test-datasets', methods=['GET'])
def test_datasets():
    """Return a list of available test dataset months under relevant-data/MRS.
    A month is included only if it has a detectable platform dir with a CRM file
    AND a PSP dir with at least one CSV/Excel file.
    """
    base = os.path.join(os.path.dirname(__file__), '..', 'relevant-data', 'MRS')
    datasets = []
    for year_dir in sorted(os.listdir(base)):
        year_path = os.path.join(base, year_dir)
        if not os.path.isdir(year_path):
            continue
        for month_dir in sorted(os.listdir(year_path)):
            month_path = os.path.join(year_path, month_dir)
            if not os.path.isdir(month_path):
                continue
            files = _detect_month_files(month_path)
            if not files['crm'] or not files['psp_dir']:
                continue
            # Require at least one PSP file
            psp_files = [f for f in os.listdir(files['psp_dir'])
                         if os.path.isfile(os.path.join(files['psp_dir'], f))
                         and os.path.splitext(f)[1].lower() in ('.csv', '.xlsx', '.xls')]
            if not psp_files:
                continue
            datasets.append({
                'id':    f'{year_dir}/{month_dir}',
                'label': f'{month_dir} ({year_dir})',
                'year':  year_dir,
                'month': month_dir,
                'psp_count': len(psp_files),
            })
    return jsonify({'status': 'success', 'datasets': datasets})


@app.route('/api/test-prefill', methods=['POST'])
def test_prefill():
    """Copy a test dataset's files to uploads and return column metadata (same shape
    as /api/upload), so the UI can proceed through Stage 2 → Stage 3 normally.
    """
    data = request.json or {}
    dataset_id = data.get('dataset_id', '2023/01. Jan. 2023')

    base = os.path.join(os.path.dirname(__file__), '..', 'relevant-data', 'MRS')
    month_dir = os.path.join(base, dataset_id)
    if not os.path.isdir(month_dir):
        return jsonify({'status': 'error', 'error': f'Dataset not found: {dataset_id}'}), 404

    try:
        all_headers, file_manifest = _copy_month_to_uploads(month_dir)
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)}), 500

    total_cols = sum(len(v['columns']) for v in all_headers.values())
    file_count = sum(1 for k in all_headers if k != 'Bank/PSP Statements') + 1
    return jsonify({
        'status': 'success',
        'message': f'Test data loaded: {dataset_id} — {total_cols} columns across {file_count} sources.',
        'sources': all_headers,
        'file_manifest': file_manifest,
    })


@app.route('/api/test', methods=['POST'])
def test_load():
    """Load ALL January 2023 PSP files + CRM from relevant-data and run reconcile.
    Legacy endpoint kept for backward compatibility — use /api/test-prefill instead.
    """
    base = os.path.join(os.path.dirname(__file__), '..', 'relevant-data',
                        'MRS', '2023', '01. Jan. 2023')
    try:
        _copy_month_to_uploads(base)  # return value unused in legacy path
    except Exception as e:
        return jsonify({'status': 'error', 'summary': {'error': str(e)}}), 500

    from flask import current_app
    with current_app.test_request_context('/api/reconcile', method='POST'):
        result = reconcile()

    return result


if __name__ == '__main__':
    print("Starting MRS 2.0 Web GUI...")
    app.run(debug=True, port=5000)
