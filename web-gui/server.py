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

    # Priority 1 — exact normalized matches (most reliable)
    exact = [
        'transactionreference',   # TrustPayments
        'transactionreference',
        'merchantreference',      # EFTpay, Swiffy
        'transactionid',          # Finrax, Solidpayments
        'txid',                   # generic
        'referenceno',            # Finrax all.xlsx
        'settlementreference',    # Korapay settlements
        'transactionreference',
        'paymentreference',       # Korapay pay-ins (alt)
        'refno',                  # Solidpayment fees
        'refid',                  # VP Refunds
    ]
    for keyword in exact:
        for col, n in normed.items():
            if n == keyword:
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
    ]
    for keyword in contains:
        for col, n in normed.items():
            if keyword in n:
                return col

    # Priority 3 — generic 'reference' substring (last resort before bare id)
    for col, n in normed.items():
        if 'reference' in n:
            return col

    # Priority 4 — bare 'id' column (Neteller, Skrill, Ozow, Zota, Zotapay)
    for col in df.columns:
        if col.strip().lower() in ('id', 'transaction details'):
            return col

    return None


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
    try:
        if ext in ('.xlsx', '.xls'):
            df = pd.read_excel(path)
        else:
            try:
                with open(path, 'r', encoding='utf-8', errors='replace') as f:
                    dialect = csv.Sniffer().sniff(f.read(4096))
                df = pd.read_csv(path, dialect=dialect, encoding='utf-8',
                                 encoding_errors='replace')
            except Exception:
                df = pd.read_csv(path, encoding='utf-8', encoding_errors='replace')
        # Skip files with no usable columns (e.g. Nuvei.xlsx balance/summary sheets)
        real_cols = [c for c in df.columns if not str(c).startswith('Unnamed')
                     and not str(c).startswith('CPanel')]
        return df if real_cols else None
    except Exception:
        return None


def build_lifecycle_df(merged):
    """Build the 52-column Lifecycle List from the outer-joined merged DataFrame."""
    g = lambda *names: _get_merged_col(merged, *names)
    out = pd.DataFrame(index=merged.index)

    out['Tran.Date'] = g('Month, Day, Year of confirmation_time', 'Month, Day, Year of created_time')
    out['Reference'] = g('psp_transaction_id', 'receipt', 'transactionreference')
    out['Deal No'] = g('mtorder_id')
    out['Amount'] = pd.to_numeric(g('amount'), errors='coerce')
    out['Commission'] = 0
    out['Total'] = out['Amount']
    out['Currency'] = g('currency_id')
    out['AmntBC'] = pd.to_numeric(g('usdamount'), errors='coerce')
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
        out['IsTiming'] = ~is_matched
        out['MatchCount'] = is_matched.astype(int)
        out['Matched By'] = merged['_merge'].map({'both': 'MRS', 'left_only': None, 'right_only': None})
        now = datetime.now()
        out['Matched On'] = merged['_merge'].apply(lambda x: now if x == 'both' else None)
    else:
        out['Match No'] = None
        out['Recon.Reason Group'] = None
        out['Recon Reason'] = None
        out['IsTiming'] = False
        out['MatchCount'] = 0
        out['Matched By'] = None
        out['Matched On'] = None

    out['EOD'] = None
    out['IsEODTran'] = None
    out['Type'] = g('transactiontype')
    out['ClientCode'] = g('vtigeraccountid')
    out['ClientAccount'] = g('tradingaccountsid')
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

    currency = g('currency_id').fillna('USD')
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
    """Build the MT4-Transactions tab (44 cols) matching Life Cycle Report-final.xlsx."""
    # Start from the full 52-col lifecycle df and remap to the 44-col schema
    lc = build_lifecycle_df(merged)
    out = pd.DataFrame(index=lc.index)

    # Columns that exist unchanged in both schemas
    shared = [
        'Tran.Date', 'Reference', 'Deal No', 'Amount', 'Commission', 'Total',
        'Currency', 'AmntBC', 'CommissionBC', 'TotalBC', 'Reason Code',
        'Payment Method', 'Bank', 'Institution', 'Details1', 'Details2',
        'Comment', 'Remarks', 'Exch.Diff. %', 'Take To Profit', 'Unrecon. Fees',
        'Match No', 'Recon.Reason Group', 'Recon Reason', 'IsTiming', 'MatchCount',
        'EOD', 'IsEODTran', 'ClientAccount', 'ReasonCodeD', 'CategoryCode',
        'Reason Code Group', 'MRS Notes', 'Country', 'IsSeggr', 'PlatformName',
        'ClientType', 'ClientGroup', 'Matched By', 'ReasonCodeGroupName', 'Matched On'
    ]
    for col in shared:
        out[col] = lc[col] if col in lc.columns else None

    # Country_1 = Country.1 (naming difference only)
    out['Country_1'] = lc['Country.1'] if 'Country.1' in lc.columns else None

    # Index = sequential row number (1-based)
    out['Index'] = range(1, len(out) + 1)

    # TRX Type = Type from our schema
    out['TRX Type'] = lc['Type'] if 'Type' in lc.columns else None

    return out[MT4_TRX_COLUMNS]


def build_pm_transactions_df(merged):
    """Build the PM-Transactions tab (31 cols) from the bank/PSP side rows."""
    g = lambda *names: _get_merged_col(merged, *names)

    # PM-Transactions covers both matched bank rows and bank-only rows
    # For matched rows, bank columns are available directly (no _bank suffix since
    # they come from bank_df which wasn't renamed)
    out = pd.DataFrame(index=merged.index)

    # Sequential index
    out['Index'] = range(1, len(merged) + 1)

    # Date — bank files rarely have a clean date; fall back to CRM date
    out['Tran.Date'] = g('tran.date', 'date', 'created_at', 'Month, Day, Year of confirmation_time',
                         'Month, Day, Year of created_time')

    # Reference from bank side
    # Look for any column that has 'reference' or 'id' in its name (from bank)
    bank_ref = None
    for col in merged.columns:
        low = col.lower()
        if ('reference' in low or 'transactionid' in low.replace('_', '').replace(' ', '')) \
                and '_crm' not in low and col not in ('_join_key', '_merge', '_psp_source'):
            bank_ref = col
            break
    out['Reference'] = merged[bank_ref] if bank_ref else g('psp_transaction_id')

    # Amount from bank side
    bank_amt = next((c for c in merged.columns
                     if 'amount' in c.lower() and '_crm' not in c and not c.startswith('_')), None)
    out['Amount']  = pd.to_numeric(merged[bank_amt], errors='coerce') if bank_amt else None
    out['Currency'] = g('currency_id')
    out['AmntBC']   = g('usdamount')

    # Payment method / bank info from CRM (most reliable source)
    out['Payment Method'] = g('payment_method')
    out['Bank']           = g('bank_name', 'payment_processor')
    out['Details1']       = g('mtorder_id').astype(str).replace('None', None).replace('nan', None)
    out['Details2']       = None
    out['Comment']        = g('comment')
    fname = g('first_name').fillna('').astype(str).str.strip()
    lname = g('last_name').fillna('').astype(str).str.strip()
    out['Remarks'] = (fname + ' ' + lname).str.strip().replace('', None)

    # Columns we can't compute — leave blank
    for col in ['ExcRate', 'Exch.Diff. %', 'ToEmail', 'TranStatus', 'Reference2']:
        out[col] = None
    out['Take To Profit'] = False

    # Reconciliation metadata
    out['ReasonCodeD']       = None
    out['MRS Notes']         = None
    out['Recon.Reason Group'] = merged.get('_merge', pd.Series(['left_only'] * len(merged))).map(
        {'both': 'Matched', 'left_only': 'Unmatched', 'right_only': 'Bank Only'})
    out['Recon Reason']       = merged.get('_merge', pd.Series(['left_only'] * len(merged))).map(
        {'both': 'Ref/Ref-Curr-Amnt>nn', 'left_only': 'No Match', 'right_only': 'No CRM Entry'})

    # Match No from lifecycle df
    lc = build_lifecycle_df(merged)
    out['Match No']            = lc['Match No']
    out['ReasonCodeGroupName'] = None

    out['TRX Type'] = g('transactiontype')

    # PM-specific columns we don't have data for
    psp_source = merged['_psp_source'] if '_psp_source' in merged.columns else None
    out['PM Name']             = psp_source
    out['PM-Cur']              = g('currency_id')
    out['Is Balance Currency'] = None
    out['Balance Currency']    = None
    out['Amount in Bal Curr']  = None
    out['Amount USD']          = g('usdamount')

    return out[PM_TRX_COLUMNS]


def build_ccy_lifecycle_df(merged, use_usd=False):
    """Build the CCY or USD per-account lifecycle pivot from transaction data.

    Computes the attributes we can derive from transactions:
    2. DP  — deposits (positive amounts)
    2. WD  — withdrawals (negative amounts)
    4. Transfer — internal transfers
    3. Timing Deposit / 3. Timing Withdrawal — timing rows

    Opening Balance and P&L require the equity report and are left blank.
    """
    g = lambda *names: _get_merged_col(merged, *names)

    # Only process CRM rows (left_only or both) — bank-only have no account info
    crm_mask = merged['_merge'] != 'right_only' if '_merge' in merged.columns \
               else pd.Series([True] * len(merged), index=merged.index)

    account  = g('tradingaccountsid').where(crm_mask)
    currency = g('currency_id').where(crm_mask)
    raw_amt  = pd.to_numeric(g('usdamount' if use_usd else 'amount'), errors='coerce').where(crm_mask)
    is_timing = merged.get('IsTiming', pd.Series([False] * len(merged), index=merged.index))
    trx_type  = g('transactiontype').fillna('').astype(str).str.lower()

    df = pd.DataFrame({
        'Client Account': account,
        'Currency': currency,
        'Amount': raw_amt,
        'IsTiming': is_timing,
        'TRX Type': trx_type,
    }).dropna(subset=['Client Account', 'Currency', 'Amount'])

    rows = []
    attr_col = 'Amount USD' if use_usd else 'Amount'

    for (acct, ccy), grp in df.groupby(['Client Account', 'Currency']):
        def add(attr, mask):
            val = grp.loc[mask, 'Amount'].sum()
            if val != 0:
                rows.append({'Client Account': acct, 'Currency': ccy, 'Attribute': attr, attr_col: round(val, 2)})

        deposits    = grp['TRX Type'].str.contains('deposit', na=False)
        withdrawals = grp['TRX Type'].str.contains('withdraw', na=False)
        transfers   = grp['TRX Type'].str.contains('transfer', na=False)
        timing      = grp['IsTiming'].fillna(False).astype(bool)

        add('2. DP',                  deposits & ~timing)
        add('2. WD',                  withdrawals & ~timing)
        add('4. Transfer',            transfers)
        add('3. Timing Deposit',      deposits & timing)
        add('3. Timing Withdrawal',   withdrawals & timing)

    result = pd.DataFrame(rows, columns=['Client Account', 'Currency', 'Attribute', attr_col]) \
             if rows else pd.DataFrame(columns=['Client Account', 'Currency', 'Attribute', attr_col])

    return result.sort_values(['Client Account', 'Currency', 'Attribute']).reset_index(drop=True)


def build_lifecycle_excel(merged):
    """Build the multi-tab Life Cycle Report Excel matching the historical format."""
    buf = io.BytesIO()

    with pd.ExcelWriter(buf, engine='openpyxl') as writer:

        def _write(df, sheet):
            df.to_excel(writer, sheet_name=sheet, index=False)
            ws = writer.sheets[sheet]
            for col_cells in ws.columns:
                max_len = max((len(str(c.value)) if c.value is not None else 0) for c in col_cells)
                ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 2, 35)

        _write(build_mt4_transactions_df(merged),          'MT4-Transactions')
        _write(build_ccy_lifecycle_df(merged, use_usd=False), 'MT4 CCY Life Cycle')
        _write(build_ccy_lifecycle_df(merged, use_usd=True),  'MT4 USD per acc Life Cycle')

        # Empty tabs matching the reference structure
        for sheet in ('PM USD Life Cycle', 'PM CCY Life Cycle'):
            pd.DataFrame().to_excel(writer, sheet_name=sheet, index=False)

        _write(build_pm_transactions_df(merged), 'PM-Transactions')

        # Mapping Rules — metadata
        meta = pd.DataFrame({
            'Key': ['Generated by', 'Date', 'Engine'],
            'Value': ['MRS 2.0', datetime.now().strftime('%Y-%m-%d %H:%M'), 'Flask/pandas per-PSP engine']
        })
        _write(meta, 'Mapping Rules')

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
    for i, f in enumerate(bank_files):
        ext = os.path.splitext(f.filename)[1] or '.csv'
        path = os.path.join(app.config['UPLOAD_FOLDER'], f"bankFile_{i}{ext}")
        f.save(path)
        headers = extract_headers(path)
        bank_columns_all.extend(headers)
        bank_filenames.append(f.filename)

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
        crm_renamed['_join_key'] = normalize_key(crm_renamed[crm_ref_col_crm])

        # ── Per-PSP loop ─────────────────────────────────────────────────────
        matched_frames = []
        bank_only_frames = []
        all_matched_crm_keys = set()
        psp_stats = []
        total_bank_rows = 0

        for bp in bank_paths:
            bank_df = _load_psp_file(bp)
            if bank_df is None:
                continue

            total_bank_rows += len(bank_df)
            bank_ref_col = _detect_bank_ref_col(bank_df)

            if not bank_ref_col:
                # Can't match this PSP — all rows go to bank_only
                bonly = bank_df.copy()
                bonly['_join_key'] = None
                bonly['_merge'] = 'right_only'
                bonly['_psp_source'] = os.path.basename(bp)
                bank_only_frames.append(bonly)
                continue

            bank_df = bank_df.copy()
            bank_df['_join_key'] = normalize_key(bank_df[bank_ref_col])
            bank_df['_psp_source'] = os.path.basename(bp)

            # Only attempt to match CRM rows not already claimed by a prior PSP
            crm_available = crm_renamed[
                crm_renamed['_join_key'].notna() &
                ~crm_renamed['_join_key'].isin(all_matched_crm_keys)
            ]

            inner = crm_available.merge(bank_df, on='_join_key', how='inner')

            if len(inner) > 0:
                inner['_merge'] = 'both'
                matched_frames.append(inner)
                all_matched_crm_keys.update(inner['_join_key'].dropna().tolist())
                psp_stats.append(f"{os.path.basename(bp)}[{bank_ref_col}]:{len(inner)}")

            # Collect bank rows that didn't match any CRM row
            matched_bank_keys = set(inner['_join_key'].dropna().tolist()) if len(inner) > 0 else set()
            bonly = bank_df[bank_df['_join_key'].notna() &
                            ~bank_df['_join_key'].isin(matched_bank_keys)].copy()
            if len(bonly) > 0:
                bonly['_merge'] = 'right_only'
                bank_only_frames.append(bonly)

        # ── Build final merged DataFrame ─────────────────────────────────────
        crm_unmatched = crm_renamed[
            crm_renamed['_join_key'].isna() |
            ~crm_renamed['_join_key'].isin(all_matched_crm_keys)
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
            bank_amt_col = next((c for c in both.columns
                                 if 'amount' in c.lower() and c != crm_amt and not c.endswith('_crm')), None)
            if crm_amt in both.columns and bank_amt_col:
                a = pd.to_numeric(both[crm_amt], errors='coerce')
                b = pd.to_numeric(both[bank_amt_col], errors='coerce')
                unrecon_fees = float((a - b).abs().sum())

        if psp_stats:
            n = len(psp_stats)
            preview = ', '.join(psp_stats[:3]) + (f' (+{n-3} more)' if n > 3 else '')
            join_info = f"CRM[{crm_ref_col}] matched {n} PSP(s): {preview}"
        else:
            join_info = f"CRM[{crm_ref_col}] — no PSP reference columns detected"

        with open(STATE_FILE, 'wb') as f:
            pickle.dump({'merged': merged, 'crm_df': crm_df}, f)

        return jsonify({
            "status": "success",
            "summary": {
                "total_crm_rows":  total_crm,
                "total_bank_rows": total_bank_rows,
                "total_matched":   matched,
                "total_orphaned":  crm_only + bank_only,
                "crm_unmatched":   crm_only,
                "bank_unmatched":  bank_only,
                "unrecon_fees":    round(unrecon_fees, 2),
                "join_keys_used":  join_info,
                "deal_no_column":  crm_deal_col or "not detected"
            }
        })

    except Exception as e:
        return jsonify({"status": "error", "summary": {"error": str(e)}}), 500


@app.route('/api/download/lifecycle')
def download_lifecycle():
    """Generate and serve the multi-tab Life Cycle Report matching the historical format."""
    try:
        with open(STATE_FILE, 'rb') as f:
            state = pickle.load(f)
    except FileNotFoundError:
        return jsonify({"error": "No reconciliation data found. Run reconciliation first."}), 400

    try:
        buf = build_lifecycle_excel(state['merged'])
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


@app.route('/api/test', methods=['POST'])
def test_load():
    """Load ALL January 2023 PSP files + CRM from relevant-data and run reconcile.
    Temporary dev endpoint — do not expose in production.
    """
    import shutil
    base = os.path.join(os.path.dirname(__file__), '..', 'relevant-data',
                        'MRS', '2023', '01. Jan. 2023')
    psp_dir   = os.path.join(base, 'PSPs')
    plat_dir  = os.path.join(base, 'platform')

    upload = app.config['UPLOAD_FOLDER']

    # Copy platform files
    shutil.copy(os.path.join(plat_dir, 'CRM Transactions Additional info.xlsx'),
                os.path.join(upload, 'platformFile.xlsx'))
    shutil.copy(os.path.join(plat_dir, 'Client Balance check.xlsx'),
                os.path.join(upload, 'equityFile.xlsx'))
    shutil.copy(os.path.join(plat_dir, 'Deposit and Withdrawal Report.csv'),
                os.path.join(upload, 'transactionsFile.csv'))

    # Clear any old bank files
    for f in os.listdir(upload):
        if f.startswith('bankFile_'):
            os.remove(os.path.join(upload, f))

    # Copy every flat PSP file (skip subdirectories)
    idx = 0
    for fname in sorted(os.listdir(psp_dir)):
        fpath = os.path.join(psp_dir, fname)
        if not os.path.isfile(fpath):
            continue
        ext = os.path.splitext(fname)[1].lower()
        if ext not in ('.csv', '.xlsx', '.xls'):
            continue
        shutil.copy(fpath, os.path.join(upload, f'bankFile_{idx}{ext}'))
        idx += 1

    from flask import current_app
    with current_app.test_request_context('/api/reconcile', method='POST'):
        result = reconcile()

    return result


if __name__ == '__main__':
    print("Starting MRS 2.0 Web GUI...")
    app.run(debug=True, port=5000)
