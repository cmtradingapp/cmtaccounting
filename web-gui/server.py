import os
import csv
import io
import pickle
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_file
import requests as http_requests
import pandas as pd

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

STATE_FILE = os.path.join(app.config['UPLOAD_FOLDER'], '_recon_state.pkl')

# The 52 output columns matching the historical Lifecycle List spec
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
    """Return the first found column from merged df, checking _crm suffix first."""
    for name in candidates:
        for variant in [name, f"{name}_crm", f"{name}_bank"]:
            if variant in merged.columns:
                return merged[variant]
    return pd.Series([None] * len(merged), index=merged.index)


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
    """Real reconciliation engine: joins Bank/PSP references against CRM data."""
    upload = app.config['UPLOAD_FOLDER']

    try:
        bank_paths = []
        crm_path = None
        for f in sorted(os.listdir(upload)):
            if f.startswith('bankFile_'): bank_paths.append(os.path.join(upload, f))
            if f.startswith('platformFile'): crm_path = os.path.join(upload, f)

        if not bank_paths or not crm_path:
            return jsonify({"status": "error", "summary": {"error": "Missing uploaded files. Please re-upload."}}), 400

        bank_frames = []
        for bp in bank_paths:
            ext = os.path.splitext(bp)[1].lower()
            try:
                if ext in ('.xlsx', '.xls'):
                    df = pd.read_excel(bp)
                else:
                    try:
                        with open(bp, 'r', encoding='utf-8', errors='replace') as f:
                            dialect = csv.Sniffer().sniff(f.read(4096))
                        df = pd.read_csv(bp, dialect=dialect)
                    except Exception:
                        df = pd.read_csv(bp)
                bank_frames.append(df)
            except Exception:
                continue

        if not bank_frames:
            return jsonify({"status": "error", "summary": {"error": "Could not parse any bank files."}}), 400

        bank_df = pd.concat(bank_frames, ignore_index=True)

        ext = os.path.splitext(crm_path)[1].lower()
        if ext in ('.xlsx', '.xls'):
            crm_df = pd.read_excel(crm_path)
        else:
            try:
                with open(crm_path, 'r', encoding='utf-8', errors='replace') as f:
                    dialect = csv.Sniffer().sniff(f.read(4096))
                crm_df = pd.read_csv(crm_path, dialect=dialect)
            except Exception:
                crm_df = pd.read_csv(crm_path)

        bank_cols_lower = {c: c.lower().strip() for c in bank_df.columns}
        crm_cols_lower = {c: c.lower().strip() for c in crm_df.columns}

        bank_ref_col = None
        for col, low in bank_cols_lower.items():
            if any(k in low for k in ['transactionreference', 'reference', 'order no', 'transaction_id', 'trans_id']):
                bank_ref_col = col
                break

        crm_ref_col = None
        for col, low in crm_cols_lower.items():
            if any(k in low for k in ['psp_transaction_id', 'transactionreference', 'reference', 'psp_id']):
                crm_ref_col = col
                break

        crm_deal_col = None
        for col, low in crm_cols_lower.items():
            if any(k in low for k in ['mtorder_id', 'deal_no', 'deal no', 'dealid', 'order_id']):
                crm_deal_col = col
                break

        bank_amount_col = None
        for col, low in bank_cols_lower.items():
            if any(k in low for k in ['baseamount', 'amount', 'settlebaseamount', 'gross']):
                bank_amount_col = col
                break

        crm_amount_col = None
        for col, low in crm_cols_lower.items():
            if any(k in low for k in ['amount', 'requested_amount', 'gross']):
                crm_amount_col = col
                break

        if bank_ref_col and crm_ref_col:
            bank_df['_join_key'] = normalize_key(bank_df[bank_ref_col])
            crm_df['_join_key'] = normalize_key(crm_df[crm_ref_col])

            merged = crm_df.merge(bank_df, on='_join_key', how='outer',
                                  suffixes=('_crm', '_bank'), indicator=True)

            total_crm = len(crm_df)
            total_bank = len(bank_df)
            matched = int((merged['_merge'] == 'both').sum())
            crm_only = int((merged['_merge'] == 'left_only').sum())
            bank_only = int((merged['_merge'] == 'right_only').sum())

            unrecon_fees = 0.0
            if bank_amount_col and crm_amount_col:
                both = merged[merged['_merge'] == 'both'].copy()
                bc = bank_amount_col if bank_amount_col in both.columns else f"{bank_amount_col}_bank"
                cc = crm_amount_col if crm_amount_col in both.columns else f"{crm_amount_col}_crm"
                if bc in both.columns and cc in both.columns:
                    both[bc] = pd.to_numeric(both[bc], errors='coerce')
                    both[cc] = pd.to_numeric(both[cc], errors='coerce')
                    both['_diff'] = (both[bc] - both[cc]).abs()
                    unrecon_fees = float(both['_diff'].sum())

            # Persist merged state so download endpoints can use it without re-running
            with open(STATE_FILE, 'wb') as f:
                pickle.dump({
                    'merged': merged, 'crm_df': crm_df, 'bank_df': bank_df,
                    'bank_ref_col': bank_ref_col, 'crm_ref_col': crm_ref_col,
                    'bank_amount_col': bank_amount_col, 'crm_amount_col': crm_amount_col,
                }, f)

            return jsonify({
                "status": "success",
                "summary": {
                    "total_crm_rows": total_crm,
                    "total_bank_rows": total_bank,
                    "total_matched": matched,
                    "total_orphaned": crm_only + bank_only,
                    "crm_unmatched": crm_only,
                    "bank_unmatched": bank_only,
                    "unrecon_fees": round(unrecon_fees, 2),
                    "join_keys_used": f"Bank[{bank_ref_col}] ↔ CRM[{crm_ref_col}]",
                    "deal_no_column": crm_deal_col or "not detected"
                }
            })
        else:
            merged = crm_df.copy()
            merged['_merge'] = 'left_only'
            with open(STATE_FILE, 'wb') as f:
                pickle.dump({
                    'merged': merged, 'crm_df': crm_df, 'bank_df': bank_df,
                    'bank_ref_col': None, 'crm_ref_col': None,
                    'bank_amount_col': bank_amount_col, 'crm_amount_col': crm_amount_col,
                }, f)
            return jsonify({
                "status": "success",
                "summary": {
                    "total_crm_rows": len(crm_df),
                    "total_bank_rows": len(bank_df),
                    "total_matched": 0,
                    "total_orphaned": len(crm_df) + len(bank_df),
                    "crm_unmatched": len(crm_df),
                    "bank_unmatched": len(bank_df),
                    "unrecon_fees": 0,
                    "join_keys_used": f"FAILED: Bank ref col={bank_ref_col}, CRM ref col={crm_ref_col}",
                    "deal_no_column": crm_deal_col or "not detected"
                }
            })
    except Exception as e:
        return jsonify({"status": "error", "summary": {"error": str(e)}}), 500


@app.route('/api/download/lifecycle')
def download_lifecycle():
    """Generate and serve Lifecycle List.xlsx with the full 52-column output schema."""
    try:
        with open(STATE_FILE, 'rb') as f:
            state = pickle.load(f)
    except FileNotFoundError:
        return jsonify({"error": "No reconciliation data found. Run reconciliation first."}), 400

    try:
        df = build_lifecycle_df(state['merged'])
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Lifecycle List', index=False)
            ws = writer.sheets['Lifecycle List']
            for col_cells in ws.columns:
                max_len = max((len(str(cell.value)) if cell.value is not None else 0) for cell in col_cells)
                ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 2, 30)
        buf.seek(0)
        filename = f"Lifecycle List {datetime.now().strftime('%Y-%m-%d')}.xlsx"
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


if __name__ == '__main__':
    print("Starting MRS 2.0 Web GUI...")
    app.run(debug=True, port=5000)
