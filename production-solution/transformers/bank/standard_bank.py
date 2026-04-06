"""Standard Bank ZAR statement transformer.

Handles PDF statements with fixed-column text layout. Each transaction occupies
two lines:
  Line 1: {page} {description} {svc_fee} {debit} {credit} {YYYYMMDD} {balance}
  Line 2: {reference / client ID}

Client IDs (MT4 login numbers) are extracted from the reference line via regex.

Ported from server.py _load_pdf_file() Strategy 2 (lines 440-481).
"""

import re
import pandas as pd
from transformers.base import BankTransformer
from transformers.normalize import normalize_key, clean_amount

try:
    import pdfplumber
    _PDF_SUPPORT = True
except ImportError:
    pdfplumber = None
    _PDF_SUPPORT = False

_TXN_RE = re.compile(
    r'^\d+\s+(.+?)\s+'               # page + description
    r'([\d,]+\.\d{2})\s+'            # service fee
    r'(-?[\d,]+\.\d{2})\s+'          # debit (may be negative)
    r'([\d,]+\.\d{2})\s+'            # credit
    r'(\d{8})\s+'                    # date YYYYMMDD
    r'([\d,]+\.\d{2})\s*$'           # running balance
)
_CLIENT_ID_RE = re.compile(r'\b(1[34]\d{6,8})\b')


class StandardBankTransformer(BankTransformer):
    bank_name = "standard_bank"
    file_patterns = ["Standard", "standard"]

    def extract(self, filepath):
        """Extract transactions from Standard Bank ZAR PDF statements."""
        import os
        ext = os.path.splitext(filepath)[1].lower()

        if ext != '.pdf':
            return super().extract(filepath)

        if not _PDF_SUPPORT:
            return None

        try:
            with pdfplumber.open(filepath) as pdf:
                all_text = []
                for page in pdf.pages:
                    txt = page.extract_text()
                    if txt:
                        all_text.append(txt)

            full_text = '\n'.join(all_text)
            lines = full_text.split('\n')
            rows = []
            i = 0

            while i < len(lines) - 1:
                m = _TXN_RE.match(lines[i].strip())
                if m:
                    desc, svcfee, debit, credit, date, balance = m.groups()
                    # Skip header / balance-forward lines
                    if not any(k in desc.upper() for k in
                               ('BALANCE BROUGHT', 'END OF REPORT', 'SERVICE FEE')):
                        ref_line = lines[i + 1].strip()
                        cid_m = _CLIENT_ID_RE.search(ref_line)
                        rows.append({
                            'date': date,
                            'description': desc.strip(),
                            'debit': debit,
                            'credit': credit,
                            'balance': balance,
                            'reference': ref_line,
                            'client_id': cid_m.group(1) if cid_m else None,
                        })
                    i += 2
                else:
                    i += 1

            if rows:
                return pd.DataFrame(rows)
            return None

        except Exception:
            return None

    def transform(self, raw_df):
        result = pd.DataFrame()

        result['reference'] = raw_df.get('reference', pd.Series(dtype=str))
        result['description'] = raw_df.get('description', pd.Series(dtype=str))
        result['client_id'] = normalize_key(raw_df.get('client_id', pd.Series(dtype=str)))

        # Determine amount: use credit if non-zero, otherwise debit (as negative)
        def _resolve_amount(row):
            credit = clean_amount(row.get('credit'))
            debit = clean_amount(row.get('debit'))
            if credit and credit != 0:
                return credit
            if debit and debit != 0:
                return -abs(debit)
            return 0.0

        result['amount'] = raw_df.apply(_resolve_amount, axis=1)
        result['date'] = pd.to_datetime(raw_df.get('date'), format='%Y%m%d', errors='coerce')
        result['currency'] = 'ZAR'

        return result
