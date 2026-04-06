"""Nedbank bank statement transformer.

Nedbank CSVs have a messy header format with metadata rows at the top:
    Statement Enquiry
    Account Number : ,1053484658
    Account Description : ,Client funds
    1935,03/01/2023,BROUGHT FORWARD,0,2647971.89,,

Transactions use a fixed-column format. Client IDs (login numbers starting
with 13 or 14) are often embedded in the reference text.
"""

import re
import pandas as pd
from transformers.base import BankTransformer
from transformers.normalize import normalize_key, clean_amount

# Matches client login numbers (MT4 accounts start with 13xxxxx or 14xxxxx)
_CLIENT_ID_RE = re.compile(r'\b(1[34]\d{6,8})\b')


class NedbankTransformer(BankTransformer):
    bank_name = "nedbank"
    file_patterns = ["Nedbank", "nedbank"]

    def extract(self, filepath):
        """Load Nedbank CSV with metadata header detection."""
        try:
            # Read all lines to find where actual data starts
            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                lines = f.readlines()

            # Find the first line that looks like transaction data (starts with digits)
            data_start = 0
            for i, line in enumerate(lines):
                stripped = line.strip()
                if stripped and stripped[0].isdigit() and ',' in stripped:
                    # Check if this line has enough fields to be a transaction
                    parts = stripped.split(',')
                    if len(parts) >= 4:
                        data_start = i
                        break

            # Read CSV from the data start, no header (these files have no column header row)
            df = pd.read_csv(filepath, skiprows=data_start, header=None, dtype=str,
                             encoding='utf-8', encoding_errors='replace')

            # Assign column names based on known Nedbank format:
            # branch, date, description, amount, balance, (optional extra cols)
            col_names = ['branch', 'date', 'description', 'amount', 'balance']
            if len(df.columns) > len(col_names):
                col_names += [f'extra_{i}' for i in range(len(df.columns) - len(col_names))]
            df.columns = col_names[:len(df.columns)]

            return df
        except Exception:
            return None

    def transform(self, raw_df):
        result = pd.DataFrame()

        result['description'] = raw_df.get('description', pd.Series(dtype=str))
        result['amount'] = raw_df.get('amount', pd.Series(dtype=str)).apply(clean_amount)
        result['date'] = pd.to_datetime(raw_df.get('date'), format='%d/%m/%Y', errors='coerce')
        result['currency'] = 'ZAR'  # Nedbank Client Funds is always ZAR
        result['reference'] = raw_df.get('description', pd.Series(dtype=str))

        # Extract client ID from description
        def _extract_client_id(desc):
            if not desc or pd.isna(desc):
                return None
            m = _CLIENT_ID_RE.search(str(desc))
            return m.group(1) if m else None

        result['client_id'] = result['description'].apply(_extract_client_id)
        result['client_id'] = normalize_key(result['client_id'])

        # Filter out non-transaction rows (BROUGHT FORWARD, etc.)
        skip_patterns = ['BROUGHT FORWARD', 'BALANCE CARRIED', 'SERVICE FEE']
        mask = ~result['description'].str.upper().str.contains(
            '|'.join(skip_patterns), na=False
        )
        result = result[mask].reset_index(drop=True)

        return result
