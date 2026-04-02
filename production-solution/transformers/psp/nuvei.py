"""Nuvei (SafeCharge) PSP transformer.

Handles Excel files with ~11 metadata rows before the real header.
19-digit SafeCharge IDs must be read as strings to avoid float64 precision loss.
"""

import pandas as pd
from transformers.base import PSPTransformer
from transformers.normalize import normalize_key, clean_amount

_HEADER_KEYWORDS = {'date', 'amount', 'id', 'reference', 'transaction',
                    'currency', 'type', 'status', 'payment', 'method',
                    'name', 'result', 'email', 'order', 'fee'}


class NuveiTransformer(PSPTransformer):
    psp_name = "nuvei"
    file_patterns = ["Nuvei", "SafeCharge"]

    COLUMN_MAP = {
        "Transaction ID": "reference_id",
        "Amount": "amount",
        "Currency": "currency",
        "Transaction Date": "date",
        "Status": "status",
        "Processing Fee": "fee",
    }

    def extract(self, filepath):
        """Load Excel with metadata row detection and dtype=str for ID precision."""
        try:
            df = pd.read_excel(filepath, dtype=str)

            # Check if the first row looks like metadata instead of real headers
            unnamed = sum(1 for c in df.columns if str(c).startswith('Unnamed'))
            first_col = str(df.columns[0]) if len(df.columns) else ''
            is_metadata = (unnamed > len(df.columns) * 0.5
                           or any(k in first_col.lower() for k in ('report', 'cpanel', 'generated')))

            if is_metadata:
                raw = pd.read_excel(filepath, header=None, dtype=str, nrows=20)
                for i in range(len(raw)):
                    vals = [v for v in raw.iloc[i].tolist() if pd.notna(v)]
                    hits = sum(1 for v in vals
                               if any(k in str(v).lower() for k in _HEADER_KEYWORDS))
                    if hits >= 3:
                        df = pd.read_excel(filepath, skiprows=i, dtype=str)
                        break

            return df
        except Exception:
            return None

    def transform(self, raw_df):
        result = pd.DataFrame()
        for raw_col, clean_col in self.COLUMN_MAP.items():
            if raw_col in raw_df.columns:
                result[clean_col] = raw_df[raw_col]
            else:
                result[clean_col] = None

        result["reference_id"] = normalize_key(result["reference_id"])
        result["amount"] = result["amount"].apply(clean_amount)
        result["fee"] = result["fee"].apply(clean_amount)
        result["date"] = pd.to_datetime(result["date"], errors="coerce")
        return result
