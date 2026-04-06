"""Generic config-driven PSP transformer.

For simple CSV/Excel PSPs where the only difference is column names.
Reads mapping from the psp_schema_registry DB table. Adding a new simple PSP
requires zero code — just insert a row in the registry.

Usage:
    transformer = GenericPSPTransformer("eftpay", registry_entry)
    clean_df = transformer.load("EFTpay_Jan2023.csv")
"""

import pandas as pd
from transformers.base import PSPTransformer
from transformers.normalize import normalize_key, clean_amount

_HEADER_KEYWORDS = {'date', 'amount', 'id', 'reference', 'transaction',
                    'currency', 'type', 'status', 'payment', 'method',
                    'name', 'result', 'email', 'order', 'fee'}


class GenericPSPTransformer(PSPTransformer):
    """Config-driven transformer that reads column mappings from a registry entry."""

    def __init__(self, psp_name: str, ref_column: str, amount_column: str = None,
                 currency_column: str = None, date_column: str = None,
                 status_column: str = None, fee_column: str = None,
                 skiprows: int = 0, file_patterns: list[str] = None,
                 date_format: str = None):
        self._psp_name = psp_name
        self._file_patterns = file_patterns or [psp_name]
        self._ref_column = ref_column
        self._amount_column = amount_column
        self._currency_column = currency_column
        self._date_column = date_column
        self._status_column = status_column
        self._fee_column = fee_column
        self._skiprows = skiprows
        self._date_format = date_format

    @property
    def psp_name(self) -> str:
        return self._psp_name

    @property
    def file_patterns(self) -> list[str]:
        return self._file_patterns

    def extract(self, filepath):
        """Load file, handling skiprows for PSPs with metadata headers."""
        import os
        ext = os.path.splitext(filepath)[1].lower()
        try:
            if ext in ('.xlsx', '.xls'):
                if self._skiprows:
                    # Try loading with skiprows first
                    df = pd.read_excel(filepath, skiprows=self._skiprows, dtype=str)
                    if not df.empty:
                        return df
                df = pd.read_excel(filepath, dtype=str)
                # Auto-detect metadata header
                if self._skiprows == 0:
                    unnamed = sum(1 for c in df.columns if str(c).startswith('Unnamed'))
                    if unnamed > len(df.columns) * 0.5:
                        raw = pd.read_excel(filepath, header=None, dtype=str, nrows=20)
                        for i in range(len(raw)):
                            vals = [v for v in raw.iloc[i].tolist() if pd.notna(v)]
                            hits = sum(1 for v in vals
                                       if any(k in str(v).lower() for k in _HEADER_KEYWORDS))
                            if hits >= 3:
                                df = pd.read_excel(filepath, skiprows=i, dtype=str)
                                break
                return df
            else:
                return super().extract(filepath)
        except Exception:
            return None

    def transform(self, raw_df):
        result = pd.DataFrame()

        col_map = {
            self._ref_column: "reference_id",
            self._amount_column: "amount",
            self._currency_column: "currency",
            self._date_column: "date",
            self._status_column: "status",
            self._fee_column: "fee",
        }

        for raw_col, clean_col in col_map.items():
            if raw_col and raw_col in raw_df.columns:
                result[clean_col] = raw_df[raw_col]
            elif clean_col not in result.columns:
                result[clean_col] = None

        result["reference_id"] = normalize_key(result["reference_id"])
        result["amount"] = result["amount"].apply(clean_amount)
        if "fee" in result.columns:
            result["fee"] = result["fee"].apply(clean_amount)
        result["date"] = pd.to_datetime(result["date"],
                                         format=self._date_format,
                                         errors="coerce")
        return result

    @classmethod
    def from_registry_row(cls, row):
        """Create a GenericPSPTransformer from a PSPSchemaRegistry ORM object."""
        return cls(
            psp_name=row.psp_name,
            ref_column=row.ref_column,
            amount_column=row.amount_column,
            currency_column=row.currency_column,
            date_column=row.date_column,
            status_column=row.status_column,
            fee_column=row.fee_column,
            skiprows=row.skiprows or 0,
            file_patterns=[row.psp_name],
            date_format=row.date_format,
        )
