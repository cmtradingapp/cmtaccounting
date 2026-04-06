"""Abstract base classes for PSP and Bank statement transformers.

Each PSP/Bank gets a concrete subclass that knows:
- Its canonical name and filename patterns
- How to extract raw data from its file format (CSV, Excel, PDF)
- How to transform raw columns into the standard clean schema

The clean schema for PSPs:   reference_id, amount, currency, fee, status, date
The clean schema for Banks:  reference, client_id, amount, currency, date, description
"""

import os
import csv
from abc import ABC, abstractmethod
from typing import Optional

import pandas as pd


class PSPTransformer(ABC):
    """Base class for all PSP statement transformers."""

    @property
    @abstractmethod
    def psp_name(self) -> str:
        """Canonical PSP name (lowercase, e.g. 'nuvei', 'korapay')."""
        ...

    @property
    @abstractmethod
    def file_patterns(self) -> list[str]:
        """Case-insensitive substrings to match against filenames."""
        ...

    @abstractmethod
    def transform(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Map raw columns to the standard clean PSP schema.

        Must return a DataFrame with columns:
            reference_id, amount, currency, fee, status, date
        """
        ...

    def extract(self, filepath: str) -> Optional[pd.DataFrame]:
        """Load raw file into a DataFrame. Handles CSV and Excel by default.

        Override in subclasses that need custom loading (e.g. skiprows, PDF).
        """
        ext = os.path.splitext(filepath)[1].lower()
        try:
            if ext in ('.xlsx', '.xls'):
                return pd.read_excel(filepath, dtype=str)
            else:
                try:
                    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                        dialect = csv.Sniffer().sniff(f.read(4096))
                    return pd.read_csv(filepath, dialect=dialect, encoding='utf-8',
                                       encoding_errors='replace', dtype=str)
                except csv.Error:
                    return pd.read_csv(filepath, encoding='utf-8',
                                       encoding_errors='replace', dtype=str)
        except Exception:
            return None

    def load(self, filepath: str) -> Optional[pd.DataFrame]:
        """Full ETL: extract → transform. Returns clean DataFrame or None."""
        raw = self.extract(filepath)
        if raw is None or raw.empty:
            return None
        return self.transform(raw)

    def matches_file(self, filename: str) -> bool:
        """Check if this transformer handles the given filename."""
        name_lower = filename.lower()
        return any(p.lower() in name_lower for p in self.file_patterns)


class BankTransformer(ABC):
    """Base class for all bank statement transformers."""

    @property
    @abstractmethod
    def bank_name(self) -> str:
        """Canonical bank name (lowercase, e.g. 'nedbank', 'standard_bank')."""
        ...

    @property
    @abstractmethod
    def file_patterns(self) -> list[str]:
        """Case-insensitive substrings to match against filenames."""
        ...

    @abstractmethod
    def transform(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        """Map raw columns to the standard clean bank schema.

        Must return a DataFrame with columns:
            reference, client_id, amount, currency, date, description
        """
        ...

    def extract(self, filepath: str) -> Optional[pd.DataFrame]:
        """Load raw file. Default handles CSV; override for PDF/Excel."""
        ext = os.path.splitext(filepath)[1].lower()
        try:
            if ext in ('.xlsx', '.xls'):
                return pd.read_excel(filepath, dtype=str)
            elif ext == '.csv':
                try:
                    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                        dialect = csv.Sniffer().sniff(f.read(4096))
                    return pd.read_csv(filepath, dialect=dialect, encoding='utf-8',
                                       encoding_errors='replace', dtype=str)
                except csv.Error:
                    return pd.read_csv(filepath, encoding='utf-8',
                                       encoding_errors='replace', dtype=str)
            else:
                return None
        except Exception:
            return None

    def load(self, filepath: str) -> Optional[pd.DataFrame]:
        """Full ETL: extract → transform. Returns clean DataFrame or None."""
        raw = self.extract(filepath)
        if raw is None or raw.empty:
            return None
        return self.transform(raw)

    def matches_file(self, filename: str) -> bool:
        """Check if this transformer handles the given filename."""
        name_lower = filename.lower()
        return any(p.lower() in name_lower for p in self.file_patterns)
