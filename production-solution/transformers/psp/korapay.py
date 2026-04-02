"""Korapay PSP transformer.

Korapay CSV files use payment_reference as the join key, which matches CRM's
transactionid (not psp_transaction_id). This is the key edge case this
transformer demonstrates.
"""

import pandas as pd
from transformers.base import PSPTransformer
from transformers.normalize import normalize_key, clean_amount


class KorapayTransformer(PSPTransformer):
    psp_name = "korapay"
    file_patterns = ["Korapay", "korapay"]

    COLUMN_MAP = {
        "payment_reference": "reference_id",
        "amount_paid": "amount",
        "currency": "currency",
        "transaction_date": "date",
        "status": "status",
        "fee": "fee",
    }

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
