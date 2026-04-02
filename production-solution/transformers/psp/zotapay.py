"""Zotapay PSP transformer.

Standard CSV format. The 'id' column matches CRM psp_transaction_id.
Note: 'external_transaction_id' is a counterparty reference — not the join key.
"""

import pandas as pd
from transformers.base import PSPTransformer
from transformers.normalize import normalize_key, clean_amount


class ZotapayTransformer(PSPTransformer):
    psp_name = "zotapay"
    file_patterns = ["Zotapay", "zotapay"]

    COLUMN_MAP = {
        "id": "reference_id",
        "order_amount": "amount",
        "order_currency": "currency",
        "created_at": "date",
        "status": "status",
    }

    def transform(self, raw_df):
        result = pd.DataFrame()
        for raw_col, clean_col in self.COLUMN_MAP.items():
            if raw_col in raw_df.columns:
                result[clean_col] = raw_df[raw_col]
            else:
                result[clean_col] = None

        result["fee"] = None
        result["reference_id"] = normalize_key(result["reference_id"])
        result["amount"] = result["amount"].apply(clean_amount)
        result["date"] = pd.to_datetime(result["date"], errors="coerce")
        return result
