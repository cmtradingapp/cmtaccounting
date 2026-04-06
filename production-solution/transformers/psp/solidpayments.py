"""SolidPayments PSP transformer.

Key edge case: SolidPayments has both TransactionId and UniqueId columns.
UniqueId has much higher overlap with CRM keys (~79 matches vs ~12).
This transformer uses UniqueId as the reference column.
"""

import pandas as pd
from transformers.base import PSPTransformer
from transformers.normalize import normalize_key, clean_amount


class SolidPaymentsTransformer(PSPTransformer):
    psp_name = "solidpayments"
    file_patterns = ["SolidPayments", "Solidpayments", "solidpayments"]

    COLUMN_MAP = {
        "UniqueId": "reference_id",
        "Debit": "amount",
        "Currency": "currency",
        "RequestTimestamp": "date",
        "Status": "status",
    }

    def transform(self, raw_df):
        result = pd.DataFrame()
        for raw_col, clean_col in self.COLUMN_MAP.items():
            if raw_col in raw_df.columns:
                result[clean_col] = raw_df[raw_col]
            else:
                result[clean_col] = None

        # Fallback: if UniqueId not found, try TransactionId
        if "reference_id" not in result.columns or result["reference_id"].isna().all():
            if "TransactionId" in raw_df.columns:
                result["reference_id"] = raw_df["TransactionId"]

        result["fee"] = None
        result["reference_id"] = normalize_key(result["reference_id"])
        result["amount"] = result["amount"].apply(clean_amount)
        result["date"] = pd.to_datetime(result["date"], errors="coerce")
        return result
