"""Tests for PSP and Bank transformers — verify correct clean schema output."""

import os
import sys
import pytest
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transformers.normalize import normalize_key, normalize_key_scalar, clean_amount, resolve_currency


# ── Normalize functions ─────────────────────────────────────────────────────

class TestNormalizeKey:
    def test_strip_whitespace(self):
        s = pd.Series(["  ABC  ", " 123 "])
        result = normalize_key(s)
        assert result.tolist() == ["ABC", "123"]

    def test_remove_float_suffix(self):
        s = pd.Series(["39120162.0", "12345.00"])
        result = normalize_key(s)
        assert result.tolist() == ["39120162", "12345"]

    def test_strip_leading_zeros(self):
        s = pd.Series(["0012345", "001"])
        result = normalize_key(s)
        assert result.tolist() == ["12345", "1"]

    def test_uppercase(self):
        s = pd.Series(["abc123"])
        result = normalize_key(s)
        assert result.tolist() == ["ABC123"]

    def test_null_handling(self):
        s = pd.Series(["nan", "None", "", "NaT"])
        result = normalize_key(s)
        assert result.tolist() == [None, None, None, None]

    def test_scalar(self):
        assert normalize_key_scalar("  39120162.0  ") == "39120162"
        assert normalize_key_scalar("0012345") == "12345"
        assert normalize_key_scalar(None) is None
        assert normalize_key_scalar("nan") is None


class TestCleanAmount:
    def test_normal(self):
        assert clean_amount("100.50") == 100.50

    def test_with_commas(self):
        assert clean_amount("2,321,000.00") == 2321000.0

    def test_none(self):
        assert clean_amount(None) is None

    def test_invalid(self):
        assert clean_amount("N/A") is None


class TestResolveCurrency:
    def test_known_ids(self):
        s = pd.Series([1, 5, 11])
        result = resolve_currency(s)
        assert result.tolist() == ["USD", "ZAR", "USDT"]

    def test_unknown_id(self):
        s = pd.Series([99])
        result = resolve_currency(s)
        assert result.tolist() == ["99"]


# ── PSP Transformers ────────────────────────────────────────────────────────

class TestNuveiTransformer:
    def test_transform_basic(self):
        from transformers.psp.nuvei import NuveiTransformer
        t = NuveiTransformer()
        assert t.psp_name == "nuvei"
        assert t.matches_file("Nuvei Jan 2023.xlsx")
        assert t.matches_file("SafeCharge_report.xlsx")
        assert not t.matches_file("Korapay.csv")

    def test_transform_dataframe(self):
        from transformers.psp.nuvei import NuveiTransformer
        t = NuveiTransformer()
        raw = pd.DataFrame({
            "Transaction ID": ["1130000004097387874", "1130000004097387875"],
            "Amount": ["4800", "250.50"],
            "Currency": ["USD", "EUR"],
            "Transaction Date": ["2023-01-15", "2023-01-16"],
            "Status": ["Approved", "Approved"],
            "Processing Fee": ["10", "5.50"],
        })
        clean = t.transform(raw)
        assert "reference_id" in clean.columns
        assert "amount" in clean.columns
        assert clean["reference_id"].iloc[0] == "1130000004097387874"
        assert clean["amount"].iloc[0] == 4800.0


class TestKorapayTransformer:
    def test_transform_dataframe(self):
        from transformers.psp.korapay import KorapayTransformer
        t = KorapayTransformer()
        raw = pd.DataFrame({
            "payment_reference": ["1065583320"],
            "amount_paid": ["19355.00"],
            "currency": ["NGN"],
            "transaction_date": ["2023-01-31 21:34:39"],
            "status": ["success"],
            "fee": ["600.00"],
        })
        clean = t.transform(raw)
        assert clean["reference_id"].iloc[0] == "1065583320"
        assert clean["amount"].iloc[0] == 19355.0


class TestZotapayTransformer:
    def test_transform_dataframe(self):
        from transformers.psp.zotapay import ZotapayTransformer
        t = ZotapayTransformer()
        raw = pd.DataFrame({
            "id": ["ZP12345"],
            "order_amount": ["500"],
            "order_currency": ["USD"],
            "created_at": ["2023-01-10"],
            "status": ["completed"],
        })
        clean = t.transform(raw)
        assert clean["reference_id"].iloc[0] == "ZP12345"


class TestSolidPaymentsTransformer:
    def test_uses_unique_id(self):
        from transformers.psp.solidpayments import SolidPaymentsTransformer
        t = SolidPaymentsTransformer()
        raw = pd.DataFrame({
            "UniqueId": ["SP001"],
            "TransactionId": ["TXN001"],
            "Debit": ["100"],
            "Currency": ["USD"],
            "RequestTimestamp": ["2023-01-10"],
            "Status": ["Completed"],
        })
        clean = t.transform(raw)
        # Should use UniqueId, not TransactionId
        assert clean["reference_id"].iloc[0] == "SP001"


class TestGenericTransformer:
    def test_from_config(self):
        from transformers.psp.generic import GenericPSPTransformer
        t = GenericPSPTransformer(
            psp_name="test_psp",
            ref_column="ref_id",
            amount_column="amt",
            currency_column="ccy",
            date_column="dt",
        )
        raw = pd.DataFrame({
            "ref_id": ["REF001"],
            "amt": ["500"],
            "ccy": ["USD"],
            "dt": ["2023-01-10"],
        })
        clean = t.transform(raw)
        assert clean["reference_id"].iloc[0] == "REF001"
        assert clean["amount"].iloc[0] == 500.0


# ── Bank Transformers ───────────────────────────────────────────────────────

class TestNedbankTransformer:
    def test_file_matching(self):
        from transformers.bank.nedbank import NedbankTransformer
        t = NedbankTransformer()
        assert t.matches_file("Nedbank Client Funds January.csv")
        assert not t.matches_file("Standard Bank ZAR.pdf")


class TestStandardBankTransformer:
    def test_file_matching(self):
        from transformers.bank.standard_bank import StandardBankTransformer
        t = StandardBankTransformer()
        assert t.matches_file("Standard Bank ZAR Statement.pdf")
        assert not t.matches_file("Nedbank.csv")
