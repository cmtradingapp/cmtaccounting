"""Create all tables and seed the PSP schema registry."""

import os
import csv
from db.engine import engine, get_session
from db.models import Base, PSPSchemaRegistry


def create_tables():
    """Create all tables defined in models.py."""
    Base.metadata.create_all(engine)


# Known PSP column mappings — derived from analysis of all PSP statement formats.
# Each entry maps the PSP's raw column names to the clean schema.
_SEED_REGISTRY = [
    {
        "psp_name": "nuvei",
        "ref_column": "Transaction ID",
        "amount_column": "Amount",
        "currency_column": "Currency",
        "date_column": "Transaction Date",
        "status_column": "Status",
        "fee_column": "Processing Fee",
        "skiprows": 11,
        "notes": "Excel with 11 metadata rows; 19-digit SafeCharge IDs — must read as str",
    },
    {
        "psp_name": "korapay",
        "ref_column": "payment_reference",
        "amount_column": "amount_paid",
        "currency_column": "currency",
        "date_column": "transaction_date",
        "status_column": "status",
        "fee_column": "fee",
        "notes": "Joins on CRM transactionid (not psp_transaction_id)",
    },
    {
        "psp_name": "zotapay",
        "ref_column": "id",
        "amount_column": "order_amount",
        "currency_column": "order_currency",
        "date_column": "created_at",
        "status_column": "status",
        "fee_column": None,
        "notes": "Standard CSV; id column matches CRM psp_transaction_id",
    },
    {
        "psp_name": "solidpayments",
        "ref_column": "UniqueId",
        "amount_column": "Debit",
        "currency_column": "Currency",
        "date_column": "RequestTimestamp",
        "status_column": "Status",
        "fee_column": None,
        "notes": "UniqueId has higher overlap than TransactionId",
    },
    {
        "psp_name": "finrax",
        "ref_column": "Transaction ID",
        "amount_column": "Transfer Amount",
        "currency_column": "Transfer Currency",
        "date_column": "Created At (UTC)",
        "status_column": None,
        "fee_column": "Balance Fee",
        "notes": "Crypto PSP",
    },
    {
        "psp_name": "directa24",
        "ref_column": "Invoice",
        "amount_column": "Amount (USD)",
        "currency_column": "Currency",
        "date_column": "Creation Date",
        "status_column": "Status",
        "fee_column": None,
        "notes": "LATAM PSP; amounts may have commas",
    },
    {
        "psp_name": "eftpay",
        "ref_column": "merchant_reference",
        "amount_column": "amount",
        "currency_column": "currency",
        "date_column": "date",
        "status_column": "status",
        "fee_column": "fee",
        "notes": "Standard CSV",
    },
    {
        "psp_name": "virtualpay",
        "ref_column": "Transaction Number",
        "amount_column": "Amount",
        "currency_column": "Currency",
        "date_column": "Date",
        "status_column": "Status",
        "fee_column": None,
        "notes": "VP Deposits format",
    },
    {
        "psp_name": "skrill",
        "ref_column": "ID",
        "amount_column": "Amount Sent",
        "currency_column": "Currency Sent",
        "date_column": "Time (CET)",
        "status_column": "Status",
        "fee_column": None,
        "notes": "Bare ID column as reference",
    },
    {
        "psp_name": "neteller",
        "ref_column": "ID",
        "amount_column": "Amount Sent",
        "currency_column": "Currency Sent",
        "date_column": "Time (CET)",
        "status_column": "Status",
        "fee_column": None,
        "notes": "Same format group as Skrill",
    },
    {
        "psp_name": "ozow",
        "ref_column": "id",
        "amount_column": "amount",
        "currency_column": "currencyCode",
        "date_column": "dateCreated",
        "status_column": "statusGroupName",
        "fee_column": None,
        "notes": "Bare id column",
    },
    {
        "psp_name": "swiffy",
        "ref_column": "merchant_reference",
        "amount_column": "amount",
        "currency_column": "currency",
        "date_column": "date",
        "status_column": "status",
        "fee_column": "fee",
        "notes": "Same format as EFTpay",
    },
    {
        "psp_name": "trustpayments",
        "ref_column": "transactionreference",
        "amount_column": "baseamount",
        "currency_column": "currencyiso3a",
        "date_column": "settleduedate",
        "status_column": "settlestatus",
        "fee_column": None,
        "notes": "Settlement batches — may have multiple rows per CRM transaction",
    },
    {
        "psp_name": "astropay",
        "ref_column": "merchant_external_id",
        "amount_column": "amount",
        "currency_column": "currency",
        "date_column": "created_at",
        "status_column": "status",
        "fee_column": None,
        "notes": "LATAM PSP",
    },
    {
        "psp_name": "letknow",
        "ref_column": "Transaction ID",
        "amount_column": "Amount",
        "currency_column": "Currency",
        "date_column": "Date",
        "status_column": "Status",
        "fee_column": None,
        "notes": "Standard format",
    },
    {
        "psp_name": "inatec",
        "ref_column": "Transaction ID",
        "amount_column": "Amount",
        "currency_column": "Currency",
        "date_column": "Date",
        "status_column": "Status",
        "fee_column": None,
        "notes": "Also known as PowerCash",
    },
]


def seed_registry():
    """Insert default PSP schema registry entries (skip duplicates)."""
    session = get_session()
    try:
        for entry in _SEED_REGISTRY:
            existing = session.query(PSPSchemaRegistry).filter_by(
                psp_name=entry["psp_name"]
            ).first()
            if not existing:
                session.add(PSPSchemaRegistry(**entry))
        session.commit()
    finally:
        session.close()


def init_db():
    """Full DB initialization: create tables + seed registry."""
    create_tables()
    seed_registry()


if __name__ == "__main__":
    init_db()
    print("Database initialized and registry seeded.")
