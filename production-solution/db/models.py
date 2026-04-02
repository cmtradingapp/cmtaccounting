"""SQLAlchemy ORM models for the 3-layer reconciliation database.

Layer 1 (raw):   As-is data, all stored as JSON blobs — no schema enforcement.
Layer 2 (clean): Standardized, typed tables — one schema per domain.
Layer 3 (final): Reconciled, report-ready output.
"""

from datetime import datetime, timezone

from sqlalchemy import (
    Column, Integer, Float, Text, DateTime, Date, JSON, ForeignKey, UniqueConstraint
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


# ═══════════════════════════════════════════════════════════════════════════════
# LAYER 1 — RAW (as-is, no transforms)
# ═══════════════════════════════════════════════════════════════════════════════

class RawPSPTransaction(Base):
    """Raw PSP statement rows, stored as JSON. One table for all PSPs."""
    __tablename__ = "raw_psp_transactions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    psp_name = Column(Text, nullable=False, index=True)
    source_file = Column(Text, nullable=False)
    row_number = Column(Integer, nullable=False)
    raw_data = Column(JSON, nullable=False)


class RawBankTransaction(Base):
    """Raw bank statement rows, stored as JSON. One table for all banks."""
    __tablename__ = "raw_bank_transactions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    bank_name = Column(Text, nullable=False, index=True)
    source_file = Column(Text, nullable=False)
    row_number = Column(Integer, nullable=False)
    raw_data = Column(JSON, nullable=False)


class RawCRMTransaction(Base):
    """Raw CRM transaction rows, stored as JSON."""
    __tablename__ = "raw_crm_transactions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_file = Column(Text, nullable=False)
    row_number = Column(Integer, nullable=False)
    raw_data = Column(JSON, nullable=False)


# ═══════════════════════════════════════════════════════════════════════════════
# LAYER 2 — CLEAN (standardized schemas)
# ═══════════════════════════════════════════════════════════════════════════════

class CleanCRMTransaction(Base):
    """Standardized CRM transaction — one row per deposit/withdrawal/credit."""
    __tablename__ = "clean_crm_transactions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    crm_transaction_id = Column(Text)           # original transactionid
    psp_transaction_id = Column(Text, index=True)  # PRIMARY join key (normalized)
    transactionid = Column(Text, index=True)       # FALLBACK join key (normalized)
    login = Column(Text)
    amount = Column(Float)
    currency = Column(Text)                     # resolved from currency_id
    usd_amount = Column(Float)
    payment_method = Column(Text)
    payment_processor = Column(Text)
    transaction_type = Column(Text)             # Deposit / Withdraw
    date = Column(Date)
    mtorder_id = Column(Text)
    report_month = Column(Text)
    first_name = Column(Text)
    last_name = Column(Text)
    comment = Column(Text)
    raw_id = Column(Integer, ForeignKey("raw_crm_transactions.id"))


class CleanPSPTransaction(Base):
    """Standardized PSP transaction — ALL PSPs normalized into one table."""
    __tablename__ = "clean_psp_transactions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    psp_name = Column(Text, nullable=False, index=True)
    reference_id = Column(Text, nullable=False, index=True)  # normalized join key
    amount = Column(Float)
    currency = Column(Text)
    fee = Column(Float)
    status = Column(Text)
    date = Column(Date)
    source_file = Column(Text)
    raw_id = Column(Integer, ForeignKey("raw_psp_transactions.id"))


class CleanBankTransaction(Base):
    """Standardized bank transaction — ALL banks normalized into one table."""
    __tablename__ = "clean_bank_transactions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    bank_name = Column(Text, nullable=False, index=True)
    reference = Column(Text)
    client_id = Column(Text, index=True)        # extracted from description/reference
    amount = Column(Float)
    currency = Column(Text)
    date = Column(Date)
    description = Column(Text)
    source_file = Column(Text)
    raw_id = Column(Integer, ForeignKey("raw_bank_transactions.id"))


class PSPSchemaRegistry(Base):
    """Per-PSP column mapping rules. Enables config-driven generic transformers."""
    __tablename__ = "psp_schema_registry"

    id = Column(Integer, primary_key=True, autoincrement=True)
    psp_name = Column(Text, nullable=False, unique=True)
    ref_column = Column(Text, nullable=False)
    amount_column = Column(Text)
    currency_column = Column(Text)
    date_column = Column(Text)
    status_column = Column(Text)
    fee_column = Column(Text)
    skiprows = Column(Integer, default=0)
    date_format = Column(Text)
    notes = Column(Text)


# ═══════════════════════════════════════════════════════════════════════════════
# LAYER 3 — FINAL (reconciled, report-ready)
# ═══════════════════════════════════════════════════════════════════════════════

class ReconciliationRun(Base):
    """Metadata for each reconciliation execution."""
    __tablename__ = "reconciliation_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    report_month = Column(Text)
    run_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    match_rate = Column(Float)
    matched = Column(Integer)
    unmatched_crm = Column(Integer)
    unmatched_psp = Column(Integer)
    unrecon_amount = Column(Float)
    triggered_by = Column(Text, default="manual")

    results = relationship("Reconciliation", back_populates="run")


class Reconciliation(Base):
    """Individual match result — one row per CRM transaction after reconciliation."""
    __tablename__ = "reconciliation"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, ForeignKey("reconciliation_runs.id"), nullable=False)
    crm_id = Column(Integer, ForeignKey("clean_crm_transactions.id"))
    psp_tx_id = Column(Integer, ForeignKey("clean_psp_transactions.id"))
    bank_tx_id = Column(Integer, ForeignKey("clean_bank_transactions.id"))
    match_status = Column(Text, nullable=False)  # matched / unmatched_crm / unmatched_psp / unmatched_bank
    crm_amount = Column(Float)
    psp_amount = Column(Float)
    bank_amount = Column(Float)
    amount_diff = Column(Float)
    currency_match = Column(Text)                # same / cross_ccy

    run = relationship("ReconciliationRun", back_populates="results")
