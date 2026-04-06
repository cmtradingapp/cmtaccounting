"""Shared test fixtures for the production reconciliation pipeline."""

import os
import sys
import pytest

# Add production-solution to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from db.models import Base


# ── Data paths ──────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

JAN_2023_DIR = os.path.join(PROJECT_ROOT, "relevant-data", "MRS", "2023", "01. Jan. 2023")
JAN_PSP_DIR = os.path.join(JAN_2023_DIR, "PSPs")
JAN_PLATFORM_DIR = os.path.join(JAN_2023_DIR, "platform")

JAN_CRM_FILE = os.path.join(JAN_PLATFORM_DIR, "CRM Transactions Additional info.xlsx")


@pytest.fixture
def jan_data_dir():
    """Path to Jan 2023 test data directory."""
    if not os.path.isdir(JAN_2023_DIR):
        pytest.skip(f"Test data not found: {JAN_2023_DIR}")
    return JAN_2023_DIR


@pytest.fixture
def jan_crm_file():
    """Path to Jan 2023 CRM file."""
    if not os.path.isfile(JAN_CRM_FILE):
        pytest.skip(f"CRM file not found: {JAN_CRM_FILE}")
    return JAN_CRM_FILE


@pytest.fixture
def jan_psp_dir():
    """Path to Jan 2023 PSP files directory."""
    if not os.path.isdir(JAN_PSP_DIR):
        pytest.skip(f"PSP directory not found: {JAN_PSP_DIR}")
    return JAN_PSP_DIR


# ── In-memory test database ────────────────────────────────────────────────

@pytest.fixture
def test_db():
    """Create an in-memory SQLite database with all tables."""
    test_engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(test_engine)
    TestSession = sessionmaker(bind=test_engine)
    session = TestSession()
    yield session
    session.close()


@pytest.fixture
def test_engine():
    """Create an in-memory SQLite engine for testing."""
    eng = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(eng)
    return eng
