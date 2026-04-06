"""
Shared fixtures for the MRS test suite.
All paths are relative to the repo root so tests work regardless of cwd.
"""
import os
import sys
import glob
import pytest
import pandas as pd

# Make web-gui importable
REPO_ROOT = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(REPO_ROOT, 'web-gui'))

DATA_ROOT   = os.path.join(REPO_ROOT, 'relevant-data')
JAN_PSP_DIR = os.path.join(DATA_ROOT, 'MRS', '2023', '01. Jan. 2023', 'PSPs')
JAN_PLAT    = os.path.join(DATA_ROOT, 'MRS', '2023', '01. Jan. 2023', 'platform',
                           'CRM Transactions Additional info.xlsx')
JAN_REF     = os.path.join(DATA_ROOT, 'Life cycle report', '2023', '1. January', 'List.xlsx')


def _flat_psp_files(directory):
    """Return only the flat (non-subdirectory) CSV/XLSX files in the PSPs folder."""
    files = []
    for f in os.listdir(directory):
        full = os.path.join(directory, f)
        if os.path.isfile(full) and f.lower().endswith(('.csv', '.xlsx', '.xls')):
            files.append(full)
    return sorted(files)


@pytest.fixture(scope='session')
def jan_crm():
    return pd.read_excel(JAN_PLAT)


@pytest.fixture(scope='session')
def jan_psp_files():
    return _flat_psp_files(JAN_PSP_DIR)


@pytest.fixture(scope='session')
def reference_lifecycle():
    return pd.read_excel(JAN_REF)


@pytest.fixture(scope='session')
def flask_client(tmp_path_factory):
    import server
    upload_dir = str(tmp_path_factory.mktemp('uploads'))
    server.app.config['TESTING'] = True
    server.app.config['UPLOAD_FOLDER'] = upload_dir
    server.STATE_FILE = os.path.join(upload_dir, '_recon_state.pkl')
    with server.app.test_client() as client:
        yield client
