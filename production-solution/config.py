"""Central configuration for the production reconciliation pipeline.

Swap DB_URL to a PostgreSQL connection string for production use.
"""

import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Database — SQLite for dev/mock, change to postgresql://... for production
DB_URL = f"sqlite:///{os.path.join(BASE_DIR, 'reconciliation.db')}"

# Default data directories (overridable via CLI args)
DEFAULT_DATA_DIR = os.path.join(os.path.dirname(BASE_DIR), "relevant-data", "MRS", "2023", "01. Jan. 2023")
MAPPING_RULES_CSV = os.path.join(os.path.dirname(BASE_DIR), "web-gui", "data", "mapping_rules.csv")

# Web GUI
WEB_HOST = "127.0.0.1"
WEB_PORT = 5001
