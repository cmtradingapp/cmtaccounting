"""SQLAlchemy engine and session factory.

Uses SQLite by default (config.DB_URL). Swap to PostgreSQL by changing the URL.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config import DB_URL

engine = create_engine(DB_URL, echo=False, future=True)
Session = sessionmaker(bind=engine)


def get_session():
    """Return a new SQLAlchemy session."""
    return Session()
