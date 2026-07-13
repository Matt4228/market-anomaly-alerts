"""SQLAlchemy engine/session setup.

`SessionLocal` sessions are short-lived by convention throughout this
codebase - opened, used, and closed within a single function rather than
held across await points. Functions that return values derived from a
session (e.g. `runtime_config.get_runtime_config`) return plain
dataclasses, not ORM objects, so callers never touch an object whose
session has already closed.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from app.config import settings

engine = create_engine(settings.database_url, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


def get_session():
    """FastAPI dependency yielding a request-scoped SQLAlchemy session.

    Yields
    ------
    sqlalchemy.orm.Session
        Closed automatically once the request finishes.
    """
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
