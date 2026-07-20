import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.db import Base, get_session


@pytest.fixture
def db_session():
    """A fresh in-memory SQLite session with all tables created, isolated
    per test. Never touches the app's real (Postgres) engine in app/db.py.
    """
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(bind=engine)
    TestSessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    session = TestSessionLocal()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def client(db_session):
    """A TestClient for the real app, with get_session overridden to the
    in-memory db_session. Deliberately NOT used as a context manager, so
    the app's lifespan hook never runs - that hook starts a real
    APScheduler job that polls live market data, which tests must not
    trigger.
    """
    from app.main import app

    def override_get_session():
        yield db_session

    app.dependency_overrides[get_session] = override_get_session
    test_client = TestClient(app)
    yield test_client
    app.dependency_overrides.clear()
