from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.config import settings
from app.models import TrackedTicker

MIN_TICKERS = 1
MAX_TICKERS = 5


def get_tracked_tickers(db: Session) -> list[str]:
    rows = db.execute(select(TrackedTicker.ticker).order_by(TrackedTicker.ticker)).scalars().all()
    if rows:
        return list(rows)

    # Normally seeded once in main.py's lifespan before any request is
    # accepted (same reasoning as RuntimeConfig's seeding) — this branch is
    # a defensive fallback, not the primary mechanism.
    for ticker in settings.tickers:
        db.add(TrackedTicker(ticker=ticker))
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
    return get_tracked_tickers(db)


def add_tracked_ticker(db: Session, ticker: str) -> list[str]:
    current = get_tracked_tickers(db)
    if ticker in current:
        raise ValueError(f"{ticker} is already tracked")
    if len(current) >= MAX_TICKERS:
        raise ValueError(f"Already tracking the maximum of {MAX_TICKERS} tickers")

    db.add(TrackedTicker(ticker=ticker))
    db.commit()
    return get_tracked_tickers(db)


def remove_tracked_ticker(db: Session, ticker: str) -> list[str]:
    current = get_tracked_tickers(db)
    if ticker not in current:
        raise ValueError(f"{ticker} is not currently tracked")
    if len(current) <= MIN_TICKERS:
        raise ValueError(f"Must track at least {MIN_TICKERS} ticker")

    row = db.get(TrackedTicker, ticker)
    db.delete(row)
    db.commit()
    return get_tracked_tickers(db)
