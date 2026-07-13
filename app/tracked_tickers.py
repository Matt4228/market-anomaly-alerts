"""The editable set of tracked tickers, backed by the `TrackedTicker` table
— same DB-overrides-env-var-after-first-seed pattern as `runtime_config.py`.
Bounded to `MIN_TICKERS`-`MAX_TICKERS` since the dashboard editor and the
detection pipeline are both sized around a small, always-fully-baselined
ticker set.
"""

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.config import settings
from app.models import TrackedTicker

MIN_TICKERS = 1
MAX_TICKERS = 5


def get_tracked_tickers(db: Session) -> list[str]:
    """Return the current tracked-ticker list, seeding it from
    `settings.tickers` if the table is empty.

    Parameters
    ----------
    db : sqlalchemy.orm.Session

    Returns
    -------
    list of str
        Sorted ticker symbols.
    """
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
    """Add a ticker to the tracked set.

    Parameters
    ----------
    db : sqlalchemy.orm.Session
    ticker : str

    Returns
    -------
    list of str
        The updated tracked-ticker list.

    Raises
    ------
    ValueError
        If already tracked, or the set is already at `MAX_TICKERS`.
    """
    current = get_tracked_tickers(db)
    if ticker in current:
        raise ValueError(f"{ticker} is already tracked")
    if len(current) >= MAX_TICKERS:
        raise ValueError(f"Already tracking the maximum of {MAX_TICKERS} tickers")

    db.add(TrackedTicker(ticker=ticker))
    db.commit()
    return get_tracked_tickers(db)


def remove_tracked_ticker(db: Session, ticker: str) -> list[str]:
    """Remove a ticker from the tracked set.

    Parameters
    ----------
    db : sqlalchemy.orm.Session
    ticker : str

    Returns
    -------
    list of str
        The updated tracked-ticker list.

    Raises
    ------
    ValueError
        If not currently tracked, or removing it would drop below
        `MIN_TICKERS`.
    """
    current = get_tracked_tickers(db)
    if ticker not in current:
        raise ValueError(f"{ticker} is not currently tracked")
    if len(current) <= MIN_TICKERS:
        raise ValueError(f"Must track at least {MIN_TICKERS} ticker")

    row = db.get(TrackedTicker, ticker)
    db.delete(row)
    db.commit()
    return get_tracked_tickers(db)
