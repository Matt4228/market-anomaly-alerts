"""On-demand historical price and fundamentals lookups for the ticker
detail modal — mirrors app/ingestion.py's blocking-call/to_thread/shared
rate-limiter pattern, but for one-off lookups rather than the live poll.
Nothing here is persisted.
"""

import asyncio
import logging
from datetime import date, timedelta

import yfinance as yf
from openbb import obb  # see app/ingestion.py for why this must be a module-level import

from app.config import settings
from app.ingestion import limiter  # shared token bucket — one budget across all OpenBB/yfinance calls

logger = logging.getLogger(__name__)

# range key -> (days back, OpenBB interval)
RANGE_CONFIG: dict[str, tuple[int, str]] = {
    "1D": (2, "1h"),
    "1W": (7, "1h"),
    "1M": (30, "1d"),
    "3M": (90, "1d"),
    "1Y": (365, "1d"),
}


def _fetch_history_blocking(ticker: str, range_key: str) -> list[dict]:
    """Blocking OpenBB historical-price call.

    Parameters
    ----------
    ticker : str
    range_key : str
        A key of `RANGE_CONFIG`, e.g. `"1D"`, `"1M"`.

    Returns
    -------
    list of dict
        `[{"date", "close"}, ...]` in chronological order.
    """
    days_back, interval = RANGE_CONFIG[range_key]
    result = obb.equity.price.historical(
        symbol=ticker,
        start_date=date.today() - timedelta(days=days_back),
        provider=settings.openbb_provider,
        interval=interval,
    )
    df = result.to_df()
    return [{"date": str(idx), "close": float(row["close"])} for idx, row in df.iterrows()]


async def fetch_history(ticker: str, range_key: str) -> list[dict]:
    """Rate-limited fetch of historical close prices for one range.

    Parameters
    ----------
    ticker : str
    range_key : str
        A key of `RANGE_CONFIG`.

    Returns
    -------
    list of dict
        Same shape as `_fetch_history_blocking`.
    """
    await limiter.acquire()
    return await asyncio.to_thread(_fetch_history_blocking, ticker, range_key)


def _fetch_fundamentals_blocking(ticker: str) -> dict:
    """Blocking dividends (OpenBB) + next-earnings (yfinance) lookup.
    Each half fails independently — a dividends or earnings-calendar
    hiccup returns an empty/None result for that half rather than
    failing the whole call.

    Parameters
    ----------
    ticker : str

    Returns
    -------
    dict
        `{"dividends": [{"ex_date", "amount"}, ...],
        "earnings": {"next_date", "eps_estimate"}}`.
    """
    dividends: list[dict] = []
    try:
        div_df = obb.equity.fundamental.dividends(symbol=ticker, provider=settings.openbb_provider).to_df()
        for _, row in div_df.tail(5).iterrows():
            dividends.append({"ex_date": str(row["ex_dividend_date"]), "amount": float(row["amount"])})
    except Exception:
        logger.exception("dividends fetch failed for %s", ticker)

    earnings: dict = {"next_date": None, "eps_estimate": None}
    try:
        calendar = yf.Ticker(ticker).calendar
        if calendar:
            next_dates = calendar.get("Earnings Date") or []
            earnings["next_date"] = str(next_dates[0]) if next_dates else None
            earnings["eps_estimate"] = calendar.get("Earnings Average")
    except Exception:
        logger.exception("earnings calendar fetch failed for %s", ticker)

    return {"dividends": dividends, "earnings": earnings}


async def fetch_fundamentals(ticker: str) -> dict:
    """Rate-limited fetch of dividends + next-earnings info.

    Parameters
    ----------
    ticker : str

    Returns
    -------
    dict
        Same shape as `_fetch_fundamentals_blocking`.
    """
    await limiter.acquire()
    return await asyncio.to_thread(_fetch_fundamentals_blocking, ticker)
