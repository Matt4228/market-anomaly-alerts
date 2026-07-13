"""Fetches live price/quote data for the poll cycle: a primary OpenBB
quote (cached + rate-limited) and a secondary yfinance reading used only
for reconciliation cross-checks.
"""

import asyncio
import logging
from datetime import datetime, timezone

import yfinance as yf

# Imported at module load (main thread) rather than inside the function that
# runs via asyncio.to_thread. OpenBB's first-ever import runs a one-time
# package-build step that registers a SIGTERM handler, and Python only
# allows registering signal handlers from the main thread — deferring this
# import into a worker thread crashes every single call with "signal only
# works in main thread of the main interpreter" (this is Linux-strict;
# Windows silently allows it, so it can look fine in local dev and only
# fail once deployed).
from openbb import obb

from app.cache import TTLCache
from app.config import settings
from app.rate_limiter import TokenBucketLimiter, with_backoff

limiter = TokenBucketLimiter(
    capacity=settings.rate_limit_capacity,
    refill_per_sec=settings.rate_limit_refill_per_sec,
)
price_cache = TTLCache(ttl_seconds=settings.price_cache_ttl_seconds)
logger = logging.getLogger(__name__)


def _fetch_price_blocking(ticker: str) -> dict:
    """Blocking OpenBB call — dispatched via asyncio.to_thread so it
    doesn't block the FastAPI event loop that the scheduler and
    websocket broadcasts also run on.

    Uses the live quote endpoint, not historical(): historical() defaults
    to daily bars, so polling it every few minutes just re-fetches the
    same last-completed-day close over and over — no price movement ever
    shows up, and the anomaly detector never has real variance to work
    with. quote() returns the actual current last-traded price.

    NOTE: OpenBB's Python interface has shifted across versions. Verify
    this against `obb.equity.price.quote.__doc__` / `obb.coverage` for
    whatever version ends up installed before relying on it.

    Parameters
    ----------
    ticker : str

    Returns
    -------
    dict
        `{"ticker", "price", "bid", "ask", "volume", "timestamp", "source"}`.

    Raises
    ------
    ValueError
        If the provider returns no rows for `ticker`.
    """
    result = obb.equity.price.quote(symbol=ticker, provider=settings.openbb_provider)
    df = result.to_df()
    if df.empty:
        raise ValueError(f"no price data returned for {ticker}")

    row = df.iloc[0]
    # yfinance's quote schema differs by asset type: EQUITY rows include
    # last_price, ETF rows (e.g. SPY) don't. bid/ask midpoint is present
    # on both, so it's used uniformly instead of branching per asset type.
    bid = float(row["bid"])
    ask = float(row["ask"])
    price = (bid + ask) / 2
    return {
        "ticker": ticker,
        "price": price,
        "bid": bid,
        "ask": ask,
        "volume": float(row.get("volume", 0) or 0),
        "timestamp": datetime.now(timezone.utc),
        "source": settings.openbb_provider,
    }


async def fetch_latest_price(ticker: str) -> dict:
    """Rate-limited, cached, retrying fetch of the current quote for
    `ticker`.

    Parameters
    ----------
    ticker : str

    Returns
    -------
    dict
        Same shape as `_fetch_price_blocking`.
    """
    cached = price_cache.get(ticker)
    if cached is not None:
        return cached

    await limiter.acquire()

    async def _attempt() -> dict:
        return await asyncio.to_thread(_fetch_price_blocking, ticker)

    price = await with_backoff(_attempt)
    price_cache.set(ticker, price)
    return price


def _fetch_reconciliation_price_blocking(ticker: str) -> float:
    """A second, independently-fetched reading of the same ticker, used
    only to cross-check against the primary OpenBB quote — not the
    ingestion path itself.

    Calls yfinance directly rather than through OpenBB's wrapper. Worth
    being honest about what this does and doesn't prove: both ultimately
    trace back to Yahoo Finance, so this isn't two unrelated vendors —
    but it's a genuinely different code path/endpoint, so timing and
    caching differences between them are real, and catching a large
    discrepancy is still a legitimate reconciliation check, the same
    pattern used to catch stale or wrong data from a single source.

    Parameters
    ----------
    ticker : str

    Returns
    -------
    float
    """
    info = yf.Ticker(ticker).fast_info
    return float(info.last_price)


async def fetch_reconciliation_price(ticker: str) -> float | None:
    """Best-effort: returns None on any failure rather than raising, since
    this is a supplementary cross-check, not the primary ingestion path —
    a hiccup here should never affect the main price/anomaly pipeline.

    Parameters
    ----------
    ticker : str

    Returns
    -------
    float or None
        None on any fetch failure.
    """
    try:
        await limiter.acquire()
        return await asyncio.to_thread(_fetch_reconciliation_price_blocking, ticker)
    except Exception:
        logger.exception("reconciliation fetch failed for %s", ticker)
        return None
