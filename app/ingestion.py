import asyncio
from datetime import datetime, timezone

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
    """
    result = obb.equity.price.quote(symbol=ticker, provider=settings.openbb_provider)
    df = result.to_df()
    if df.empty:
        raise ValueError(f"no price data returned for {ticker}")

    row = df.iloc[0]
    # yfinance's quote schema differs by asset type: EQUITY rows include
    # last_price, ETF rows (e.g. SPY) don't. bid/ask midpoint is present
    # on both, so it's used uniformly instead of branching per asset type.
    price = (float(row["bid"]) + float(row["ask"])) / 2
    return {
        "ticker": ticker,
        "price": price,
        "volume": float(row.get("volume", 0) or 0),
        "timestamp": datetime.now(timezone.utc),
        "source": settings.openbb_provider,
    }


async def fetch_latest_price(ticker: str) -> dict:
    cached = price_cache.get(ticker)
    if cached is not None:
        return cached

    await limiter.acquire()

    async def _attempt() -> dict:
        return await asyncio.to_thread(_fetch_price_blocking, ticker)

    price = await with_backoff(_attempt)
    price_cache.set(ticker, price)
    return price
