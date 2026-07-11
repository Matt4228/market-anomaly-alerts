import asyncio
from datetime import datetime, timezone

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

    NOTE: OpenBB's Python interface has shifted across versions. Verify
    this against `obb.equity.price.historical.__doc__` / `obb.coverage`
    for whatever version ends up installed before relying on it.
    """
    from openbb import obb

    result = obb.equity.price.historical(
        symbol=ticker,
        provider=settings.openbb_provider,
        limit=1,
    )
    df = result.to_df()
    if df.empty:
        raise ValueError(f"no price data returned for {ticker}")

    row = df.iloc[-1]
    return {
        "ticker": ticker,
        "price": float(row["close"]),
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
