"""Shared rate limiting for outbound calls to OpenBB/yfinance: a token
bucket to pace requests, and a backoff helper to retry failures politely.
"""

import asyncio
import random
import time
from typing import Awaitable, Callable, TypeVar

T = TypeVar("T")


class TokenBucketLimiter:
    """Shared token bucket gating calls to a rate-limited API.

    Sized below the provider's documented cap on purpose: several tickers
    can be polled in the same cycle, and this bucket is what serializes
    them instead of firing all requests at once and tripping a 429.

    Parameters
    ----------
    capacity : int
        Maximum number of tokens (i.e. burst size) the bucket can hold.
    refill_per_sec : float
        Tokens added back per second.
    """

    def __init__(self, capacity: int, refill_per_sec: float):
        self.capacity = capacity
        self.refill_per_sec = refill_per_sec
        self._tokens = float(capacity)
        self._updated_at = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Block until a token is available, then consume it.

        Returns
        -------
        None
        """
        async with self._lock:
            while True:
                now = time.monotonic()
                elapsed = now - self._updated_at
                self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_per_sec)
                self._updated_at = now

                if self._tokens >= 1:
                    self._tokens -= 1
                    return

                wait_for = (1 - self._tokens) / self.refill_per_sec
                await asyncio.sleep(wait_for)


async def with_backoff(
    fn: Callable[[], Awaitable[T]],
    *,
    max_attempts: int = 5,
    base_delay: float = 1.0,
) -> T:
    """Retry `fn` with exponential backoff and jitter.

    Jitter matters here specifically because every ticker in a poll cycle
    could hit a limit at once - without jitter they'd all retry in
    lockstep and re-trigger the same limit on the next attempt.

    Parameters
    ----------
    fn : Callable[[], Awaitable[T]]
        Zero-argument async callable to retry on failure.
    max_attempts : int, optional
        Maximum number of attempts before giving up, by default 5.
    base_delay : float, optional
        Base delay in seconds for the exponential backoff, by default 1.0.

    Returns
    -------
    T
        Whatever `fn` returns on success.

    Raises
    ------
    Exception
        Re-raises the last exception if all attempts fail.
    """
    last_exc: Exception | None = None
    for attempt in range(max_attempts):
        try:
            return await fn()
        except Exception as exc:  # noqa: BLE001 - provider exception types vary by version
            last_exc = exc
            if attempt == max_attempts - 1:
                break
            delay = base_delay * (2**attempt) + random.uniform(0, base_delay)
            await asyncio.sleep(delay)

    assert last_exc is not None
    raise last_exc
