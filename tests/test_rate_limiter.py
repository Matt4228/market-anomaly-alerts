import time

import pytest

from app.rate_limiter import TokenBucketLimiter, with_backoff


@pytest.mark.asyncio
async def test_acquire_consumes_a_token_immediately_when_capacity_is_available():
    limiter = TokenBucketLimiter(capacity=2, refill_per_sec=1.0)
    await limiter.acquire()
    assert limiter._tokens == pytest.approx(1.0, abs=0.05)


@pytest.mark.asyncio
async def test_acquire_blocks_until_a_token_refills_when_the_bucket_is_empty():
    limiter = TokenBucketLimiter(capacity=1, refill_per_sec=50.0)
    await limiter.acquire()  # drains the single starting token
    start = time.monotonic()
    await limiter.acquire()
    elapsed = time.monotonic() - start
    assert elapsed >= 0.01  # had to wait for a refill, wasn't served instantly


@pytest.mark.asyncio
async def test_with_backoff_returns_immediately_on_first_success():
    calls = []

    async def succeeds():
        calls.append(1)
        return "ok"

    result = await with_backoff(succeeds, max_attempts=5, base_delay=0.01)
    assert result == "ok"
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_with_backoff_retries_until_success_within_max_attempts():
    calls = []

    async def fails_twice_then_succeeds():
        calls.append(1)
        if len(calls) < 3:
            raise ValueError("transient failure")
        return "ok"

    result = await with_backoff(fails_twice_then_succeeds, max_attempts=5, base_delay=0.01)
    assert result == "ok"
    assert len(calls) == 3


@pytest.mark.asyncio
async def test_with_backoff_raises_the_last_exception_after_exhausting_attempts():
    async def always_fails():
        raise ValueError("permanent failure")

    with pytest.raises(ValueError, match="permanent failure"):
        await with_backoff(always_fails, max_attempts=3, base_delay=0.01)
