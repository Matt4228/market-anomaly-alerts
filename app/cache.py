"""In-memory TTL cache used to collapse duplicate price fetches within a
single poll cycle.
"""

import time


class TTLCache:
    """Short-TTL cache in front of the price fetcher.

    Deliberately short (seconds, not minutes): anomaly detection needs
    reasonably fresh prices, so this isn't a "cache aggressively" layer —
    it just collapses duplicate fetches for the same ticker within one
    poll cycle instead of hitting the provider twice for the same data.

    Parameters
    ----------
    ttl_seconds : int
        How long a cached value stays valid before it's treated as
        expired.
    """

    def __init__(self, ttl_seconds: int):
        self.ttl_seconds = ttl_seconds
        self._store: dict[str, tuple[float, dict]] = {}

    def get(self, key: str) -> dict | None:
        """Return the cached value for `key`, if present and not expired.

        Parameters
        ----------
        key : str
            Cache key (typically a ticker symbol).

        Returns
        -------
        dict or None
            The cached value, or None if missing or past its TTL.
        """
        entry = self._store.get(key)
        if entry is None:
            return None
        stored_at, value = entry
        if time.monotonic() - stored_at > self.ttl_seconds:
            del self._store[key]
            return None
        return value

    def set(self, key: str, value: dict) -> None:
        """Store `value` under `key`, timestamped for TTL expiry.

        Parameters
        ----------
        key : str
            Cache key.
        value : dict
            Value to cache.
        """
        self._store[key] = (time.monotonic(), value)
