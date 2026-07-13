"""Static, env-var-sourced configuration.

Note the split in responsibility: alert thresholds defined here
(`anomaly_zscore_threshold`, `stale_threshold`, `alert_cooldown_minutes`,
`reconciliation_tolerance`) and the tracked ticker list (`tracked_tickers`)
are only used as *seed defaults* — see `runtime_config.py` and
`tracked_tickers.py`, which move the live values into Postgres so they're
adjustable from the dashboard without a redeploy. Everything else here
(DB connection, provider, rate limits, Slack, debug token) stays
env-var-only.
"""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings, loaded once from environment variables/.env.

    Attributes
    ----------
    database_url : str
        Postgres connection string.
    tracked_tickers : str
        Comma-separated seed list of tickers (see module docstring).
    openbb_provider : str
        Provider name passed to OpenBB calls.
    poll_interval_minutes : int
        How often the background poll cycle runs.
    anomaly_zscore_threshold, stale_threshold, alert_cooldown_minutes,
    reconciliation_tolerance
        Seed defaults for the runtime-adjustable alert thresholds.
    min_stddev_fraction : float
        Floor applied to baseline stddev (relative to the metric's scale)
        so near-zero variance can't produce a meaningless huge z-score.
    rate_limit_capacity, rate_limit_refill_per_sec : int, float
        Token bucket sizing for outbound provider calls.
    price_cache_ttl_seconds : int
        TTL for the short-lived price cache.
    slack_webhook_url : str or None
        Optional Slack incoming webhook URL.
    debug_token : str or None
        Shared secret required by mutating debug/admin endpoints. Fails
        closed if unset (see README).
    """

    database_url: str = "postgresql://postgres:postgres@localhost:5432/market_alerts"

    tracked_tickers: str = "AAPL,MSFT,GOOGL,TSLA,SPY"
    openbb_provider: str = "yfinance"

    poll_interval_minutes: int = 5
    alert_cooldown_minutes: int = 30
    anomaly_zscore_threshold: float = 3.0
    min_stddev_fraction: float = 0.0005
    stale_threshold: int = 3
    reconciliation_tolerance: float = 0.02

    # Free-tier providers like yfinance/OpenBB aggregate several backends;
    # this bucket is sized conservatively rather than at the documented cap
    # so a burst of tickers in one poll cycle doesn't trip a downstream limit.
    rate_limit_capacity: int = 5
    rate_limit_refill_per_sec: float = 1.0

    price_cache_ttl_seconds: int = 60

    slack_webhook_url: str | None = None
    # Fails closed if unset: no token configured means /debug/test-alert is
    # fully refused, not silently open. See README for why this only stops
    # casual/automated abuse, not someone reading the dashboard's own JS.
    debug_token: str | None = None

    class Config:
        env_file = ".env"

    @property
    def tickers(self) -> list[str]:
        """Parsed, upper-cased seed ticker list.

        Returns
        -------
        list of str
            Ticker symbols from `tracked_tickers`. Only used to seed
            `tracked_ticker` on first startup — see `tracked_tickers.py`.
        """
        return [t.strip().upper() for t in self.tracked_tickers.split(",") if t.strip()]


settings = Settings()
