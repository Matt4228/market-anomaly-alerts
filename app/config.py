from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://postgres:postgres@localhost:5432/market_alerts"

    tracked_tickers: str = "AAPL,MSFT,GOOGL,TSLA,SPY"
    openbb_provider: str = "yfinance"

    poll_interval_minutes: int = 5
    alert_cooldown_minutes: int = 30
    anomaly_zscore_threshold: float = 3.0
    min_stddev_fraction: float = 0.0005
    stale_threshold: int = 3

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
        return [t.strip().upper() for t in self.tracked_tickers.split(",") if t.strip()]


settings = Settings()
