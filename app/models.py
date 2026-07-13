"""SQLAlchemy ORM models.

Two tables are append-only logs (`PriceHistory`, `Alert`); the rest hold
current/mutable state (`TickerBaseline` per ticker, and the two
singleton-or-set tables `RuntimeConfig`/`TrackedTicker` that back the
runtime-adjustable settings - see their own docstrings for why).
"""

from datetime import datetime, timezone

from sqlalchemy import DateTime, Float, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


def utcnow() -> datetime:
    """Return the current time as a timezone-aware UTC datetime.

    Returns
    -------
    datetime.datetime
        Current UTC time.
    """
    return datetime.now(timezone.utc)


class PriceHistory(Base):
    """Append-only log of every raw price/volume tick fetched.

    Attributes
    ----------
    ticker : str
    timestamp : datetime.datetime
    price : float
    volume : float or None
    source : str
        Provider name the tick came from.
    """

    __tablename__ = "price_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ticker: Mapped[str] = mapped_column(String(16), index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    price: Mapped[float] = mapped_column(Float)
    volume: Mapped[float] = mapped_column(Float, nullable=True)
    source: Mapped[str] = mapped_column(String(32))


class TickerBaseline(Base):
    """Running mean/stddev per ticker (Welford's algorithm), so anomaly
    checks compare against an incrementally updated baseline instead of
    rescanning full price history on every poll.

    Four independent series share one `sample_count`, since they're
    always observed together: price, volume, bid/ask spread, and
    tick-to-tick delta magnitude (the volatility-clustering proxy).

    Attributes
    ----------
    ticker : str
    mean, variance_sum : float
        Running price mean and M2 (Welford's algorithm).
    volume_mean, volume_variance_sum : float
    spread_mean, spread_variance_sum : float
    delta_mean, delta_variance_sum : float
        Baseline of `abs(price - previous_price)` per poll.
    last_price, last_volume, last_spread : float or None
        Most recent observed values, used for display backfill and as
        the "previous" reference for the next delta calculation.
    stale_count : int
        Consecutive polls with unchanged price and zero volume.
    sample_count : int
    updated_at : datetime.datetime
    """

    __tablename__ = "ticker_baseline"

    ticker: Mapped[str] = mapped_column(String(16), primary_key=True)
    mean: Mapped[float] = mapped_column(Float, default=0.0)
    variance_sum: Mapped[float] = mapped_column(Float, default=0.0)  # M2 in Welford's algorithm
    volume_mean: Mapped[float] = mapped_column(Float, default=0.0)
    volume_variance_sum: Mapped[float] = mapped_column(Float, default=0.0)
    spread_mean: Mapped[float] = mapped_column(Float, default=0.0)
    spread_variance_sum: Mapped[float] = mapped_column(Float, default=0.0)
    # "Volatility clustering" proxy: baseline of |price - previous_price| per
    # poll. A tick-to-tick change magnitude that's anomalously large relative
    # to its own history signals a shift in volatility regime, distinct from
    # any single price level being far from the mean.
    delta_mean: Mapped[float] = mapped_column(Float, default=0.0)
    delta_variance_sum: Mapped[float] = mapped_column(Float, default=0.0)
    last_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    last_volume: Mapped[float | None] = mapped_column(Float, nullable=True)
    last_spread: Mapped[float | None] = mapped_column(Float, nullable=True)
    stale_count: Mapped[int] = mapped_column(Integer, default=0)
    sample_count: Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    @property
    def stddev(self) -> float:
        """Sample standard deviation of price, derived from `variance_sum`.

        Returns
        -------
        float
            0.0 if fewer than 2 samples have been recorded.
        """
        if self.sample_count < 2:
            return 0.0
        return (self.variance_sum / (self.sample_count - 1)) ** 0.5

    @property
    def volume_stddev(self) -> float:
        """Sample standard deviation of volume.

        Returns
        -------
        float
        """
        if self.sample_count < 2:
            return 0.0
        return (self.volume_variance_sum / (self.sample_count - 1)) ** 0.5

    @property
    def spread_stddev(self) -> float:
        """Sample standard deviation of bid/ask spread.

        Returns
        -------
        float
        """
        if self.sample_count < 2:
            return 0.0
        return (self.spread_variance_sum / (self.sample_count - 1)) ** 0.5

    @property
    def delta_stddev(self) -> float:
        """Sample standard deviation of tick-to-tick delta magnitude.

        Returns
        -------
        float
        """
        if self.sample_count < 2:
            return 0.0
        return (self.delta_variance_sum / (self.sample_count - 1)) ** 0.5


class Alert(Base):
    """A fired alert, real or test-triggered.

    Attributes
    ----------
    ticker : str
    triggered_at : datetime.datetime
    price : float
        The price at trigger time (0.0 for non-price signal types).
    z_score : float
        The triggering signal's z-score, or reconciliation % diff * 100.
    message : str
        Human-readable summary shown in the dashboard alert feed.
    context : str or None
        JSON-serialized snapshot of signals/baseline at trigger time.
    """

    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ticker: Mapped[str] = mapped_column(String(16), index=True)
    triggered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    price: Mapped[float] = mapped_column(Float)
    z_score: Mapped[float] = mapped_column(Float)
    message: Mapped[str] = mapped_column(String(256))
    # JSON-serialized snapshot of the signals/baseline stats that were true
    # AT TRIGGER TIME (captured where the alert already fires in
    # scheduler.py) - lets the alert detail view show what actually
    # happened, not today's baseline mislabeled as historical.
    context: Mapped[str | None] = mapped_column(Text, nullable=True)


class RuntimeConfig(Base):
    """Singleton row (id=1) holding the alert thresholds that used to be
    static env-var-only settings. Once this row exists it's the permanent
    source of truth - a later env var change has no effect, since the DB
    row wins. That's intentional (lets thresholds be adjusted live from the
    dashboard without a redeploy), but worth knowing if a threshold ever
    looks like it "isn't taking effect" after an env/deploy change."""

    __tablename__ = "runtime_config"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    anomaly_zscore_threshold: Mapped[float] = mapped_column(Float)
    stale_threshold: Mapped[int] = mapped_column(Integer)
    alert_cooldown_minutes: Mapped[int] = mapped_column(Integer)
    reconciliation_tolerance: Mapped[float] = mapped_column(Float)


class TrackedTicker(Base):
    """The editable set of tickers being polled - same "DB overrides the
    original env var once seeded" pattern as RuntimeConfig. Kept to a
    dedicated table (rather than a column on RuntimeConfig) since it's a
    list, not a scalar."""

    __tablename__ = "tracked_ticker"

    ticker: Mapped[str] = mapped_column(String(16), primary_key=True)
    added_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
