from datetime import datetime, timezone

from sqlalchemy import DateTime, Float, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class PriceHistory(Base):
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
    rescanning full price history on every poll."""

    __tablename__ = "ticker_baseline"

    ticker: Mapped[str] = mapped_column(String(16), primary_key=True)
    mean: Mapped[float] = mapped_column(Float, default=0.0)
    variance_sum: Mapped[float] = mapped_column(Float, default=0.0)  # M2 in Welford's algorithm
    volume_mean: Mapped[float] = mapped_column(Float, default=0.0)
    volume_variance_sum: Mapped[float] = mapped_column(Float, default=0.0)
    sample_count: Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)

    @property
    def stddev(self) -> float:
        if self.sample_count < 2:
            return 0.0
        return (self.variance_sum / (self.sample_count - 1)) ** 0.5

    @property
    def volume_stddev(self) -> float:
        if self.sample_count < 2:
            return 0.0
        return (self.volume_variance_sum / (self.sample_count - 1)) ** 0.5


class Alert(Base):
    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ticker: Mapped[str] = mapped_column(String(16), index=True)
    triggered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, index=True)
    price: Mapped[float] = mapped_column(Float)
    z_score: Mapped[float] = mapped_column(Float)
    message: Mapped[str] = mapped_column(String(256))
