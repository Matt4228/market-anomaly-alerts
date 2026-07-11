from sqlalchemy.orm import Session

from app.config import settings
from app.models import TickerBaseline


def _effective_stddev(baseline: TickerBaseline, price: float) -> float:
    # Floor stddev relative to price scale. Without this, a baseline with
    # near-zero real variance (e.g. a data-quality issue, or a market
    # that's been closed) turns any ordinary price tick into a z-score in
    # the millions/billions — dividing a real delta by a near-zero
    # denominator, not a genuine anomaly.
    return max(baseline.stddev, price * settings.min_stddev_fraction)


def check_anomaly(db: Session, ticker: str, price: float) -> tuple[bool, float]:
    """Compares price against the baseline BEFORE folding it in, then
    updates the baseline via Welford's algorithm. Checking first matters:
    scoring against a baseline that already includes the current point
    would dampen the very deviation we're trying to detect.
    """
    baseline = db.get(TickerBaseline, ticker)
    if baseline is None:
        baseline = TickerBaseline(ticker=ticker, mean=price, variance_sum=0.0, sample_count=1)
        db.add(baseline)
        db.commit()
        return False, 0.0

    z_score = 0.0
    if baseline.sample_count >= 2:
        z_score = abs(price - baseline.mean) / _effective_stddev(baseline, price)

    baseline.sample_count += 1
    delta = price - baseline.mean
    baseline.mean += delta / baseline.sample_count
    delta2 = price - baseline.mean
    baseline.variance_sum += delta * delta2
    db.commit()

    is_anomaly = baseline.sample_count > 5 and z_score >= settings.anomaly_zscore_threshold
    return is_anomaly, z_score


def synthetic_anomalous_price(db: Session, ticker: str) -> tuple[float, float] | None:
    """Read-only: computes a price that would cross the anomaly threshold
    against the ticker's CURRENT baseline, without mutating it.

    Used by the manual test-trigger endpoint so a demo alert doesn't
    permanently skew real baseline stats with a synthetic data point —
    unlike check_anomaly, this never writes to ticker_baseline.
    """
    baseline = db.get(TickerBaseline, ticker)
    if baseline is None or baseline.sample_count <= 5:
        return None

    effective_stddev = _effective_stddev(baseline, baseline.mean)
    synthetic_price = baseline.mean + effective_stddev * (settings.anomaly_zscore_threshold + 1)
    z_score = abs(synthetic_price - baseline.mean) / effective_stddev
    return synthetic_price, z_score
