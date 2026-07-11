from sqlalchemy.orm import Session

from app.config import settings
from app.models import TickerBaseline


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
    if baseline.sample_count >= 2 and baseline.stddev > 0:
        z_score = abs(price - baseline.mean) / baseline.stddev

    baseline.sample_count += 1
    delta = price - baseline.mean
    baseline.mean += delta / baseline.sample_count
    delta2 = price - baseline.mean
    baseline.variance_sum += delta * delta2
    db.commit()

    is_anomaly = baseline.sample_count > 5 and z_score >= settings.anomaly_zscore_threshold
    return is_anomaly, z_score
