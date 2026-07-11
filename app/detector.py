from dataclasses import dataclass

from sqlalchemy.orm import Session

from app.config import settings
from app.models import TickerBaseline


@dataclass
class AnomalyResult:
    is_anomaly: bool
    z_score: float  # max(price_z, volume_z) — the driving score, for Alert/message purposes
    kind: str  # "none" | "price" | "volume" | "price+volume"
    price_z: float
    volume_z: float


def _effective_stddev(stddev: float, scale: float) -> float:
    # Floor stddev relative to the metric's own scale. Without this, a
    # baseline with near-zero real variance (e.g. a data-quality issue, or
    # a market that's been closed) turns any ordinary tick into a z-score
    # in the millions/billions — dividing a real delta by a near-zero
    # denominator, not a genuine anomaly.
    return max(stddev, scale * settings.min_stddev_fraction)


def _welford_update(mean: float, variance_sum: float, count: int, value: float) -> tuple[float, float]:
    delta = value - mean
    new_mean = mean + delta / count
    delta2 = value - new_mean
    new_variance_sum = variance_sum + delta * delta2
    return new_mean, new_variance_sum


def check_anomaly(db: Session, ticker: str, price: float, volume: float) -> AnomalyResult:
    """Compares price AND volume against the baseline BEFORE folding them
    in, then updates both via Welford's algorithm. Checking first matters:
    scoring against a baseline that already includes the current point
    would dampen the very deviation we're trying to detect.

    Price and volume are tracked as two independent series sharing one
    sample_count, since they're always observed together (one poll = one
    price + one volume reading) — no need for two separate counters.
    """
    baseline = db.get(TickerBaseline, ticker)
    if baseline is None:
        baseline = TickerBaseline(
            ticker=ticker,
            mean=price,
            variance_sum=0.0,
            volume_mean=volume,
            volume_variance_sum=0.0,
            sample_count=1,
        )
        db.add(baseline)
        db.commit()
        return AnomalyResult(is_anomaly=False, z_score=0.0, kind="none", price_z=0.0, volume_z=0.0)

    price_z = 0.0
    volume_z = 0.0
    if baseline.sample_count >= 2:
        price_z = abs(price - baseline.mean) / _effective_stddev(baseline.stddev, price)
        volume_z = abs(volume - baseline.volume_mean) / _effective_stddev(baseline.volume_stddev, max(volume, 1.0))

    baseline.sample_count += 1
    baseline.mean, baseline.variance_sum = _welford_update(
        baseline.mean, baseline.variance_sum, baseline.sample_count, price
    )
    baseline.volume_mean, baseline.volume_variance_sum = _welford_update(
        baseline.volume_mean, baseline.volume_variance_sum, baseline.sample_count, volume
    )
    db.commit()

    price_is_anomaly = baseline.sample_count > 5 and price_z >= settings.anomaly_zscore_threshold
    volume_is_anomaly = baseline.sample_count > 5 and volume_z >= settings.anomaly_zscore_threshold

    if price_is_anomaly and volume_is_anomaly:
        kind = "price+volume"
    elif price_is_anomaly:
        kind = "price"
    elif volume_is_anomaly:
        kind = "volume"
    else:
        kind = "none"

    return AnomalyResult(
        is_anomaly=price_is_anomaly or volume_is_anomaly,
        z_score=max(price_z, volume_z),
        kind=kind,
        price_z=price_z,
        volume_z=volume_z,
    )


def synthetic_anomalous_sample(db: Session, ticker: str, kind: str = "price") -> dict | None:
    """Read-only: computes a price or volume value that would cross the
    anomaly threshold against the ticker's CURRENT baseline, without
    mutating it.

    Used by the manual test-trigger endpoint so a demo alert doesn't
    permanently skew real baseline stats with a synthetic data point —
    unlike check_anomaly, this never writes to ticker_baseline.
    """
    baseline = db.get(TickerBaseline, ticker)
    if baseline is None or baseline.sample_count <= 5:
        return None

    if kind == "volume":
        effective_stddev = _effective_stddev(baseline.volume_stddev, max(baseline.volume_mean, 1.0))
        synthetic_value = baseline.volume_mean + effective_stddev * (settings.anomaly_zscore_threshold + 1)
        z_score = abs(synthetic_value - baseline.volume_mean) / effective_stddev
    else:
        effective_stddev = _effective_stddev(baseline.stddev, baseline.mean)
        synthetic_value = baseline.mean + effective_stddev * (settings.anomaly_zscore_threshold + 1)
        z_score = abs(synthetic_value - baseline.mean) / effective_stddev

    return {"kind": kind, "value": synthetic_value, "z_score": z_score}
