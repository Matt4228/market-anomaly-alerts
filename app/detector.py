from dataclasses import dataclass, field

from sqlalchemy.orm import Session

from app.config import settings
from app.models import TickerBaseline


@dataclass
class AnomalyResult:
    is_anomaly: bool
    z_score: float  # max across triggered numeric signals (0.0 if stale-only)
    kind: str  # "+"-joined triggered signal names, "stale", or "none"
    signals: dict[str, float] = field(default_factory=dict)  # all computed z-scores
    stale: bool = False
    stale_count: int = 0


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


def check_anomaly(db: Session, ticker: str, price: float, volume: float, bid: float, ask: float) -> AnomalyResult:
    """Compares price, volume, bid/ask spread, and tick-to-tick change
    magnitude against the baseline BEFORE folding them in, then updates all
    four via Welford's algorithm. Checking first matters: scoring against a
    baseline that already includes the current point would dampen the very
    deviation we're trying to detect.

    All four series share one sample_count, since they're always observed
    together (one poll = one price + volume + bid + ask reading).
    """
    spread = ask - bid
    baseline = db.get(TickerBaseline, ticker)
    if baseline is None:
        baseline = TickerBaseline(
            ticker=ticker,
            mean=price,
            variance_sum=0.0,
            volume_mean=volume,
            volume_variance_sum=0.0,
            spread_mean=spread,
            spread_variance_sum=0.0,
            delta_mean=0.0,
            delta_variance_sum=0.0,
            last_price=price,
            stale_count=0,
            sample_count=1,
        )
        db.add(baseline)
        db.commit()
        return AnomalyResult(is_anomaly=False, z_score=0.0, kind="none")

    previous_price = baseline.last_price if baseline.last_price is not None else price
    delta = abs(price - previous_price)
    is_stale_tick = price == previous_price and volume == 0

    signals: dict[str, float] = {}
    if baseline.sample_count >= 2:
        signals["price"] = abs(price - baseline.mean) / _effective_stddev(baseline.stddev, price)
        signals["volume"] = abs(volume - baseline.volume_mean) / _effective_stddev(baseline.volume_stddev, max(volume, 1.0))
        signals["spread"] = abs(spread - baseline.spread_mean) / _effective_stddev(baseline.spread_stddev, max(price, 1.0))
        signals["volatility"] = abs(delta - baseline.delta_mean) / _effective_stddev(baseline.delta_stddev, max(price, 1.0))

    baseline.sample_count += 1
    baseline.mean, baseline.variance_sum = _welford_update(baseline.mean, baseline.variance_sum, baseline.sample_count, price)
    baseline.volume_mean, baseline.volume_variance_sum = _welford_update(
        baseline.volume_mean, baseline.volume_variance_sum, baseline.sample_count, volume
    )
    baseline.spread_mean, baseline.spread_variance_sum = _welford_update(
        baseline.spread_mean, baseline.spread_variance_sum, baseline.sample_count, spread
    )
    baseline.delta_mean, baseline.delta_variance_sum = _welford_update(
        baseline.delta_mean, baseline.delta_variance_sum, baseline.sample_count, delta
    )
    baseline.last_price = price
    baseline.stale_count = baseline.stale_count + 1 if is_stale_tick else 0
    db.commit()

    enough_samples = baseline.sample_count > 5
    triggered = [name for name, z in signals.items() if enough_samples and z >= settings.anomaly_zscore_threshold]
    is_stale = enough_samples and baseline.stale_count >= settings.stale_threshold

    if is_stale:
        kind = "+".join(triggered + ["stale"]) if triggered else "stale"
    elif triggered:
        kind = "+".join(triggered)
    else:
        kind = "none"

    return AnomalyResult(
        is_anomaly=bool(triggered) or is_stale,
        z_score=max((signals[name] for name in triggered), default=0.0),
        kind=kind,
        signals=signals,
        stale=is_stale,
        stale_count=baseline.stale_count,
    )


def synthetic_anomalous_sample(db: Session, ticker: str, kind: str = "price") -> dict | None:
    """Read-only: computes a value that would cross the anomaly threshold
    for the given signal against the ticker's CURRENT baseline, without
    mutating it.

    Used by the manual test-trigger endpoint so a demo alert doesn't
    permanently skew real baseline stats with a synthetic data point —
    unlike check_anomaly, this never writes to ticker_baseline. "stale" is
    a multi-poll state rather than a single synthetic value, so it isn't
    supported here — it's exercised by the real poll cycle only.
    """
    baseline = db.get(TickerBaseline, ticker)
    if baseline is None or baseline.sample_count <= 5:
        return None

    if kind == "volume":
        effective_stddev = _effective_stddev(baseline.volume_stddev, max(baseline.volume_mean, 1.0))
        synthetic_value = baseline.volume_mean + effective_stddev * (settings.anomaly_zscore_threshold + 1)
        z_score = abs(synthetic_value - baseline.volume_mean) / effective_stddev
    elif kind == "spread":
        effective_stddev = _effective_stddev(baseline.spread_stddev, max(baseline.mean, 1.0))
        synthetic_value = baseline.spread_mean + effective_stddev * (settings.anomaly_zscore_threshold + 1)
        z_score = abs(synthetic_value - baseline.spread_mean) / effective_stddev
    elif kind == "volatility":
        effective_stddev = _effective_stddev(baseline.delta_stddev, max(baseline.mean, 1.0))
        synthetic_value = baseline.delta_mean + effective_stddev * (settings.anomaly_zscore_threshold + 1)
        z_score = abs(synthetic_value - baseline.delta_mean) / effective_stddev
    else:
        effective_stddev = _effective_stddev(baseline.stddev, baseline.mean)
        synthetic_value = baseline.mean + effective_stddev * (settings.anomaly_zscore_threshold + 1)
        z_score = abs(synthetic_value - baseline.mean) / effective_stddev

    return {"kind": kind, "value": synthetic_value, "z_score": z_score}
