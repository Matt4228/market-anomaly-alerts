"""Anomaly detection: Welford's incremental algorithm applied to four
independent per-ticker series (price, volume, spread, tick-to-tick delta),
plus a separate stale/halted-quote check. See `check_anomaly` for the
live detection path and `synthetic_anomalous_sample` for the read-only
path used by the manual test-trigger endpoint.
"""

from dataclasses import dataclass, field

from sqlalchemy.orm import Session

from app.config import settings
from app.models import TickerBaseline


@dataclass
class AnomalyResult:
    """Outcome of a single `check_anomaly` (or synthetic-test) call.

    Attributes
    ----------
    is_anomaly : bool
    z_score : float
        Max z-score across triggered signals (0.0 if stale-only).
    kind : str
        "+"-joined triggered signal names, "stale", or "none".
    signals : dict of str to float
        Every computed z-score, not just the triggered ones.
    stale : bool
    stale_count : int
    baseline_snapshot : dict
        Mean/stddev per series at this point, for alert context capture.
    """

    is_anomaly: bool
    z_score: float
    kind: str
    signals: dict[str, float] = field(default_factory=dict)
    stale: bool = False
    stale_count: int = 0
    baseline_snapshot: dict = field(default_factory=dict)


def _effective_stddev(stddev: float, scale: float) -> float:
    """Floor a baseline stddev relative to the metric's own scale.

    Without this, a baseline with near-zero real variance (e.g. a
    data-quality issue, or a market that's been closed) turns any
    ordinary tick into a z-score in the millions/billions — dividing a
    real delta by a near-zero denominator, not a genuine anomaly.

    Parameters
    ----------
    stddev : float
        The baseline's actual sample standard deviation.
    scale : float
        Reference magnitude (typically the current price) the floor is
        computed relative to.

    Returns
    -------
    float
        `max(stddev, scale * settings.min_stddev_fraction)`.
    """
    return max(stddev, scale * settings.min_stddev_fraction)


def _welford_update(mean: float, variance_sum: float, count: int, value: float) -> tuple[float, float]:
    """One step of Welford's online mean/variance algorithm.

    Parameters
    ----------
    mean : float
        Running mean before this update.
    variance_sum : float
        Running sum of squared differences from the mean (M2).
    count : int
        Sample count *after* including `value`.
    value : float
        The new observation.

    Returns
    -------
    tuple of (float, float)
        Updated (mean, variance_sum).
    """
    delta = value - mean
    new_mean = mean + delta / count
    delta2 = value - new_mean
    new_variance_sum = variance_sum + delta * delta2
    return new_mean, new_variance_sum


def check_anomaly(
    db: Session,
    ticker: str,
    price: float,
    volume: float,
    bid: float,
    ask: float,
    zscore_threshold: float,
    stale_threshold: int,
) -> AnomalyResult:
    """Compare a new tick against the ticker's baseline, then update it.

    Compares price, volume, bid/ask spread, and tick-to-tick change
    magnitude against the baseline BEFORE folding them in, then updates
    all four via Welford's algorithm. Checking first matters: scoring
    against a baseline that already includes the current point would
    dampen the very deviation we're trying to detect.

    All four series share one sample_count, since they're always observed
    together (one poll = one price + volume + bid + ask reading).

    Parameters
    ----------
    db : sqlalchemy.orm.Session
    ticker : str
    price : float
    volume : float
    bid : float
    ask : float
    zscore_threshold : float
        Threshold above which a signal counts as anomalous. Passed in
        (from RuntimeConfig, via the caller) rather than read from
        settings directly, so it can be adjusted live from the dashboard
        without a restart.
    stale_threshold : int
        Consecutive unchanged-tick count above which the ticker is
        flagged as stale/halted.

    Returns
    -------
    AnomalyResult
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
            last_volume=volume,
            last_spread=spread,
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
    baseline.last_volume = volume
    baseline.last_spread = spread
    baseline.stale_count = baseline.stale_count + 1 if is_stale_tick else 0
    db.commit()

    enough_samples = baseline.sample_count > 5
    triggered = [name for name, z in signals.items() if enough_samples and z >= zscore_threshold]
    is_stale = enough_samples and baseline.stale_count >= stale_threshold

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
        baseline_snapshot={
            "mean": baseline.mean,
            "stddev": baseline.stddev,
            "volume_mean": baseline.volume_mean,
            "volume_stddev": baseline.volume_stddev,
            "spread_mean": baseline.spread_mean,
            "spread_stddev": baseline.spread_stddev,
            "sample_count": baseline.sample_count,
        },
    )


def current_zscores(baseline: TickerBaseline) -> dict[str, float]:
    """Recompute price/volume/spread z-scores from the baseline's own
    last-known values, without touching the database.

    For showing "how anomalous does the most recent tick look" on page
    load, before the next real poll's WebSocket broadcast would
    otherwise be the only source of this.

    Slightly different from the z-score computed live in `check_anomaly`:
    that one compares an incoming point against the baseline BEFORE
    folding it in, whereas this compares the baseline's last point
    against its own current (already-updated) stats — a small
    self-referential difference that doesn't matter for a display/
    backfill purpose like this one.

    Parameters
    ----------
    baseline : TickerBaseline

    Returns
    -------
    dict of str to float
        Empty dict if there isn't enough baseline data yet.
    """
    if baseline.sample_count < 2 or baseline.last_price is None:
        return {}
    return {
        "price": abs(baseline.last_price - baseline.mean) / _effective_stddev(baseline.stddev, baseline.last_price),
        "volume": abs((baseline.last_volume or 0) - baseline.volume_mean)
        / _effective_stddev(baseline.volume_stddev, max(baseline.last_volume or 0, 1.0)),
        "spread": abs((baseline.last_spread or 0) - baseline.spread_mean)
        / _effective_stddev(baseline.spread_stddev, max(baseline.last_price, 1.0)),
    }


def synthetic_anomalous_sample(db: Session, ticker: str, zscore_threshold: float, kind: str = "price") -> dict:
    """Compute a value that would cross the anomaly threshold for a given
    signal, against the ticker's current baseline, without mutating it.

    Used by the manual test-trigger endpoint so a demo alert doesn't
    permanently skew real baseline stats with a synthetic data point —
    unlike `check_anomaly`, this never writes to `ticker_baseline`.
    "stale" is a multi-poll state rather than a single synthetic value,
    so it isn't supported here — it's exercised by the real poll cycle
    only.

    Parameters
    ----------
    db : sqlalchemy.orm.Session
    ticker : str
    zscore_threshold : float
        The synthetic value is sized to land one std dev past this.
    kind : {"price", "volume", "spread", "volatility"}, optional
        Which signal to synthesize, by default "price".

    Returns
    -------
    dict
        `{"kind", "value", "z_score"}` on success, or
        `{"error", "sample_count"}` if there isn't enough baseline data
        yet — callers use this to report real progress ("3/6 samples so
        far") instead of a flat "no baseline" message.
    """
    baseline = db.get(TickerBaseline, ticker)
    if baseline is None:
        return {"error": "no_baseline", "sample_count": 0}
    if baseline.sample_count <= 5:
        return {"error": "insufficient_samples", "sample_count": baseline.sample_count}

    if kind == "volume":
        effective_stddev = _effective_stddev(baseline.volume_stddev, max(baseline.volume_mean, 1.0))
        synthetic_value = baseline.volume_mean + effective_stddev * (zscore_threshold + 1)
        z_score = abs(synthetic_value - baseline.volume_mean) / effective_stddev
    elif kind == "spread":
        effective_stddev = _effective_stddev(baseline.spread_stddev, max(baseline.mean, 1.0))
        synthetic_value = baseline.spread_mean + effective_stddev * (zscore_threshold + 1)
        z_score = abs(synthetic_value - baseline.spread_mean) / effective_stddev
    elif kind == "volatility":
        effective_stddev = _effective_stddev(baseline.delta_stddev, max(baseline.mean, 1.0))
        synthetic_value = baseline.delta_mean + effective_stddev * (zscore_threshold + 1)
        z_score = abs(synthetic_value - baseline.delta_mean) / effective_stddev
    else:
        effective_stddev = _effective_stddev(baseline.stddev, baseline.mean)
        synthetic_value = baseline.mean + effective_stddev * (zscore_threshold + 1)
        z_score = abs(synthetic_value - baseline.mean) / effective_stddev

    return {"kind": kind, "value": synthetic_value, "z_score": z_score}
