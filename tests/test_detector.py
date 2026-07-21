import pytest

from app.detector import (
    _effective_stddev,
    _welford_update,
    check_anomaly,
    current_zscores,
    synthetic_anomalous_sample,
)
from app.models import TickerBaseline


def _tick(db_session, price, volume=1000.0, bid=99.5, ask=100.5, zscore_threshold=3.0, stale_threshold=3):
    return check_anomaly(
        db_session, "AAPL", price=price, volume=volume, bid=bid, ask=ask,
        zscore_threshold=zscore_threshold, stale_threshold=stale_threshold,
    )


def test_effective_stddev_floors_near_zero_variance():
    assert _effective_stddev(stddev=0.0, scale=100.0) == 100.0 * 0.0005


def test_effective_stddev_keeps_real_stddev_when_above_the_floor():
    assert _effective_stddev(stddev=5.0, scale=100.0) == 5.0


def test_welford_update_matches_known_mean_and_sample_variance():
    # 2, 4, 4, 4, 5, 5, 7, 9 -> mean=5.0, sample variance (ddof=1)=32/7
    values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]
    mean, variance_sum = 0.0, 0.0
    for i, value in enumerate(values, start=1):
        mean, variance_sum = _welford_update(mean, variance_sum, i, value)
    assert mean == 5.0
    assert variance_sum / (len(values) - 1) == pytest.approx(32 / 7)


def test_check_anomaly_first_tick_seeds_baseline_without_flagging(db_session):
    result = _tick(db_session, price=100.0)
    assert result.is_anomaly is False
    assert result.kind == "none"
    baseline = db_session.get(TickerBaseline, "AAPL")
    assert baseline is not None
    assert baseline.sample_count == 1


def test_check_anomaly_does_not_trigger_before_enough_samples(db_session):
    # Seed tick (count=1) + 3 normal ticks (count->4). A big deviation on
    # the next tick brings count to 5, but enough_samples requires >5, so
    # it must not trigger even though the z-score itself would be huge.
    _tick(db_session, price=100.0)
    for price in [100.1, 99.9, 100.05]:
        _tick(db_session, price=price)
    result = _tick(db_session, price=500.0)
    assert result.is_anomaly is False
    assert db_session.get(TickerBaseline, "AAPL").sample_count == 5


def test_check_anomaly_triggers_price_signal_past_threshold(db_session):
    # Seed tick + 4 normal ticks brings count to 5; the next tick brings
    # it to 6 (>5), so the gate opens and the big deviation should trigger.
    _tick(db_session, price=100.0)
    for price in [100.1, 99.9, 100.05, 99.95]:
        _tick(db_session, price=price)
    result = _tick(db_session, price=500.0)
    assert result.is_anomaly is True
    assert "price" in result.kind
    assert db_session.get(TickerBaseline, "AAPL").sample_count == 6


def test_check_anomaly_flags_stale_after_threshold_unchanged_zero_volume_ticks(db_session):
    _tick(db_session, price=100.0)
    for price in [100.1, 99.9, 100.05, 99.95]:
        _tick(db_session, price=price)
    result = None
    for _ in range(3):
        result = _tick(db_session, price=99.95, volume=0.0)
    assert result.stale is True
    assert "stale" in result.kind


def test_current_zscores_is_empty_before_two_samples():
    baseline = TickerBaseline(ticker="AAPL", mean=100.0, variance_sum=0.0, sample_count=1, last_price=100.0)
    assert current_zscores(baseline) == {}


def test_synthetic_anomalous_sample_errors_with_no_baseline(db_session):
    result = synthetic_anomalous_sample(db_session, "AAPL", zscore_threshold=3.0)
    assert result == {"error": "no_baseline", "sample_count": 0}


def test_synthetic_anomalous_sample_errors_with_insufficient_samples(db_session):
    _tick(db_session, price=100.0)
    _tick(db_session, price=100.1)
    result = synthetic_anomalous_sample(db_session, "AAPL", zscore_threshold=3.0)
    assert result == {"error": "insufficient_samples", "sample_count": 2}


def test_synthetic_anomalous_sample_price_kind_lands_past_threshold(db_session):
    _tick(db_session, price=100.0)
    for price in [100.1, 99.9, 100.05, 99.95, 100.02]:
        _tick(db_session, price=price)
    result = synthetic_anomalous_sample(db_session, "AAPL", zscore_threshold=3.0, kind="price")
    assert result["kind"] == "price"
    assert result["z_score"] > 3.0


def test_synthetic_anomalous_sample_does_not_mutate_the_baseline(db_session):
    _tick(db_session, price=100.0)
    for price in [100.1, 99.9, 100.05, 99.95, 100.02]:
        _tick(db_session, price=price)
    before = db_session.get(TickerBaseline, "AAPL").sample_count
    synthetic_anomalous_sample(db_session, "AAPL", zscore_threshold=3.0, kind="volume")
    after = db_session.get(TickerBaseline, "AAPL").sample_count
    assert before == after
