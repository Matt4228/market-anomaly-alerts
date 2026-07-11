import logging

from app.alerts import alert_manager
from app.config import settings
from app.db import SessionLocal
from app.detector import AnomalyResult, check_anomaly, synthetic_anomalous_sample
from app.ingestion import fetch_latest_price
from app.models import Alert, PriceHistory

logger = logging.getLogger(__name__)


async def poll_cycle() -> None:
    for ticker in settings.tickers:
        try:
            price_data = await fetch_latest_price(ticker)
            await process_price(ticker, price_data, persist_price_history=True)
        except Exception:
            # One ticker's provider hiccup shouldn't take down the whole cycle.
            logger.exception("poll failed for %s", ticker)


def _build_message(ticker: str, result: AnomalyResult, price: float, volume: float, prefix: str = "") -> str:
    if result.kind == "price+volume":
        return f"{prefix}{ticker} price+volume both anomalous (price z={result.price_z:.1f}, volume z={result.volume_z:.1f})"
    if result.kind == "volume":
        return f"{prefix}{ticker} volume spiked {result.volume_z:.1f} std devs from baseline (volume={volume:,.0f})"
    return f"{prefix}{ticker} price moved {result.price_z:.1f} std devs from baseline (price={price:.2f})"


async def process_price(ticker: str, price_data: dict, *, persist_price_history: bool) -> AnomalyResult:
    """Runs detection + alert dispatch for a given price/volume point, and
    broadcasts a live tick regardless of whether it's an anomaly.

    Shared by the real poller and the manual test-trigger endpoint, so
    both go through the exact same detection/alert code path — the test
    endpoint exercises the real pipeline, not a separate mock of it.
    """
    price = price_data["price"]
    volume = price_data["volume"]

    db = SessionLocal()
    try:
        if persist_price_history:
            db.add(
                PriceHistory(
                    ticker=ticker,
                    timestamp=price_data["timestamp"],
                    price=price,
                    volume=volume,
                    source=price_data["source"],
                )
            )
            db.commit()

        result = check_anomaly(db, ticker, price, volume)
    finally:
        db.close()

    await alert_manager.broadcast(
        {
            "type": "price_update",
            "ticker": ticker,
            "price": price,
            "volume": volume,
            "z_score": result.z_score,
            "kind": result.kind,
        }
    )

    if not result.is_anomaly or not alert_manager.should_alert(ticker):
        return result

    message = _build_message(ticker, result, price, volume)

    db = SessionLocal()
    try:
        db.add(Alert(ticker=ticker, price=price, z_score=result.z_score, message=message))
        db.commit()
    finally:
        db.close()

    alert_manager.record_alert(ticker)
    await alert_manager.broadcast(
        {"type": "alert", "ticker": ticker, "price": price, "z_score": result.z_score, "message": message}
    )
    await alert_manager.notify_slack(message)
    return result


async def trigger_test_alert(ticker: str, kind: str = "price") -> dict:
    """Fires a real alert (stored, broadcast, Slack-notified) using a
    synthetic price or volume computed against the ticker's current
    baseline — for demoing the alert path on demand (either signal type)
    rather than waiting on real market volatility to cross the threshold.

    Deliberately bypasses alert_manager.should_alert() on the way in (a
    manual trigger should always fire when called), but still calls
    record_alert() afterward so it participates in cooldown bookkeeping
    same as a real alert — calling this repeatedly won't spam duplicates.
    """
    db = SessionLocal()
    try:
        sample = synthetic_anomalous_sample(db, ticker, kind=kind)
    finally:
        db.close()

    if sample is None:
        raise ValueError(f"no baseline yet for {ticker} — wait for a few real poll cycles first")

    synthetic_value = sample["value"]
    z_score = sample["z_score"]
    result = AnomalyResult(
        is_anomaly=True,
        z_score=z_score,
        kind=kind,
        price_z=z_score if kind == "price" else 0.0,
        volume_z=z_score if kind == "volume" else 0.0,
    )
    price = synthetic_value if kind == "price" else 0.0
    volume = synthetic_value if kind == "volume" else 0.0
    message = _build_message(ticker, result, price, volume, prefix="[TEST] ")

    db = SessionLocal()
    try:
        db.add(Alert(ticker=ticker, price=price, z_score=z_score, message=message))
        db.commit()
    finally:
        db.close()

    alert_manager.record_alert(ticker)
    await alert_manager.broadcast({"type": "alert", "ticker": ticker, "price": price, "z_score": z_score, "message": message})
    await alert_manager.notify_slack(message)
    return {"ticker": ticker, "kind": kind, "synthetic_value": synthetic_value, "z_score": z_score, "message": message}
