import logging

from app.alerts import alert_manager
from app.config import settings
from app.db import SessionLocal
from app.detector import AnomalyResult, check_anomaly, synthetic_anomalous_sample
from app.ingestion import fetch_latest_price, fetch_reconciliation_price
from app.models import Alert, PriceHistory

logger = logging.getLogger(__name__)


async def poll_cycle() -> None:
    for ticker in settings.tickers:
        try:
            price_data = await fetch_latest_price(ticker)
            await process_price(ticker, price_data, persist_price_history=True)
            await check_reconciliation(ticker, price_data["price"])
        except Exception:
            # One ticker's provider hiccup shouldn't take down the whole cycle.
            logger.exception("poll failed for %s", ticker)


async def check_reconciliation(ticker: str, primary_price: float) -> None:
    """Cross-checks the primary (OpenBB) price against a second,
    independently-fetched reading (see ingestion.fetch_reconciliation_price
    for the honest caveat on how independent that really is). Uses its own
    cooldown key so a reconciliation mismatch never competes with, or gets
    suppressed by, a price/volume/spread/volatility alert's cooldown."""
    recon_price = await fetch_reconciliation_price(ticker)
    if recon_price is None:
        return

    pct_diff = abs(primary_price - recon_price) / primary_price
    if pct_diff < settings.reconciliation_tolerance:
        return

    cooldown_key = f"{ticker}:reconciliation"
    if not alert_manager.should_alert(cooldown_key):
        return

    message = (
        f"{ticker} price reconciliation mismatch: primary={primary_price:.2f} vs "
        f"independent={recon_price:.2f} ({pct_diff * 100:.1f}% diff)"
    )

    db = SessionLocal()
    try:
        db.add(Alert(ticker=ticker, price=primary_price, z_score=pct_diff * 100, message=message))
        db.commit()
    finally:
        db.close()

    alert_manager.record_alert(cooldown_key)
    await alert_manager.broadcast(
        {"type": "alert", "ticker": ticker, "price": primary_price, "z_score": pct_diff * 100, "message": message}
    )
    await alert_manager.notify_slack(message)


def _describe_signal(name: str, z: float, price: float, volume: float, spread: float) -> str:
    if name == "price":
        return f"price moved {z:.1f} std devs (price={price:.2f})"
    if name == "volume":
        return f"volume spiked {z:.1f} std devs (volume={volume:,.0f})"
    if name == "spread":
        return f"bid/ask spread widened {z:.1f} std devs (spread={spread:.2f})"
    if name == "volatility":
        return f"tick-to-tick volatility {z:.1f} std devs above normal"
    return f"{name} z={z:.1f}"


def _build_message(
    ticker: str, result: AnomalyResult, price: float, volume: float, spread: float = 0.0, prefix: str = ""
) -> str:
    if result.stale:
        base = f"{ticker} looks halted/stale — no price movement and zero volume for {result.stale_count} consecutive polls"
        triggered_others = [name for name in result.signals if name in result.kind.split("+")]
        if triggered_others:
            extra = "; ".join(_describe_signal(n, result.signals[n], price, volume, spread) for n in triggered_others)
            return f"{prefix}{base} (also: {extra})"
        return f"{prefix}{base}"

    triggered = [name for name in result.signals if result.signals[name] >= settings.anomaly_zscore_threshold]
    descriptions = "; ".join(_describe_signal(n, result.signals[n], price, volume, spread) for n in triggered)
    return f"{prefix}{ticker} {descriptions}"


async def process_price(ticker: str, price_data: dict, *, persist_price_history: bool) -> AnomalyResult:
    """Runs detection + alert dispatch for a given price/volume/bid/ask
    point, and broadcasts a live tick regardless of whether it's an anomaly.

    Shared by the real poller and the manual test-trigger endpoint, so
    both go through the exact same detection/alert code path — the test
    endpoint exercises the real pipeline, not a separate mock of it.
    """
    price = price_data["price"]
    volume = price_data["volume"]
    bid = price_data.get("bid", price)
    ask = price_data.get("ask", price)
    spread = ask - bid

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

        result = check_anomaly(db, ticker, price, volume, bid, ask)
    finally:
        db.close()

    await alert_manager.broadcast(
        {
            "type": "price_update",
            "ticker": ticker,
            "price": price,
            "volume": volume,
            "spread": spread,
            "z_score": result.z_score,
            "kind": result.kind,
        }
    )

    if not result.is_anomaly or not alert_manager.should_alert(ticker):
        return result

    message = _build_message(ticker, result, price, volume, spread)

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
    synthetic value computed against the ticker's current baseline — for
    demoing the alert path on demand (any signal type except "stale", which
    is a multi-poll state rather than a single synthetic value) rather than
    waiting on real market volatility to cross the threshold.

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
    result = AnomalyResult(is_anomaly=True, z_score=z_score, kind=kind, signals={kind: z_score})

    price = synthetic_value if kind == "price" else 0.0
    volume = synthetic_value if kind == "volume" else 0.0
    spread = synthetic_value if kind == "spread" else 0.0
    message = _build_message(ticker, result, price, volume, spread, prefix="[TEST] ")

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
