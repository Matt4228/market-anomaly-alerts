"""Background poll cycle: fetch each tracked ticker's latest price, run
anomaly detection, cross-check reconciliation, and dispatch alerts.
"""

import json
import logging

from app.alerts import alert_manager
from app.db import SessionLocal
from app.detector import AnomalyResult, check_anomaly, synthetic_anomalous_sample
from app.ingestion import fetch_latest_price, fetch_reconciliation_price
from app.models import Alert, PriceHistory
from app.runtime_config import get_runtime_config
from app.tracked_tickers import get_tracked_tickers

logger = logging.getLogger(__name__)


async def poll_cycle() -> None:
    """Fetch and process one price tick for every currently tracked ticker.

    Run on a schedule by APScheduler (see `app/main.py`). Each ticker's
    failure is caught and logged individually so one provider hiccup
    doesn't take down the whole cycle.
    """
    # Read fresh each cycle (not cached) so adding/removing a ticker via the
    # dashboard takes effect on the next poll, not just after a restart -
    # same reasoning as RuntimeConfig being read fresh per operation.
    db = SessionLocal()
    try:
        tickers = get_tracked_tickers(db)
    finally:
        db.close()

    for ticker in tickers:
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
    suppressed by, a price/volume/spread/volatility alert's cooldown.

    Parameters
    ----------
    ticker : str
    primary_price : float
        Price already fetched this cycle via the primary provider path.
    """
    recon_price = await fetch_reconciliation_price(ticker)
    if recon_price is None:
        return

    db = SessionLocal()
    try:
        config = get_runtime_config(db)
    finally:
        db.close()

    pct_diff = abs(primary_price - recon_price) / primary_price
    if pct_diff < config.reconciliation_tolerance:
        return

    cooldown_key = f"{ticker}:reconciliation"
    if not alert_manager.should_alert(cooldown_key, config.alert_cooldown_minutes):
        return

    message = (
        f"{ticker} price reconciliation mismatch: primary={primary_price:.2f} vs "
        f"independent={recon_price:.2f} ({pct_diff * 100:.1f}% diff)"
    )
    context = json.dumps(
        {
            "kind": "reconciliation",
            "primary_price": primary_price,
            "independent_price": recon_price,
            "pct_diff": pct_diff,
            "tolerance": config.reconciliation_tolerance,
        }
    )

    db = SessionLocal()
    try:
        alert_row = Alert(ticker=ticker, price=primary_price, z_score=pct_diff * 100, message=message, context=context)
        db.add(alert_row)
        db.commit()
        alert_id = alert_row.id
    finally:
        db.close()

    alert_manager.record_alert(cooldown_key)
    await alert_manager.broadcast(
        {
            "type": "alert",
            "id": alert_id,
            "ticker": ticker,
            "price": primary_price,
            "z_score": pct_diff * 100,
            "message": message,
        }
    )
    await alert_manager.notify_slack(message)


def _describe_signal(name: str, z: float, price: float, volume: float, spread: float) -> str:
    """Render one triggered signal as a human-readable phrase for the
    alert message.

    Parameters
    ----------
    name : {"price", "volume", "spread", "volatility"}
    z : float
    price, volume, spread : float

    Returns
    -------
    str
    """
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
    ticker: str,
    result: AnomalyResult,
    price: float,
    volume: float,
    spread: float,
    zscore_threshold: float,
    prefix: str = "",
) -> str:
    """Build the human-readable alert message for a detection result.

    Parameters
    ----------
    ticker : str
    result : AnomalyResult
    price, volume, spread : float
    zscore_threshold : float
        Used to decide which signals count as "triggered" for the message.
    prefix : str, optional
        Prepended to the message, e.g. `"[TEST] "` for synthetic alerts.

    Returns
    -------
    str
    """
    if result.stale:
        base = f"{ticker} looks halted/stale - no price movement and zero volume for {result.stale_count} consecutive polls"
        triggered_others = [name for name in result.signals if name in result.kind.split("+")]
        if triggered_others:
            extra = "; ".join(_describe_signal(n, result.signals[n], price, volume, spread) for n in triggered_others)
            return f"{prefix}{base} (also: {extra})"
        return f"{prefix}{base}"

    triggered = [name for name in result.signals if result.signals[name] >= zscore_threshold]
    descriptions = "; ".join(_describe_signal(n, result.signals[n], price, volume, spread) for n in triggered)
    return f"{prefix}{ticker} {descriptions}"


async def process_price(ticker: str, price_data: dict, *, persist_price_history: bool) -> AnomalyResult:
    """Runs detection + alert dispatch for a given price/volume/bid/ask
    point, and broadcasts a live tick regardless of whether it's an anomaly.

    Shared by the real poller and the manual test-trigger endpoint, so
    both go through the exact same detection/alert code path - the test
    endpoint exercises the real pipeline, not a separate mock of it.

    Parameters
    ----------
    ticker : str
    price_data : dict
        `{"price", "volume", "bid", "ask", "timestamp", "source"}` as
        returned by `ingestion.fetch_latest_price`.
    persist_price_history : bool
        Whether to write a `PriceHistory` row for this tick (the real
        poller does; some callers may not want to grow that table).

    Returns
    -------
    AnomalyResult
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

        config = get_runtime_config(db)
        result = check_anomaly(
            db, ticker, price, volume, bid, ask, config.anomaly_zscore_threshold, config.stale_threshold
        )
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

    if not result.is_anomaly or not alert_manager.should_alert(ticker, config.alert_cooldown_minutes):
        return result

    message = _build_message(ticker, result, price, volume, spread, config.anomaly_zscore_threshold)
    context = json.dumps(
        {
            "kind": result.kind,
            "signals": result.signals,
            "stale_count": result.stale_count,
            "baseline": result.baseline_snapshot,
            "price": price,
            "volume": volume,
            "spread": spread,
        }
    )

    db = SessionLocal()
    try:
        alert_row = Alert(ticker=ticker, price=price, z_score=result.z_score, message=message, context=context)
        db.add(alert_row)
        db.commit()
        alert_id = alert_row.id
    finally:
        db.close()

    alert_manager.record_alert(ticker)
    await alert_manager.broadcast(
        {"type": "alert", "id": alert_id, "ticker": ticker, "price": price, "z_score": result.z_score, "message": message}
    )
    await alert_manager.notify_slack(message)
    return result


async def trigger_test_alert(ticker: str, kind: str = "price") -> dict:
    """Fires a real alert (stored, broadcast, Slack-notified) using a
    synthetic value computed against the ticker's current baseline - for
    demoing the alert path on demand (any signal type except "stale", which
    is a multi-poll state rather than a single synthetic value) rather than
    waiting on real market volatility to cross the threshold.

    Deliberately bypasses alert_manager.should_alert() on the way in (a
    manual trigger should always fire when called), but still calls
    record_alert() afterward so it participates in cooldown bookkeeping
    same as a real alert - calling this repeatedly won't spam duplicates.

    Parameters
    ----------
    ticker : str
    kind : {"price", "volume", "spread", "volatility"}, optional
        Which signal to synthesize, by default "price".

    Returns
    -------
    dict
        `{"ticker", "kind", "synthetic_value", "z_score", "message"}`.

    Raises
    ------
    ValueError
        If there's no baseline yet, or too few samples to synthesize
        against safely.
    """
    db = SessionLocal()
    try:
        config = get_runtime_config(db)
        sample = synthetic_anomalous_sample(db, ticker, config.anomaly_zscore_threshold, kind=kind)
    finally:
        db.close()

    if "error" in sample:
        if sample["error"] == "no_baseline":
            raise ValueError(f"no baseline yet for {ticker} - wait for the first real poll cycle")
        raise ValueError(
            f"{ticker} has {sample['sample_count']}/6 samples so far - wait for a few more real poll cycles"
        )

    synthetic_value = sample["value"]
    z_score = sample["z_score"]
    result = AnomalyResult(is_anomaly=True, z_score=z_score, kind=kind, signals={kind: z_score})

    price = synthetic_value if kind == "price" else 0.0
    volume = synthetic_value if kind == "volume" else 0.0
    spread = synthetic_value if kind == "spread" else 0.0
    message = _build_message(ticker, result, price, volume, spread, config.anomaly_zscore_threshold, prefix="[TEST] ")
    context = json.dumps({"kind": kind, "synthetic_value": synthetic_value, "z_score": z_score, "is_test": True})

    db = SessionLocal()
    try:
        alert_row = Alert(ticker=ticker, price=price, z_score=z_score, message=message, context=context)
        db.add(alert_row)
        db.commit()
        alert_id = alert_row.id
    finally:
        db.close()

    alert_manager.record_alert(ticker)
    await alert_manager.broadcast(
        {"type": "alert", "id": alert_id, "ticker": ticker, "price": price, "z_score": z_score, "message": message}
    )
    await alert_manager.notify_slack(message)
    return {"ticker": ticker, "kind": kind, "synthetic_value": synthetic_value, "z_score": z_score, "message": message}
