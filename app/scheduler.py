import logging

from app.alerts import alert_manager
from app.config import settings
from app.db import SessionLocal
from app.detector import check_anomaly
from app.ingestion import fetch_latest_price
from app.models import Alert, PriceHistory

logger = logging.getLogger(__name__)


async def poll_cycle() -> None:
    for ticker in settings.tickers:
        try:
            await _poll_ticker(ticker)
        except Exception:
            # One ticker's provider hiccup shouldn't take down the whole cycle.
            logger.exception("poll failed for %s", ticker)


async def _poll_ticker(ticker: str) -> None:
    price_data = await fetch_latest_price(ticker)

    db = SessionLocal()
    try:
        db.add(
            PriceHistory(
                ticker=ticker,
                timestamp=price_data["timestamp"],
                price=price_data["price"],
                volume=price_data["volume"],
                source=price_data["source"],
            )
        )
        db.commit()

        is_anomaly, z_score = check_anomaly(db, ticker, price_data["price"])
    finally:
        db.close()

    if not is_anomaly or not alert_manager.should_alert(ticker):
        return

    message = f"{ticker} moved {z_score:.1f} std devs from baseline (price={price_data['price']:.2f})"

    db = SessionLocal()
    try:
        db.add(Alert(ticker=ticker, price=price_data["price"], z_score=z_score, message=message))
        db.commit()
    finally:
        db.close()

    alert_manager.record_alert(ticker)
    await alert_manager.broadcast(
        {"ticker": ticker, "price": price_data["price"], "z_score": z_score, "message": message}
    )
    await alert_manager.notify_slack(message)
