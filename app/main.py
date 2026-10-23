"""FastAPI app: routes, WebSocket endpoint, and the lifespan hook that
creates tables, seeds the runtime-config/tracked-ticker singletons, and
starts the APScheduler poll job.
"""

import json
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import Depends, FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import asc, desc
from sqlalchemy.orm import Session

from app.alerts import alert_manager
from app.config import settings
from app.db import Base, SessionLocal, engine, get_session
from app.detector import current_zscores
from app.ingestion import fetch_latest_price
from app.models import Alert, PriceHistory, TickerBaseline
from app.runtime_config import get_runtime_config, update_runtime_config
from app.scheduler import poll_cycle, trigger_test_alert
from app.ticker_info import RANGE_CONFIG, fetch_fundamentals, fetch_history
from app.tracked_tickers import add_tracked_ticker, get_tracked_tickers, remove_tracked_ticker

scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: create tables, seed the RuntimeConfig/TrackedTicker
    singletons, and start the poll scheduler. Shutdown: stop the
    scheduler.
    """
    Base.metadata.create_all(bind=engine)

    # Seeds the RuntimeConfig singleton row once, before any request is
    # accepted - this is what makes the "two concurrent requests both find
    # it missing" race in get_runtime_config a non-issue in practice.
    db = SessionLocal()
    try:
        get_runtime_config(db)
        get_tracked_tickers(db)
    finally:
        db.close()

    scheduler.add_job(poll_cycle, "interval", minutes=settings.poll_interval_minutes, next_run_time=datetime.now())
    scheduler.start()
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(title="Market Anomaly Alerts", lifespan=lifespan)

STATIC_DIR = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def require_admin_token(x_debug_token: str | None = Header(default=None)) -> None:
    """Reject the request unless `X-Debug-Token` matches `DEBUG_TOKEN`.

    Attached to every endpoint that changes alerting behavior or sends a
    Slack message. Fails closed: if `DEBUG_TOKEN` isn't configured, every
    request is refused.

    Raises
    ------
    fastapi.HTTPException
        403 if the token is missing, wrong, or not configured.
    """
    expected = settings.debug_token
    supplied = x_debug_token or ""
    # compare_digest takes the same time wherever the first wrong character
    # is, so response timing can't leak the token one character at a time.
    # Compared as bytes because it raises TypeError on non-ASCII str input.
    if not expected or not secrets.compare_digest(supplied.encode(), expected.encode()):
        raise HTTPException(status_code=403, detail="forbidden")


admin_only = [Depends(require_admin_token)]


@app.get("/")
def dashboard():
    """Serve the dashboard HTML.

    The page holds no secrets: anyone can view it, and the admin actions
    ask the visitor for the token (see `require_admin_token`).

    Returns
    -------
    fastapi.responses.FileResponse
    """
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/admin/check", dependencies=admin_only)
def admin_check():
    """Let the dashboard verify an admin token before saving it.

    Returns
    -------
    dict
        `{"ok": True}`.
    """
    return {"ok": True}


@app.get("/health")
def health():
    """Liveness check for the host and for Pulse's market-hours uptime checks.

    Deliberately doesn't touch the database, so pinging it outside market
    hours keeps the web service awake without waking Postgres.

    Returns
    -------
    dict
        `{"status": "ok"}`.
    """
    return {"status": "ok"}


@app.get("/tickers")
def list_tickers(db: Session = Depends(get_session)):
    """Return the current tracked-ticker list and poll interval.

    Returns
    -------
    dict
        `{"tickers", "poll_interval_minutes"}`.
    """
    return {"tickers": get_tracked_tickers(db), "poll_interval_minutes": settings.poll_interval_minutes}


@app.post("/tickers/{ticker}", dependencies=admin_only)
async def add_ticker(ticker: str, db: Session = Depends(get_session)):
    """Adds a ticker to the tracked set (max 5). Validates that we can
    actually fetch live data for it BEFORE persisting - the same
    fetch_latest_price() call the real poller uses, so "valid" means
    "our actual ingestion pipeline works for this symbol", not just
    "looks like a ticker format"."""
    ticker = ticker.upper()
    try:
        await fetch_latest_price(ticker)
    except Exception:
        raise HTTPException(status_code=400, detail=f"couldn't fetch data for {ticker} - check the symbol is valid")
    try:
        tickers = add_tracked_ticker(db, ticker)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"tickers": tickers}


@app.delete("/tickers/{ticker}", dependencies=admin_only)
def remove_ticker(ticker: str, db: Session = Depends(get_session)):
    """Remove a ticker from the tracked set (min 1 remaining)."""
    try:
        tickers = remove_tracked_ticker(db, ticker.upper())
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"tickers": tickers}


@app.get("/alerts")
def list_alerts(limit: int = 50, db: Session = Depends(get_session)):
    """Return the most recent alerts, newest first.

    Returns
    -------
    list of dict
    """
    rows = db.query(Alert).order_by(desc(Alert.triggered_at)).limit(limit).all()
    return [
        {
            "id": a.id,
            "ticker": a.ticker,
            "triggered_at": a.triggered_at,
            "price": a.price,
            "z_score": a.z_score,
            "message": a.message,
        }
        for a in rows
    ]


@app.get("/alerts/{alert_id}")
def get_alert(alert_id: int, db: Session = Depends(get_session)):
    """Return one alert's full detail, including its parsed context
    snapshot and a +/-30 minute window of surrounding price history for
    the alert detail modal's context chart.

    Returns
    -------
    dict

    Raises
    ------
    fastapi.HTTPException
        404 if `alert_id` doesn't exist.
    """
    a = db.get(Alert, alert_id)
    if a is None:
        raise HTTPException(status_code=404, detail="alert not found")

    # A small window of price_history around the trigger time, for a
    # context chart - the same table every real poll already writes to.
    window = timedelta(minutes=30)
    nearby = (
        db.query(PriceHistory)
        .filter(
            PriceHistory.ticker == a.ticker,
            PriceHistory.timestamp >= a.triggered_at - window,
            PriceHistory.timestamp <= a.triggered_at + window,
        )
        .order_by(asc(PriceHistory.timestamp))
        .all()
    )

    return {
        "id": a.id,
        "ticker": a.ticker,
        "triggered_at": a.triggered_at,
        "price": a.price,
        "z_score": a.z_score,
        "message": a.message,
        "context": json.loads(a.context) if a.context else None,
        "nearby_prices": [{"date": str(p.timestamp), "close": p.price} for p in nearby],
    }


@app.get("/tickers/{ticker}/baseline")
def get_baseline(ticker: str, db: Session = Depends(get_session)):
    """Return a ticker's current baseline stats, backfilled for page load.

    Returns
    -------
    dict
    """
    baseline = db.get(TickerBaseline, ticker.upper())
    if baseline is None:
        return {"ticker": ticker.upper(), "sample_count": 0}

    # Lets the dashboard backfill price/volume/spread/z-score on page load
    # instead of showing blanks until the next WebSocket broadcast - all of
    # this is already sitting in the baseline row, just wasn't exposed here.
    zscores = current_zscores(baseline)
    config = get_runtime_config(db)
    triggered_z = [z for z in zscores.values() if z >= config.anomaly_zscore_threshold]

    return {
        "ticker": baseline.ticker,
        "mean": baseline.mean,
        "stddev": baseline.stddev,
        "sample_count": baseline.sample_count,
        "updated_at": baseline.updated_at,
        "last_price": baseline.last_price,
        "last_volume": baseline.last_volume,
        "last_spread": baseline.last_spread,
        "z_score": max(triggered_z, default=0.0),
    }


@app.get("/config")
def get_config(db: Session = Depends(get_session)):
    """Return the current runtime-adjustable alert thresholds."""
    config = get_runtime_config(db)
    return {
        "anomaly_zscore_threshold": config.anomaly_zscore_threshold,
        "stale_threshold": config.stale_threshold,
        "alert_cooldown_minutes": config.alert_cooldown_minutes,
        "reconciliation_tolerance": config.reconciliation_tolerance,
    }


@app.post("/config", dependencies=admin_only)
def post_config(
    anomaly_zscore_threshold: float | None = None,
    stale_threshold: int | None = None,
    alert_cooldown_minutes: int | None = None,
    reconciliation_tolerance: float | None = None,
    db: Session = Depends(get_session),
):
    """Runtime-adjustable alert thresholds - the only mutating endpoint
    besides /debug/test-alert that changes production alerting behavior,
    so it gets the same admin token. Once written, these values are the permanent
    source of truth; a later env var change has no effect (see
    app/runtime_config.py)."""
    try:
        config = update_runtime_config(
            db,
            anomaly_zscore_threshold=anomaly_zscore_threshold,
            stale_threshold=stale_threshold,
            alert_cooldown_minutes=alert_cooldown_minutes,
            reconciliation_tolerance=reconciliation_tolerance,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {
        "anomaly_zscore_threshold": config.anomaly_zscore_threshold,
        "stale_threshold": config.stale_threshold,
        "alert_cooldown_minutes": config.alert_cooldown_minutes,
        "reconciliation_tolerance": config.reconciliation_tolerance,
    }


@app.get("/tickers/{ticker}/history")
async def ticker_history(ticker: str, range: str = "1M"):
    """Return historical close prices for the ticker detail modal chart.

    Parameters
    ----------
    ticker : str
    range : str, optional
        A key of `RANGE_CONFIG` (e.g. "1D", "1M"), by default "1M".

    Raises
    ------
    fastapi.HTTPException
        400 if `range` isn't a valid `RANGE_CONFIG` key.
    """
    if range not in RANGE_CONFIG:
        raise HTTPException(status_code=400, detail=f"range must be one of {list(RANGE_CONFIG)}")
    points = await fetch_history(ticker.upper(), range)
    return {"ticker": ticker.upper(), "range": range, "points": points}


@app.get("/tickers/{ticker}/fundamentals")
async def ticker_fundamentals(ticker: str):
    """Return recent dividends and next-earnings info for the ticker
    detail modal."""
    return await fetch_fundamentals(ticker.upper())


TEST_ALERT_KINDS = ("price", "volume", "spread", "volatility")


@app.post("/debug/test-alert/{ticker}", dependencies=admin_only)
async def test_alert(ticker: str, kind: str = "price"):
    """Demo/testing only - fires a real alert through the same storage,
    broadcast, and Slack code path as a genuine detection, using a
    synthetic value instead of waiting on real market volatility.
    kind: "price" (default), "volume", "spread", or "volatility".
    ("stale" isn't synthesizable as a single value - it's a multi-poll
    state, exercised only by the real poll cycle.)

    Requires the admin token (see `require_admin_token`)."""
    if kind not in TEST_ALERT_KINDS:
        raise HTTPException(status_code=400, detail=f"kind must be one of {TEST_ALERT_KINDS}")
    try:
        return await trigger_test_alert(ticker.upper(), kind=kind)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.websocket("/ws/alerts")
async def ws_alerts(websocket: WebSocket):
    """Dashboard WebSocket: receives price_update/alert broadcasts,
    doesn't expect any inbound messages from the client."""
    await alert_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        alert_manager.disconnect(websocket)
