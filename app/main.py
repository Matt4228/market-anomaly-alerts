import json
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import Depends, FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from sqlalchemy import asc, desc
from sqlalchemy.orm import Session

from app.alerts import alert_manager
from app.config import settings
from app.db import Base, SessionLocal, engine, get_session
from app.detector import current_zscores
from app.models import Alert, PriceHistory, TickerBaseline
from app.runtime_config import get_runtime_config, update_runtime_config
from app.scheduler import poll_cycle, trigger_test_alert
from app.ticker_info import RANGE_CONFIG, fetch_fundamentals, fetch_history

scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)

    # Seeds the RuntimeConfig singleton row once, before any request is
    # accepted — this is what makes the "two concurrent requests both find
    # it missing" race in get_runtime_config a non-issue in practice.
    db = SessionLocal()
    try:
        get_runtime_config(db)
    finally:
        db.close()

    scheduler.add_job(poll_cycle, "interval", minutes=settings.poll_interval_minutes, next_run_time=datetime.now())
    scheduler.start()
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(title="Market Anomaly Alerts", lifespan=lifespan)

STATIC_DIR = Path(__file__).parent / "static"


@app.get("/")
def dashboard():
    # Rendered, not served as a static file: the debug token placeholder is
    # substituted from settings at request time, so the real secret only
    # ever lives in the environment, never in the committed HTML/git repo.
    html = (STATIC_DIR / "index.html").read_text()
    html = html.replace("__DEBUG_TOKEN__", settings.debug_token or "")
    return HTMLResponse(html)


@app.get("/tickers")
def list_tickers():
    return {"tickers": settings.tickers, "poll_interval_minutes": settings.poll_interval_minutes}


@app.get("/alerts")
def list_alerts(limit: int = 50, db: Session = Depends(get_session)):
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
    a = db.get(Alert, alert_id)
    if a is None:
        raise HTTPException(status_code=404, detail="alert not found")

    # A small window of price_history around the trigger time, for a
    # context chart — the same table every real poll already writes to.
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
    baseline = db.get(TickerBaseline, ticker.upper())
    if baseline is None:
        return {"ticker": ticker.upper(), "sample_count": 0}

    # Lets the dashboard backfill price/volume/spread/z-score on page load
    # instead of showing blanks until the next WebSocket broadcast — all of
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
    config = get_runtime_config(db)
    return {
        "anomaly_zscore_threshold": config.anomaly_zscore_threshold,
        "stale_threshold": config.stale_threshold,
        "alert_cooldown_minutes": config.alert_cooldown_minutes,
        "reconciliation_tolerance": config.reconciliation_tolerance,
    }


@app.post("/config")
def post_config(
    anomaly_zscore_threshold: float | None = None,
    stale_threshold: int | None = None,
    alert_cooldown_minutes: int | None = None,
    reconciliation_tolerance: float | None = None,
    x_debug_token: str | None = Header(default=None),
    db: Session = Depends(get_session),
):
    """Runtime-adjustable alert thresholds — the only mutating endpoint
    besides /debug/test-alert that changes production alerting behavior,
    so it gets the same auth. Once written, these values are the permanent
    source of truth; a later env var change has no effect (see
    app/runtime_config.py)."""
    if not settings.debug_token or x_debug_token != settings.debug_token:
        raise HTTPException(status_code=403, detail="forbidden")
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
    if range not in RANGE_CONFIG:
        raise HTTPException(status_code=400, detail=f"range must be one of {list(RANGE_CONFIG)}")
    points = await fetch_history(ticker.upper(), range)
    return {"ticker": ticker.upper(), "range": range, "points": points}


@app.get("/tickers/{ticker}/fundamentals")
async def ticker_fundamentals(ticker: str):
    return await fetch_fundamentals(ticker.upper())


TEST_ALERT_KINDS = ("price", "volume", "spread", "volatility")


@app.post("/debug/test-alert/{ticker}")
async def test_alert(ticker: str, kind: str = "price", x_debug_token: str | None = Header(default=None)):
    """Demo/testing only — fires a real alert through the same storage,
    broadcast, and Slack code path as a genuine detection, using a
    synthetic value instead of waiting on real market volatility.
    kind: "price" (default), "volume", "spread", or "volatility".
    ("stale" isn't synthesizable as a single value — it's a multi-poll
    state, exercised only by the real poll cycle.)

    Requires X-Debug-Token to match DEBUG_TOKEN. Fails closed: if
    DEBUG_TOKEN isn't configured, this endpoint refuses every request
    rather than being silently open."""
    if not settings.debug_token or x_debug_token != settings.debug_token:
        raise HTTPException(status_code=403, detail="forbidden")
    if kind not in TEST_ALERT_KINDS:
        raise HTTPException(status_code=400, detail=f"kind must be one of {TEST_ALERT_KINDS}")
    try:
        return await trigger_test_alert(ticker.upper(), kind=kind)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.websocket("/ws/alerts")
async def ws_alerts(websocket: WebSocket):
    await alert_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        alert_manager.disconnect(websocket)
