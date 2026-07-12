from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import Depends, FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.alerts import alert_manager
from app.config import settings
from app.db import Base, engine, get_session
from app.detector import current_zscores
from app.models import Alert, TickerBaseline
from app.scheduler import poll_cycle, trigger_test_alert

scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
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
            "ticker": a.ticker,
            "triggered_at": a.triggered_at,
            "price": a.price,
            "z_score": a.z_score,
            "message": a.message,
        }
        for a in rows
    ]


@app.get("/tickers/{ticker}/baseline")
def get_baseline(ticker: str, db: Session = Depends(get_session)):
    baseline = db.get(TickerBaseline, ticker.upper())
    if baseline is None:
        return {"ticker": ticker.upper(), "sample_count": 0}

    # Lets the dashboard backfill price/volume/spread/z-score on page load
    # instead of showing blanks until the next WebSocket broadcast — all of
    # this is already sitting in the baseline row, just wasn't exposed here.
    zscores = current_zscores(baseline)
    triggered_z = [z for z in zscores.values() if z >= settings.anomaly_zscore_threshold]

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
