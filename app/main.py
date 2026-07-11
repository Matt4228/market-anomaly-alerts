from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import Depends, FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from sqlalchemy import desc
from sqlalchemy.orm import Session

from app.alerts import alert_manager
from app.config import settings
from app.db import Base, engine, get_session
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
    return FileResponse(STATIC_DIR / "index.html")


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
    return {
        "ticker": baseline.ticker,
        "mean": baseline.mean,
        "stddev": baseline.stddev,
        "sample_count": baseline.sample_count,
        "updated_at": baseline.updated_at,
    }


@app.post("/debug/test-alert/{ticker}")
async def test_alert(ticker: str):
    """Demo/testing only — fires a real alert through the same storage,
    broadcast, and Slack code path as a genuine detection, using a
    synthetic price instead of waiting on real market volatility."""
    try:
        return await trigger_test_alert(ticker.upper())
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
