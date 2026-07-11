import json
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import WebSocket

from app.config import settings


class AlertManager:
    """Per-ticker cooldown + fan-out to websocket clients and Slack.

    Cooldown exists so a ticker that stays anomalous for many consecutive
    polls doesn't produce an alert every cycle — one alert, then silence
    until the cooldown window passes or the anomaly clears.
    """

    def __init__(self, cooldown_minutes: int):
        self.cooldown = timedelta(minutes=cooldown_minutes)
        self._last_alert_at: dict[str, datetime] = {}
        self._connections: set[WebSocket] = set()

    def should_alert(self, ticker: str) -> bool:
        last = self._last_alert_at.get(ticker)
        if last is None:
            return True
        return datetime.now(timezone.utc) - last >= self.cooldown

    def record_alert(self, ticker: str) -> None:
        self._last_alert_at[ticker] = datetime.now(timezone.utc)

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        self._connections.add(ws)

    def disconnect(self, ws: WebSocket) -> None:
        self._connections.discard(ws)

    async def broadcast(self, payload: dict) -> None:
        message = json.dumps(payload, default=str)
        dead = []
        for ws in self._connections:
            try:
                await ws.send_text(message)
            except Exception:  # noqa: BLE001 - drop connections that error on send
                dead.append(ws)
        for ws in dead:
            self._connections.discard(ws)

    async def notify_slack(self, text: str) -> None:
        if not settings.slack_webhook_url:
            return
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.post(settings.slack_webhook_url, json={"text": text})


alert_manager = AlertManager(cooldown_minutes=settings.alert_cooldown_minutes)
