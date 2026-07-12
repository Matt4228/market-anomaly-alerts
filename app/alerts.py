import json
import logging
from datetime import datetime, timedelta, timezone

import httpx
from fastapi import WebSocket

from app.config import settings

logger = logging.getLogger(__name__)


class AlertManager:
    """Per-ticker cooldown + fan-out to websocket clients and Slack.

    Cooldown exists so a ticker that stays anomalous for many consecutive
    polls doesn't produce an alert every cycle — one alert, then silence
    until the cooldown window passes or the anomaly clears.
    """

    def __init__(self):
        self._last_alert_at: dict[str, datetime] = {}
        self._connections: set[WebSocket] = set()

    def should_alert(self, ticker: str, cooldown_minutes: int) -> bool:
        last = self._last_alert_at.get(ticker)
        if last is None:
            return True
        return datetime.now(timezone.utc) - last >= timedelta(minutes=cooldown_minutes)

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
        # Slack is a best-effort side channel: the DB row and WebSocket
        # broadcast have already succeeded by the time this runs, so a
        # Slack outage or misconfigured webhook should never take down
        # the alert pipeline — log it and move on instead of raising.
        payload = {
            "blocks": [
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f":rotating_light: {text}"},
                }
            ]
        }
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.post(settings.slack_webhook_url, json=payload)
                response.raise_for_status()
        except Exception:
            logger.exception("failed to post Slack notification")


alert_manager = AlertManager()
