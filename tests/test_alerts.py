from datetime import datetime, timedelta, timezone

import pytest

from app.alerts import AlertManager


def test_should_alert_true_when_ticker_never_alerted():
    manager = AlertManager()
    assert manager.should_alert("AAPL", cooldown_minutes=30) is True


def test_should_alert_false_immediately_after_an_alert_within_cooldown():
    manager = AlertManager()
    manager.record_alert("AAPL")
    assert manager.should_alert("AAPL", cooldown_minutes=30) is False


def test_should_alert_true_once_the_cooldown_window_has_elapsed():
    manager = AlertManager()
    manager.record_alert("AAPL")
    manager._last_alert_at["AAPL"] = datetime.now(timezone.utc) - timedelta(minutes=31)
    assert manager.should_alert("AAPL", cooldown_minutes=30) is True


def test_signal_and_reconciliation_cooldowns_are_tracked_independently():
    manager = AlertManager()
    manager.record_alert("AAPL")
    assert manager.should_alert("AAPL", cooldown_minutes=30) is False
    assert manager.should_alert("AAPL:reconciliation", cooldown_minutes=30) is True


@pytest.mark.asyncio
async def test_broadcast_drops_connections_that_error_on_send_but_keeps_working_ones():
    manager = AlertManager()

    class WorkingSocket:
        def __init__(self):
            self.sent = []

        async def send_text(self, message):
            self.sent.append(message)

    class BrokenSocket:
        async def send_text(self, message):
            raise RuntimeError("connection closed")

    working = WorkingSocket()
    broken = BrokenSocket()
    manager._connections = {working, broken}

    await manager.broadcast({"type": "price_update", "ticker": "AAPL"})

    assert working.sent == ['{"type": "price_update", "ticker": "AAPL"}']
    assert working in manager._connections
    assert broken not in manager._connections
