from datetime import datetime, timezone

import pytest

from app import scheduler
from app.market_hours import MARKET_TZ, is_market_open


def ny(*args):
    """A New York-local aware datetime, e.g. ny(2026, 10, 21, 12, 0)."""
    return datetime(*args, tzinfo=MARKET_TZ)


@pytest.mark.parametrize(
    "moment, expected",
    [
        (ny(2026, 10, 21, 12, 0), True),  # Wednesday midday
        (ny(2026, 10, 21, 9, 29), False),  # one minute before the open
        (ny(2026, 10, 21, 9, 30), True),  # the open itself counts
        (ny(2026, 10, 21, 15, 59), True),  # last minute of the session
        (ny(2026, 10, 21, 16, 0), False),  # the close does not
        (ny(2026, 10, 24, 12, 0), False),  # Saturday
        (ny(2026, 10, 25, 12, 0), False),  # Sunday
        (ny(2026, 11, 26, 12, 0), False),  # Thanksgiving
        (ny(2026, 4, 3, 12, 0), False),  # Good Friday: NYSE closes, federal holiday calendars don't
    ],
)
def test_is_market_open(moment, expected):
    assert is_market_open(moment) is expected


def test_utc_input_respects_daylight_saving():
    # The 9:30 AM open is 13:30 UTC in summer (EDT) but 14:30 UTC in winter (EST).
    assert is_market_open(datetime(2026, 7, 15, 13, 30, tzinfo=timezone.utc))
    assert not is_market_open(datetime(2026, 12, 15, 13, 30, tzinfo=timezone.utc))
    assert is_market_open(datetime(2026, 12, 15, 14, 30, tzinfo=timezone.utc))


async def test_poll_cycle_skips_when_market_closed(monkeypatch):
    monkeypatch.setattr(scheduler.settings, "market_hours_only", True)
    monkeypatch.setattr(scheduler, "is_market_open", lambda: False)

    def fail_if_called(*args, **kwargs):
        raise AssertionError("poll_cycle touched the database while the market was closed")

    monkeypatch.setattr(scheduler, "SessionLocal", fail_if_called)
    await scheduler.poll_cycle()


async def test_poll_cycle_runs_when_market_hours_only_disabled(monkeypatch):
    monkeypatch.setattr(scheduler.settings, "market_hours_only", False)
    monkeypatch.setattr(scheduler, "is_market_open", lambda: False)

    class Reached(Exception):
        pass

    def reached(*args, **kwargs):
        raise Reached

    monkeypatch.setattr(scheduler, "SessionLocal", reached)
    with pytest.raises(Reached):
        await scheduler.poll_cycle()
