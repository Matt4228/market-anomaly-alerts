import json

import httpx
import pytest

from app import ingestion, pulse, scheduler, ticker_info

URL = "https://pulse.test"
KEY = "test-ingest-key"


@pytest.fixture
def pulse_requests(monkeypatch):
    """Points the reporter at a fake Pulse and returns the requests it receives."""
    received: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        received.append(request)
        return httpx.Response(202, json={"stored": 1})

    monkeypatch.setattr(pulse.settings, "pulse_url", URL)
    monkeypatch.setattr(pulse.settings, "pulse_key", KEY)
    monkeypatch.setattr(pulse, "_client", lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    pulse.source_counts.clear()
    yield received
    pulse.source_counts.clear()


def body(request: httpx.Request) -> dict:
    return json.loads(request.content)


@pytest.fixture
def market(monkeypatch):
    """A poll cycle with the database and price pipeline stubbed out.

    Returns a dict the test fills in: which tickers fail to fetch.
    """
    state = {"failing": set(), "tickers": ["AAPL", "MSFT", "SPY"]}

    class FakeSession:
        def close(self):
            pass

    async def fake_fetch(ticker):
        if ticker in state["failing"]:
            raise RuntimeError("Results not found.")
        return {"price": 100.0, "volume": 1.0, "source": "yfinance"}

    async def noop(*args, **kwargs):
        return None

    monkeypatch.setattr(scheduler, "is_market_open", lambda: True)
    monkeypatch.setattr(scheduler, "SessionLocal", FakeSession)
    monkeypatch.setattr(scheduler, "get_tracked_tickers", lambda db: state["tickers"])
    monkeypatch.setattr(scheduler, "fetch_latest_price", fake_fetch)
    monkeypatch.setattr(scheduler, "process_price", noop)
    monkeypatch.setattr(scheduler, "check_reconciliation", noop)
    return state


async def test_heartbeat_is_fail_when_every_ticker_failed(pulse_requests, market):
    market["failing"] = {"AAPL", "MSFT", "SPY"}
    await scheduler.poll_cycle()

    heartbeat = pulse_requests[0]
    assert heartbeat.url == f"{URL}/ingest/heartbeat"
    assert heartbeat.headers["authorization"] == f"Bearer {KEY}"
    assert body(heartbeat) == {"job": "maa-poll", "status": "fail", "detail": {"tickers": 3, "ok": 0, "failed": 3}}


async def test_heartbeat_is_ok_when_one_ticker_succeeded(pulse_requests, market):
    market["failing"] = {"AAPL", "MSFT"}
    await scheduler.poll_cycle()
    expected = {"job": "maa-poll", "status": "ok", "detail": {"tickers": 3, "ok": 1, "failed": 2}}
    assert body(pulse_requests[0]) == expected


async def test_no_tickers_tracked_is_ok_not_fail(pulse_requests, market):
    market["tickers"] = []
    await scheduler.poll_cycle()
    assert body(pulse_requests[0])["status"] == "ok"


async def test_no_heartbeat_is_sent_while_the_market_is_closed(pulse_requests, market, monkeypatch):
    monkeypatch.setattr(scheduler, "is_market_open", lambda: False)
    await scheduler.poll_cycle()
    assert pulse_requests == []


async def test_nothing_is_sent_when_pulse_url_is_unset(pulse_requests, market, monkeypatch):
    monkeypatch.setattr(pulse.settings, "pulse_url", None)
    pulse.record_source("quote")
    await scheduler.poll_cycle()
    assert pulse_requests == []


async def test_a_pulse_timeout_does_not_raise_out_of_poll_cycle(market, monkeypatch):
    def handler(request):
        raise httpx.ConnectTimeout("timed out", request=request)

    monkeypatch.setattr(pulse.settings, "pulse_url", URL)
    monkeypatch.setattr(pulse.settings, "pulse_key", KEY)
    monkeypatch.setattr(pulse, "_client", lambda: httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    pulse.record_source("quote")
    await scheduler.poll_cycle()  # must not raise


async def test_a_pulse_error_status_does_not_raise(market, monkeypatch, caplog):
    monkeypatch.setattr(pulse.settings, "pulse_url", URL)
    monkeypatch.setattr(pulse.settings, "pulse_key", "wrong")
    monkeypatch.setattr(
        pulse, "_client", lambda: httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(401)))
    )
    await scheduler.poll_cycle()
    assert "401" in caplog.text  # a wrong key shows up in the Render logs


async def test_source_counts_are_sent_as_metrics_and_then_cleared(pulse_requests, market):
    for name in ["chart_fallback", "chart_fallback", "reconciliation"]:
        pulse.record_source(name)
    await scheduler.poll_cycle()

    metrics = [r for r in pulse_requests if r.url.path == "/ingest/metric"]
    assert len(metrics) == 1
    assert sorted(body(metrics[0])["metrics"], key=lambda m: m["name"]) == [
        {"name": "source.chart_fallback", "value": 2},
        {"name": "source.reconciliation", "value": 1},
    ]
    assert not pulse.source_counts


async def test_no_metric_request_when_nothing_was_fetched(pulse_requests, market):
    market["tickers"] = []
    await scheduler.poll_cycle()
    assert [r.url.path for r in pulse_requests] == ["/ingest/heartbeat"]


def test_chart_fallback_is_counted_as_chart_fallback(pulse_requests, monkeypatch):
    def quote_fails(ticker):
        raise RuntimeError("Results not found.")

    monkeypatch.setattr(ingestion, "_fetch_quote_blocking", quote_fails)
    monkeypatch.setattr(ingestion, "_fetch_chart_price_blocking", lambda ticker: {"price": 1.0})
    ingestion._fetch_price_blocking("AAPL")
    assert pulse.source_counts == {"chart_fallback": 1}


def test_a_working_quote_is_counted_as_quote(pulse_requests, monkeypatch):
    monkeypatch.setattr(ingestion, "_fetch_quote_blocking", lambda ticker: {"price": 1.0})
    ingestion._fetch_price_blocking("AAPL")
    assert pulse.source_counts == {"quote": 1}


async def test_reconciliation_is_counted_only_on_success(pulse_requests, monkeypatch):
    async def no_wait():
        return None

    monkeypatch.setattr(ingestion.limiter, "acquire", no_wait)
    monkeypatch.setattr(ingestion, "_fetch_reconciliation_price_blocking", lambda ticker: 101.0)
    await ingestion.fetch_reconciliation_price("AAPL")

    def fails(ticker):
        raise RuntimeError("boom")

    monkeypatch.setattr(ingestion, "_fetch_reconciliation_price_blocking", fails)
    await ingestion.fetch_reconciliation_price("AAPL")
    assert pulse.source_counts == {"reconciliation": 1}


async def test_history_and_fundamentals_lookups_are_counted(pulse_requests, monkeypatch):
    async def no_wait():
        return None

    monkeypatch.setattr(ticker_info.limiter, "acquire", no_wait)
    monkeypatch.setattr(ticker_info, "_fetch_history_blocking", lambda ticker, range_key: [])
    monkeypatch.setattr(ticker_info, "_fetch_fundamentals_blocking", lambda ticker: {})
    await ticker_info.fetch_history("AAPL", "1M")
    await ticker_info.fetch_fundamentals("AAPL")
    assert pulse.source_counts == {"history": 1, "fundamentals": 1}
