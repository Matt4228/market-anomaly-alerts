from types import SimpleNamespace

import pytest

from app import ingestion

QUOTE = {"ticker": "AAPL", "price": 100.0, "bid": 99.9, "ask": 100.1, "volume": 5.0, "source": "yfinance"}


def fake_yf(last_price, last_volume=0):
    """Stands in for the yfinance module: Ticker(...).fast_info has these values."""
    info = SimpleNamespace(last_price=last_price, last_volume=last_volume)
    return SimpleNamespace(Ticker=lambda ticker: SimpleNamespace(fast_info=info))


def quote_fails(ticker):
    # What Yahoo's quote endpoint does from Render: OpenBB raises on the
    # empty result instead of returning an empty frame.
    raise RuntimeError("Results not found.")


def test_uses_the_quote_when_it_works(monkeypatch):
    monkeypatch.setattr(ingestion, "_fetch_quote_blocking", lambda ticker: QUOTE)
    monkeypatch.setattr(ingestion, "yf", None)  # would crash if the fallback ran
    assert ingestion._fetch_price_blocking("AAPL") == QUOTE


def test_falls_back_to_chart_data_when_the_quote_fails(monkeypatch):
    monkeypatch.setattr(ingestion, "_fetch_quote_blocking", quote_fails)
    monkeypatch.setattr(ingestion, "yf", fake_yf(102.5, 23_000_000))

    result = ingestion._fetch_price_blocking("AAPL")

    assert result["price"] == 102.5
    assert result["volume"] == 23_000_000.0  # today's total, same meaning as the quote's
    assert result["bid"] == result["ask"] == 102.5  # no bid/ask in chart data, so spread is 0
    assert result["source"].endswith("-chart")


def test_chart_fallback_with_no_price_raises(monkeypatch):
    monkeypatch.setattr(ingestion, "_fetch_quote_blocking", quote_fails)
    monkeypatch.setattr(ingestion, "yf", fake_yf(None))
    with pytest.raises(ValueError, match="no chart data"):
        ingestion._fetch_price_blocking("AAPL")
