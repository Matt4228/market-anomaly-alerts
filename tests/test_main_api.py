from unittest.mock import AsyncMock

from app.config import settings


def test_list_tickers_seeds_and_returns_the_default_tracked_set(client):
    response = client.get("/tickers")
    assert response.status_code == 200
    body = response.json()
    assert body["tickers"] == sorted(settings.tickers)
    assert body["poll_interval_minutes"] == settings.poll_interval_minutes


def test_add_ticker_without_a_debug_token_is_forbidden(client):
    response = client.post("/tickers/NFLX")
    assert response.status_code == 403


def test_add_ticker_with_the_wrong_debug_token_is_forbidden(client, monkeypatch):
    monkeypatch.setattr(settings, "debug_token", "correct-token")
    response = client.post("/tickers/NFLX", headers={"X-Debug-Token": "wrong-token"})
    assert response.status_code == 403


def test_add_ticker_with_a_valid_token_persists_the_new_ticker(client, monkeypatch):
    monkeypatch.setattr(settings, "debug_token", "correct-token")
    monkeypatch.setattr("app.main.fetch_latest_price", AsyncMock(return_value={"price": 100.0}))
    headers = {"X-Debug-Token": "correct-token"}

    client.get("/tickers")  # triggers seeding of the 5 default tickers
    assert client.delete("/tickers/SPY", headers=headers).status_code == 200  # free a slot under MAX_TICKERS

    response = client.post("/tickers/NFLX", headers=headers)
    assert response.status_code == 200
    assert "NFLX" in response.json()["tickers"]


def test_remove_ticker_below_the_minimum_is_rejected(client, monkeypatch):
    monkeypatch.setattr(settings, "debug_token", "correct-token")
    headers = {"X-Debug-Token": "correct-token"}
    client.get("/tickers")  # triggers seeding of the 5 default tickers

    for ticker in ["MSFT", "GOOGL", "TSLA", "SPY"]:
        assert client.delete(f"/tickers/{ticker}", headers=headers).status_code == 200

    response = client.delete("/tickers/AAPL", headers=headers)  # last remaining ticker
    assert response.status_code == 400


def test_list_alerts_is_empty_before_any_alert_has_fired(client):
    response = client.get("/alerts")
    assert response.status_code == 200
    assert response.json() == []


def test_get_alert_404s_for_an_unknown_id(client):
    response = client.get("/alerts/999")
    assert response.status_code == 404


def test_debug_test_alert_fails_closed_when_no_token_is_configured(client, monkeypatch):
    monkeypatch.setattr(settings, "debug_token", None)
    response = client.post("/debug/test-alert/AAPL")
    assert response.status_code == 403
