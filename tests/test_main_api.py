from unittest.mock import AsyncMock

import pytest

from app.config import settings


def test_health_is_ok_without_touching_the_database(client, monkeypatch):
    from app import main

    def fail_if_called(*args, **kwargs):
        raise AssertionError("/health touched the database")

    monkeypatch.setattr(main, "SessionLocal", fail_if_called)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


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


def test_remove_ticker_without_a_debug_token_is_forbidden(client):
    response = client.delete("/tickers/AAPL")
    assert response.status_code == 403


def test_post_config_without_a_debug_token_is_forbidden(client):
    response = client.post("/config")
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


TOKEN = "correct-token"


def mutating_routes():
    """Every (method, path) the app serves that changes state.

    Read from the app's own route table so a route added later is covered
    by the test below without anyone remembering to list it.
    """
    from app.main import app

    for route in app.routes:
        for method in sorted(getattr(route, "methods", None) or ()):
            if method in {"POST", "PUT", "PATCH", "DELETE"}:
                yield method, route.path.replace("{ticker}", "AAPL")


def test_mutating_routes_were_found():
    # Guards the test below: if discovery ever matched nothing, the
    # parametrized test would run zero cases and still pass.
    assert len(list(mutating_routes())) >= 4


@pytest.mark.parametrize("method, path", list(mutating_routes()))
def test_every_mutating_route_requires_the_admin_token(client, monkeypatch, method, path):
    monkeypatch.setattr(settings, "debug_token", TOKEN)
    assert client.request(method, path).status_code == 403
    assert client.request(method, path, headers={"X-Debug-Token": "wrong"}).status_code == 403


def test_dashboard_never_contains_the_admin_token(client, monkeypatch):
    monkeypatch.setattr(settings, "debug_token", "s3cret-admin-value")
    response = client.get("/")
    assert response.status_code == 200
    assert "Market Anomaly Alerts" in response.text
    assert "s3cret-admin-value" not in response.text


def test_admin_check_accepts_the_right_token(client, monkeypatch):
    monkeypatch.setattr(settings, "debug_token", TOKEN)
    response = client.get("/admin/check", headers={"X-Debug-Token": TOKEN})
    assert response.status_code == 200
    assert response.json() == {"ok": True}


def test_admin_check_rejects_a_wrong_or_missing_token(client, monkeypatch):
    monkeypatch.setattr(settings, "debug_token", TOKEN)
    assert client.get("/admin/check").status_code == 403
    assert client.get("/admin/check", headers={"X-Debug-Token": "wrong"}).status_code == 403


def test_admin_check_fails_closed_when_no_token_is_configured(client, monkeypatch):
    monkeypatch.setattr(settings, "debug_token", None)
    assert client.get("/admin/check", headers={"X-Debug-Token": ""}).status_code == 403


def test_non_ascii_token_is_403_not_a_crash(client, monkeypatch):
    # secrets.compare_digest raises TypeError on non-ASCII str, which would
    # surface as a 500 if the comparison weren't done on encoded bytes.
    # Sent as raw UTF-8 bytes: HTTP clients only accept ASCII header strings.
    monkeypatch.setattr(settings, "debug_token", TOKEN)
    response = client.get("/admin/check", headers={"X-Debug-Token": "pässwörd".encode()})
    assert response.status_code == 403
