# Market Anomaly Alerts

A Python backend that polls market data (via [OpenBB](https://openbb.co/)), detects
statistically anomalous price/volume moves against a per-ticker rolling baseline, and
pushes alerts out over WebSocket and Slack. Built as a practice project standing in for
Bloomberg-style market data API experience, with a deliberate focus on the mechanics
that come up around any rate-limited external API: throttling, backoff, caching,
scalability, and cost tradeoffs.

## Design decisions

- **Polling, not push.** Free OpenBB providers don't offer webhooks, and different data
  types warrant different intervals (equities every few minutes during market hours;
  macro data far less often) — tiering poll frequency by data type avoids wasting calls
  on data that hasn't changed.
- **Throttling via a shared token bucket** (`app/rate_limiter.py`), sized below the
  provider's documented cap rather than at it, so a burst of tickers in one poll cycle
  can't trip a 429. Backoff on failure is exponential with jitter — without jitter,
  every queued ticker would retry in lockstep and re-trigger the same limit.
- **Short-TTL caching** (`app/cache.py`), on the order of seconds. Anomaly detection
  needs fresh data, so this isn't "cache aggressively" — it just collapses duplicate
  fetches for the same ticker within one cycle.
- **Incremental baseline, not full-history rescans.** `TickerBaseline` maintains a
  running mean/stddev per ticker via Welford's algorithm, so each anomaly check is O(1)
  against stored aggregates instead of scanning `price_history`.
- **Debounced alerts.** A per-ticker cooldown (default 30 min) means a ticker that stays
  anomalous for many consecutive polls fires one alert, not one per cycle.
- **In-process WebSocket broadcast for the MVP.** `AlertManager` holds connections in a
  set on a single process. That's a known limit: scaling to multiple instances would
  mean moving broadcast to Redis pub/sub (or similar) so alerts fan out across
  processes instead of only to clients connected to whichever instance polled the hit.
  Naming this limitation is deliberate — it's the honest answer to "how would this
  scale."
- **Rule-based detection (z-score), not ML.** Keeps the MVP explainable. Swapping in a
  model later is a natural extension, not a redesign.

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
```

Create a local Postgres database matching `DATABASE_URL` in `.env` (or point it at a
free Supabase/Neon instance). Tables are created automatically on startup.

## Run

```bash
uvicorn app.main:app --reload
```

- `GET /tickers` — tracked tickers and poll interval
- `GET /alerts` — recent alerts
- `GET /tickers/{ticker}/baseline` — current rolling mean/stddev for a ticker
- `WS /ws/alerts` — live alert stream

## Notes

- Default tracked tickers and thresholds live in `.env` — see `.env.example`.
- OpenBB's Python interface has shifted across versions; if `app/ingestion.py`'s call
  into `obb.equity.price.historical` doesn't match the installed version, check
  `obb.coverage` / the installed package's docs.
- Slack alerts are optional — leave `SLACK_WEBHOOK_URL` blank to skip them.

## Possible next steps

- Batch multi-symbol requests where the provider supports it, and move polling from a
  single loop to a queue of per-ticker jobs pulled by multiple workers — the path to
  scaling from a handful of tickers to hundreds.
- Move alert broadcast to Redis pub/sub so it works across multiple app instances.
- Pipe daily aggregates into Snowflake as a reporting layer.
- Deploy to Azure App Service (free tier) for a live demo.
