"""Reports this app's health to Pulse, the monitoring service for these projects.

Two things are sent after each poll cycle:

- A heartbeat for the `maa-poll` job. Its status says whether the cycle
  actually worked (at least one ticker fetched), not just that it ran,
  so a cycle where every fetch fails is reported as `fail`.
- How many times each data source was used since the last report
  (`quote`, `chart_fallback`, `reconciliation`, `history`,
  `fundamentals`), so the dashboard can show how often the chart
  fallback is carrying the app.

Reporting must never hurt the app: every call has a 3 second timeout,
every error is swallowed, and nothing is sent unless both `PULSE_URL`
and `PULSE_KEY` are set (so local dev and tests send nothing).
"""

import logging
import threading
from collections import Counter

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

JOB = "maa-poll"
TIMEOUT_SECONDS = 3.0

# Fetches run in worker threads (asyncio.to_thread), so increments take a lock.
source_counts: Counter[str] = Counter()
_counts_lock = threading.Lock()


def record_source(name: str) -> None:
    """Count one successful fetch from data source `name`."""
    with _counts_lock:
        source_counts[name] += 1


def _client() -> httpx.AsyncClient:
    """The HTTP client for one report. A function so tests can swap the transport."""
    return httpx.AsyncClient(timeout=TIMEOUT_SECONDS)


async def _post(path: str, payload: dict) -> None:
    if not (settings.pulse_url and settings.pulse_key):
        return
    url = settings.pulse_url.rstrip("/") + path
    try:
        async with _client() as client:
            res = await client.post(url, json=payload, headers={"Authorization": f"Bearer {settings.pulse_key}"})
        if res.status_code >= 400:
            # Usually a wrong or missing PULSE_KEY; worth seeing in the logs.
            logger.warning("Pulse rejected %s with %s", path, res.status_code)
    except Exception as exc:  # noqa: BLE001 - monitoring must never break polling
        logger.debug("Pulse report to %s failed: %s", path, exc)


async def send_heartbeat(status: str, detail: dict) -> None:
    """Report one poll cycle. `status` is "ok" or "fail"."""
    await _post("/ingest/heartbeat", {"job": JOB, "status": status, "detail": detail})


async def flush_metrics() -> None:
    """Send the source counts gathered since the last flush, then reset them.

    Counts from a failed send are dropped rather than retried: they are a
    usage signal, and losing one cycle's worth is fine.
    """
    with _counts_lock:
        counts = dict(source_counts)
        source_counts.clear()
    if not counts:
        return
    await _post("/ingest/metric", {"metrics": [{"name": f"source.{n}", "value": v} for n, v in counts.items()]})
