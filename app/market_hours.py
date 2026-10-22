"""US equity market-hours check, used to skip poll cycles while the
market is closed.

Prices don't move outside regular hours, so polling then only produces
noise: on a holiday the price sits still with zero volume, which is
exactly what the stale/halted detector looks for. Skipping those cycles
also keeps the free-tier Postgres idle overnight and on weekends, so
the monthly compute allowance covers market hours only.

Early-close days (e.g. the day after Thanksgiving, 1:00 PM close) aren't
modeled - the extra ~3 hours of polling a few times a year is harmless.
"""

from datetime import datetime, time
from zoneinfo import ZoneInfo

import holidays

MARKET_TZ = ZoneInfo("America/New_York")
MARKET_OPEN = time(9, 30)
MARKET_CLOSE = time(16, 0)

# Lazily expands to whatever year is looked up, so this never goes stale.
_NYSE_HOLIDAYS = holidays.financial_holidays("NYSE")


def is_market_open(now: datetime | None = None) -> bool:
    """Whether NYSE regular trading hours are in session.

    Parameters
    ----------
    now : datetime, optional
        Timezone-aware moment to check, by default the current time.
        Converted to New York time, so callers can pass any timezone
        (including UTC) and daylight saving is handled for them.

    Returns
    -------
    bool
        True from 9:30 AM up to (not including) 4:00 PM New York time,
        Monday through Friday, excluding NYSE holidays.
    """
    local = (now or datetime.now(MARKET_TZ)).astimezone(MARKET_TZ)
    if local.weekday() >= 5 or local.date() in _NYSE_HOLIDAYS:
        return False
    return MARKET_OPEN <= local.time() < MARKET_CLOSE
