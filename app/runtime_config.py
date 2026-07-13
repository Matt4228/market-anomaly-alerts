"""Runtime-adjustable alert thresholds, backed by a singleton `RuntimeConfig`
row rather than the static env-var `Settings`. Once seeded (in `main.py`'s
lifespan), the DB row is the permanent source of truth — a later env var
change has no further effect. See `app/config.py`'s module docstring.
"""

from dataclasses import dataclass

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.config import settings
from app.models import RuntimeConfig

SINGLETON_ID = 1


@dataclass
class RuntimeConfigValues:
    """A detached snapshot, not the ORM row — SessionLocal sessions here are
    short-lived and closed in `finally` blocks, so returning the live ORM
    object would risk an expire_on_commit refresh against an already-closed
    session wherever the value gets used after the fetching session ends.

    Attributes
    ----------
    anomaly_zscore_threshold : float
    stale_threshold : int
    alert_cooldown_minutes : int
    reconciliation_tolerance : float
    """

    anomaly_zscore_threshold: float
    stale_threshold: int
    alert_cooldown_minutes: int
    reconciliation_tolerance: float


def _to_values(row: RuntimeConfig) -> RuntimeConfigValues:
    """Detach a `RuntimeConfig` ORM row into a plain dataclass.

    Parameters
    ----------
    row : RuntimeConfig

    Returns
    -------
    RuntimeConfigValues
    """
    return RuntimeConfigValues(
        anomaly_zscore_threshold=row.anomaly_zscore_threshold,
        stale_threshold=row.stale_threshold,
        alert_cooldown_minutes=row.alert_cooldown_minutes,
        reconciliation_tolerance=row.reconciliation_tolerance,
    )


def get_runtime_config(db: Session) -> RuntimeConfigValues:
    """Read the singleton runtime config row, seeding it from `settings`
    if it doesn't exist yet.

    Parameters
    ----------
    db : sqlalchemy.orm.Session

    Returns
    -------
    RuntimeConfigValues
    """
    row = db.get(RuntimeConfig, SINGLETON_ID)
    if row is not None:
        return _to_values(row)

    # Normally seeded once in main.py's lifespan before any request is
    # accepted, so this "insert if missing" branch is a defensive fallback
    # (e.g. someone manually deleted the row), not the primary mechanism —
    # the IntegrityError catch guards the narrow remaining race where two
    # requests both hit this branch concurrently.
    row = RuntimeConfig(
        id=SINGLETON_ID,
        anomaly_zscore_threshold=settings.anomaly_zscore_threshold,
        stale_threshold=settings.stale_threshold,
        alert_cooldown_minutes=settings.alert_cooldown_minutes,
        reconciliation_tolerance=settings.reconciliation_tolerance,
    )
    db.add(row)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        row = db.get(RuntimeConfig, SINGLETON_ID)
    return _to_values(row)


def update_runtime_config(db: Session, **kwargs: float | int | None) -> RuntimeConfigValues:
    """Validate and apply one or more threshold updates to the singleton
    row. Keys not present in `kwargs`, or passed as `None`, are left
    unchanged.

    Parameters
    ----------
    db : sqlalchemy.orm.Session
    **kwargs : float or int or None
        Any of `anomaly_zscore_threshold`, `stale_threshold`,
        `alert_cooldown_minutes`, `reconciliation_tolerance`.

    Returns
    -------
    RuntimeConfigValues

    Raises
    ------
    ValueError
        If any provided value is out of its valid range.
    """
    if kwargs.get("anomaly_zscore_threshold") is not None and kwargs["anomaly_zscore_threshold"] <= 0:
        raise ValueError("anomaly_zscore_threshold must be > 0")
    if kwargs.get("stale_threshold") is not None and kwargs["stale_threshold"] <= 0:
        raise ValueError("stale_threshold must be a positive integer")
    if kwargs.get("alert_cooldown_minutes") is not None and kwargs["alert_cooldown_minutes"] < 0:
        raise ValueError("alert_cooldown_minutes must be >= 0")
    if kwargs.get("reconciliation_tolerance") is not None and not (0 < kwargs["reconciliation_tolerance"] < 1):
        raise ValueError("reconciliation_tolerance must be between 0 and 1")

    get_runtime_config(db)  # ensures the singleton row exists before we update it
    row = db.get(RuntimeConfig, SINGLETON_ID)
    for key, value in kwargs.items():
        if value is not None:
            setattr(row, key, value)
    db.commit()
    return _to_values(row)
