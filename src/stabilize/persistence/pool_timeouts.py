"""The session timeouts the engine asks PostgreSQL for on its pooled connections, and a check that they took."""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from stabilize.persistence.pool_options import PoolOptions

logger = logging.getLogger(__name__)

TIMEOUT_SETTINGS = ("lock_timeout", "statement_timeout", "idle_in_transaction_session_timeout")
EXPECTED_ATTRIBUTE = "stabilize_expected_timeouts"


def validate_timeout(name: str, value: Any) -> None:
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not value > 0:
        raise ValueError(f"{name} must be a positive number of seconds or None, not {value!r}")


def configured_timeouts(options: PoolOptions) -> dict[str, int]:
    """Each timeout the options set, in milliseconds."""
    wanted: dict[str, int] = {}
    for name in TIMEOUT_SETTINGS:
        seconds = getattr(options, name)
        if seconds is not None:
            wanted[name] = round(float(seconds) * 1000)
    return wanted


def _sets(options: str | None, name: str) -> bool:
    if not options:
        return False
    return re.search(rf"(?:^|\s)(?:-c\s*|--){re.escape(name)}\s*=", options) is not None


def dsn_options(connection_string: str) -> str | None:
    from psycopg.conninfo import conninfo_to_dict

    try:
        found = conninfo_to_dict(connection_string).get("options")
    except Exception:
        return None
    return str(found) if found else None


def merged_options(options: PoolOptions, connect_kwargs: dict[str, Any], connection_string: str) -> str | None:
    """The libpq ``options`` to connect with: the caller's own, then each timeout the caller did not set.

    Returns None when there is nothing to add, so the caller's kwargs and DSN are used untouched.
    """
    own = connect_kwargs.get("options") if "options" in connect_kwargs else dsn_options(connection_string)
    added = [f"-c {name}={ms}" for name, ms in configured_timeouts(options).items() if not _sets(own, name)]
    if not added:
        return None
    return " ".join(([str(own)] if own else []) + added)


def expected_timeouts(options: PoolOptions, effective_options: str | None) -> dict[str, int]:
    """The timeouts the engine itself asked for, which a connection is then checked against."""
    return {
        name: ms
        for name, ms in configured_timeouts(options).items()
        if effective_options is not None and f"-c {name}={ms}" in effective_options
    }


_UNIT_MS = {"us": 0.001, "ms": 1.0, "s": 1000.0, "min": 60_000.0, "h": 3_600_000.0, "d": 86_400_000.0}
_SETTING_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([a-z]*)\s*$")


def setting_ms(text: object) -> int | None:
    """A PostgreSQL time setting as SHOW prints it ('30s', '1min', '0'), in milliseconds; None when unreadable."""
    match = _SETTING_RE.match(str(text)) if text is not None else None
    if match is None:
        return None
    factor = _UNIT_MS.get(match.group(2) or "ms")
    return None if factor is None else round(float(match.group(1)) * factor)


def report_unheld(conn: Any, pool: Any) -> None:
    """Log, once per pool, each timeout the engine asked for that *conn* does not hold."""
    expected = getattr(pool, EXPECTED_ATTRIBUTE, None)
    if not expected:
        return
    setattr(pool, EXPECTED_ATTRIBUTE, {})
    with conn.cursor() as cur:
        cur.execute("SELECT " + ", ".join(f"current_setting('{name}') AS {name}" for name in expected))
        row = cur.fetchone()
    for index, (name, wanted) in enumerate(expected.items()):
        text = row[name] if isinstance(row, dict) else row[index]
        held = setting_ms(text)
        if held is not None and held != wanted:
            logger.warning(
                "%s is %s on an engine connection, not the configured %gs; a configure callback, ALTER ROLE or a "
                "pooler that drops startup options is overriding it",
                name,
                text,
                wanted / 1000,
            )
