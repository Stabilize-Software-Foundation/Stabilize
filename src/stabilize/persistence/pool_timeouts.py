"""The session timeouts the engine asks PostgreSQL for on its pooled connections, and a check that they took."""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from stabilize.persistence.pool_options import PoolOptions

logger = logging.getLogger(__name__)

TIMEOUT_SETTINGS = ("lock_timeout", "statement_timeout", "idle_in_transaction_session_timeout")
FINDINGS_ATTRIBUTE = "_stabilize_timeout_findings"


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


def checking_configure(
    expected: dict[str, int],
    configure: Callable[[Any], None] | None,
    findings: list[str],
) -> Callable[[Any], None] | None:
    """A configure callback that runs the caller's own, then records each timeout the connection does not hold."""
    if not expected:
        return configure

    def run(conn: Any) -> None:
        if configure is not None:
            configure(conn)
        try:
            row = conn.execute(
                "SELECT "
                + ", ".join(
                    f"(EXTRACT(EPOCH FROM current_setting('{name}')::interval) * 1000)::bigint AS {name}, "
                    f"current_setting('{name}') AS {name}_text"
                    for name in expected
                )
            ).fetchone()
            conn.rollback()
        except Exception as error:
            logger.debug("The session timeouts of a new connection could not be read: %s", error)
            try:
                conn.rollback()
            except Exception as inner:
                logger.debug("Rollback after a failed timeout read failed: %s", inner)
            return
        for name, wanted in expected.items():
            held = row[name] if isinstance(row, dict) else None
            if held is not None and int(held) != wanted and len(findings) < 8:
                findings.append(
                    f"{name} is {row[name + '_text']} on an engine connection, not the configured "
                    f"{wanted / 1000:g}s; a configure callback, ALTER ROLE or a pooler that drops startup "
                    "options is overriding it"
                )

    return run


def report_findings(pool: Any) -> None:
    """Log, once, each timeout a connection of *pool* was found not to hold."""
    findings = getattr(pool, FINDINGS_ATTRIBUTE, None)
    while findings:
        logger.warning("%s", findings.pop(0))
