"""At start, a processor with a worker id releases the leases its predecessor held, when it is safe to."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from stabilize.persistence.tenancy import EngineSchemaError
from stabilize.persistence.worker_tag import INCARNATION, parse_worker_tag

logger = logging.getLogger(__name__)

SESSIONS_SQL = (
    "SELECT application_name FROM pg_stat_activity "
    "WHERE datname = current_database() AND application_name LIKE 'stabilize/%%'"
)


@dataclass(frozen=True)
class ReclaimResult:
    """What a processor's start did about leases held under its worker id."""

    released: int = 0
    reason: str = "no worker id"


def engine_sessions(conn: Any) -> list[str]:
    """The application_name of every engine session on this database that the connection can see."""
    with conn.cursor() as cur:
        cur.execute(SESSIONS_SQL)
        rows = cur.fetchall()
    return [str(row["application_name"] if isinstance(row, dict) else row[0]) for row in rows]


def reclaim(queue: Any) -> ReclaimResult:
    """Release the leases held under the queue's worker id unless another incarnation of it has a session."""
    worker_id = getattr(queue, "worker_id", None)
    if not isinstance(worker_id, str) or not worker_id:
        return ReclaimResult()
    queue.require_locked_by()
    try:
        with queue._borrow() as conn:
            tags = [tag for tag in (parse_worker_tag(name) for name in engine_sessions(conn)) if tag is not None]
            if (worker_id, INCARNATION) not in tags:
                conn.rollback()
                return _withheld(
                    worker_id,
                    "the check cannot see its own session in pg_stat_activity, so it cannot rule out a live holder",
                )
            others = sum(1 for held_by, incarnation in tags if held_by == worker_id and incarnation != INCARNATION)
            if others:
                conn.rollback()
                return _withheld(worker_id, f"another incarnation of this worker id has {others} session(s)")
            released = int(queue.release_held_by_worker(conn))
            conn.commit()
    except EngineSchemaError:
        raise
    except Exception as error:
        return _withheld(worker_id, f"the check failed: {error}")
    if released:
        queue.announce_ready()
        logger.info("worker id %s: released %d lease(s) its predecessor held", worker_id, released)
    return ReclaimResult(released, "released the leases its predecessor held")


def _withheld(worker_id: str, reason: str) -> ReclaimResult:
    logger.warning(
        "worker id %s: no lease was released at start, because %s. Messages its predecessor held wait for "
        "their lock to expire.",
        worker_id,
        reason,
    )
    return ReclaimResult(0, reason)
