"""How a process with a worker id names its PostgreSQL sessions, and how those names are read back."""

from __future__ import annotations

import re
import uuid

WORKER_ID_RE = re.compile(r"^[A-Za-z0-9.-]{1,24}$")
INCARNATION = uuid.uuid4().hex[:12]


def validate_worker_id(worker_id: object) -> None:
    if worker_id is None:
        return
    if not isinstance(worker_id, str) or not WORKER_ID_RE.match(worker_id):
        raise ValueError(f"worker_id {worker_id!r} is not allowed: 1 to 24 characters from A-Z a-z 0-9 . -")


def worker_application_name(worker_id: str) -> str:
    """The application_name of every pooled session of this process: release, worker id and incarnation."""
    from stabilize import __version__

    return f"stabilize/{__version__} w={worker_id} i={INCARNATION}"[:63]


def parse_worker_tag(application_name: str | None) -> tuple[str, str] | None:
    """The (worker id, incarnation) an engine session's application_name carries, or None."""
    if not application_name or not application_name.startswith("stabilize/"):
        return None
    worker: str | None = None
    incarnation: str | None = None
    for token in application_name.split(" "):
        if token.startswith("w="):
            worker = token[2:]
        elif token.startswith("i="):
            incarnation = token[2:]
    if not worker or not incarnation:
        return None
    return worker, incarnation
