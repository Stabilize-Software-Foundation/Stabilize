"""Digests of the request keys a stage's context held when the stage was planned.

The planner fills a stage's context with its ancestors' outputs. HTTPTask leaves a value out of
its request when only an ancestor supplied it. A task may write the same key after planning, in
the stored context or in a copy it passes on. The digest recorded at planning tells the two
apart from the context alone.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

PLANNED_DIGESTS = "_planned_digests"
DECLARED_VALUES = "_declared_values"
DIGESTED_KEYS = frozenset({"url", "headers", "body", "host", "user"})


def _normalised(value: Any) -> Any:
    if isinstance(value, bool) or not isinstance(value, (int, float, list, dict)):
        return value
    if isinstance(value, list):
        return [_normalised(item) for item in value]
    if isinstance(value, dict):
        return {key: _normalised(item) for key, item in value.items()}
    number = float(value)
    return int(number) if number.is_integer() and abs(number) < 2**53 else number


def digest(value: Any) -> str:
    """SHA-256 of the value as the stores write it and read it back.

    The stores serialise a context with ``json.dumps(..., default=str)``. PostgreSQL returns a
    number in its own notation, so 1.0 and 1 and 1e2 and 100 are digested alike.
    """
    try:
        text = json.dumps(_normalised(json.loads(json.dumps(value, default=str))), sort_keys=True)
    except (TypeError, ValueError, OverflowError, RecursionError):
        text = repr(value)
    return hashlib.sha256(text.encode("utf-8", "backslashreplace")).hexdigest()


def planned_digests(context: Mapping[str, Any], ancestor_keys: Iterable[str]) -> dict[str, str]:
    return {key: digest(context[key]) for key in sorted(DIGESTED_KEYS.intersection(ancestor_keys)) if key in context}


def changed_since_planning(context: Mapping[str, Any], key: str) -> bool:
    digests = context.get(PLANNED_DIGESTS)
    if not isinstance(digests, dict) or key not in digests:
        return False
    return bool(digest(context.get(key)) != digests[key])


def declared_or_current(context: Mapping[str, Any], key: str) -> Any:
    digests = context.get(PLANNED_DIGESTS)
    declared = context.get(DECLARED_VALUES)
    if not isinstance(digests, dict) or key not in digests:
        return context.get(key)
    if not isinstance(declared, dict) or key not in declared:
        return context.get(key)
    if digest(context.get(key)) != digests[key]:
        return context.get(key)
    return declared[key]
