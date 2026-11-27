from __future__ import annotations

from typing import TYPE_CHECKING, Any

from stabilize.models.stage.planned_digests import digest

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

DISPLACED_REQUEST = "_displaced_request"


def displaced_record(
    context: Mapping[str, Any], written: Mapping[str, Any], request_keys: Iterable[str]
) -> dict[str, Any] | None:
    known = context.get(DISPLACED_REQUEST)
    record: dict[str, Any] = dict(known) if isinstance(known, dict) else {}
    changed = False
    for key in request_keys:
        if key not in written:
            continue
        wrote = digest(written[key])
        entry = record.get(key)
        if isinstance(entry, dict):
            if entry.get("wrote") != wrote:
                record[key] = {**entry, "wrote": wrote}
                changed = True
            continue
        if key in context and digest(context[key]) == wrote:
            continue
        record[key] = {"wrote": wrote, "was": context[key]} if key in context else {"wrote": wrote}
        changed = True
    return record if changed else None


def restore_displaced(context: dict[str, Any]) -> None:
    record = context.pop(DISPLACED_REQUEST, None)
    if not isinstance(record, dict):
        return
    for key, entry in record.items():
        if not isinstance(entry, dict) or key not in context or digest(context[key]) != entry.get("wrote"):
            continue
        if "was" in entry:
            context[key] = entry["was"]
        else:
            del context[key]
