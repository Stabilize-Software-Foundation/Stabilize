"""What the current delivery committed, per thread: marks, pushes, and the pushes and events outside any transaction."""

from __future__ import annotations

import os
import threading
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

_local = threading.local()

DEFECT_LIMIT = 200


@dataclass
class DeliveryDefect:
    message_type: str
    message_id: str | None
    reasons: tuple[str, ...]


@dataclass
class _Delivery:
    marked: set[str] = field(default_factory=set)
    pushes: list[tuple[str, ...]] = field(default_factory=list)
    bare_pushes: list[str] = field(default_factory=list)
    bare_events: list[str] = field(default_factory=list)
    dropped_events: int = 0
    active: bool = False
    exempt: int = 0
    delivered_id: str | None = None
    delivered_attempts: int | None = None
    acked: set[str] = field(default_factory=set)
    hops: int = 0
    hop_queue: Any = None
    claim_held: bool = False
    claimed: Any = None


_recent: deque[DeliveryDefect] = deque(maxlen=DEFECT_LIMIT)


def _state() -> _Delivery:
    state: _Delivery | None = getattr(_local, "delivery", None)
    if state is None:
        state = _Delivery()
        _local.delivery = state
    return state


def record_committed(message_ids: Iterable[str], pushed: bool = False) -> None:
    """Record a committed transaction: the ids it marked processed, and whether it pushed."""
    state = _state()
    marked = tuple(message_ids)
    state.marked.update(marked)
    if pushed:
        state.pushes.append(marked)


def record_bare_push(message_type: str) -> None:
    """Record a push made on the queue's own connection, outside any store transaction."""
    state = _state()
    if state.exempt == 0:
        state.bare_pushes.append(message_type)


def record_bare_event(event_type: str) -> None:
    """Record an engine event appended while a message is handled, with no store transaction open on the thread."""
    state = _state()
    if state.active and state.exempt == 0 and len(state.bare_events) < DEFECT_LIMIT:
        state.bare_events.append(event_type)


def record_dropped_events(count: int) -> None:
    """Record events that were held for a store transaction the handler never opened."""
    state = _state()
    if state.active:
        state.dropped_events += count


def begin_delivery(message: Any = None) -> None:
    """Forget the previous delivery on this thread and start counting for the next one.

    *message* is the queue delivery being handled; its id and attempt let the transaction that marks
    it processed remove its queue row.
    """
    clear_committed()
    state = _state()
    state.active = True
    attempts = getattr(message, "attempts", None)
    state.delivered_id = getattr(message, "message_id", None)
    state.delivered_attempts = attempts if isinstance(attempts, int) and not isinstance(attempts, bool) else None


def end_delivery() -> None:
    state = _state()
    state.active = False
    state.delivered_id = None
    state.delivered_attempts = None


def delivery_fence(message_id: str, queue_table: str) -> tuple[int, int] | None:
    """The queue row id and attempt of the delivery being handled, when *message_id* names it on *queue_table*."""
    from stabilize.queue.message_ids import own_row_ids

    state = _state()
    if not state.active or state.delivered_id != message_id or state.delivered_attempts is None:
        return None
    owned = own_row_ids(queue_table, [message_id])
    if not owned:
        return None
    return next(iter(owned)), state.delivered_attempts


def allow_continuation(hops: int, queue_key: Any) -> None:
    """Let the next delivery on this thread claim one successor on the queue named by *queue_key*; 0 forbids it."""
    state = _state()
    state.hops = max(0, int(hops))
    state.hop_queue = queue_key if hops > 0 else None
    state.claim_held = False
    state.claimed = None


def may_claim(queue: Any) -> bool:
    """Whether a transaction of the delivery being handled may insert a successor already leased to this worker."""
    state = _state()
    if not state.active or state.exempt or state.hops <= 0 or state.claim_held or state.claimed is not None:
        return False
    if not callable(getattr(type(queue), "wake_key", None)):
        return False
    return bool(queue.wake_key() == state.hop_queue)


def hold_claim() -> None:
    _state().claim_held = True


def drop_claim() -> None:
    _state().claim_held = False


def publish_claim(message: Any) -> None:
    """Record the successor a committed transaction inserted already leased to this worker."""
    state = _state()
    state.claim_held = False
    state.claimed = message


def take_claimed() -> Any:
    """The successor claimed by the delivery that just ended on this thread, once; None when there is none."""
    state = _state()
    claimed, state.claimed = state.claimed, None
    state.claim_held = False
    return claimed


def record_acked(message_id: str) -> None:
    """Record that a committed transaction removed the queue row of *message_id*."""
    _state().acked.add(message_id)


def consume_acked(message_id: str) -> bool:
    """Return True, and forget it, when a committed transaction on this thread removed the row of *message_id*."""
    state = _state()
    if message_id in state.acked:
        state.acked.discard(message_id)
        return True
    return False


def log_defect(defect: DeliveryDefect) -> None:
    _recent.append(defect)


def recent_defects() -> list[DeliveryDefect]:
    """The last defects any processor in this process reported, oldest first."""
    return list(_recent)


def clear_recent_defects() -> None:
    _recent.clear()


@contextmanager
def delivery_exempt() -> Iterator[None]:
    """Pushes in this block belong to no handler: the orchestrator, recovery, signalling, NoOpTransaction."""
    state = _state()
    state.exempt += 1
    try:
        yield
    finally:
        state.exempt -= 1


def consume_committed(message_id: str) -> bool:
    """Return True, and forget it, when *message_id* was marked by a committed transaction on this thread."""
    state = _state()
    if message_id in state.marked:
        state.marked.discard(message_id)
        return True
    return False


def delivery_defects(message_id: str | None) -> list[str]:
    """How this delivery broke the contract; empty when every committed push sat beside its mark."""
    state = _state()
    reasons = [f"{kind} was pushed outside any store transaction" for kind in state.bare_pushes]
    reasons.extend(f"{kind} was recorded outside any store transaction" for kind in state.bare_events)
    if state.dropped_events:
        reasons.append(f"{state.dropped_events} held event(s) never reached a store transaction")
    if message_id is not None:
        for marked in state.pushes:
            if message_id not in marked:
                marks = f"; it marked {', '.join(marked)}" if marked else ""
                reasons.append(f"a transaction pushed without marking {message_id}{marks}")
    return reasons


def strict_delivery() -> bool:
    return os.environ.get("STABILIZE_STRICT_DELIVERY", "").strip().lower() not in ("", "0", "false", "no")


def clear_committed() -> None:
    """Forget what the previous delivery committed on this thread."""
    state = _state()
    state.marked.clear()
    state.pushes.clear()
    state.bare_pushes.clear()
    state.bare_events.clear()
    state.dropped_events = 0
    state.acked.clear()
