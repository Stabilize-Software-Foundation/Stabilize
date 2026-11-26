"""Announcements that a queue table has work, shared by every queue object on that table in this process."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Hashable

logger = logging.getLogger(__name__)

Listener = Callable[[float], None]

_lock = threading.Lock()
_listeners: dict[Hashable, tuple[Listener, ...]] = {}


def subscribe(key: Hashable | None, listener: Listener) -> None:
    if key is None:
        return
    with _lock:
        _listeners[key] = (*_listeners.get(key, ()), listener)


def unsubscribe(key: Hashable | None, listener: Listener) -> None:
    if key is None:
        return
    with _lock:
        kept = tuple(found for found in _listeners.get(key, ()) if found != listener)
        if kept:
            _listeners[key] = kept
        else:
            _listeners.pop(key, None)


def announce(key: Hashable | None, due_in: float = 0.0) -> None:
    """Tell every subscriber of *key* that a message is ready now, or will be in *due_in* seconds."""
    if key is None:
        return
    for listener in _listeners.get(key, ()):
        try:
            listener(due_in)
        except Exception:
            logger.exception("A queue wake listener failed")


def subscribers(key: Hashable | None) -> int:
    return len(_listeners.get(key, ())) if key is not None else 0
