"""The stop event of the processor whose thread is running, so a wait for a pooled connection can end."""

from __future__ import annotations

import time
from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import threading
    from collections.abc import Callable, Iterator
    from contextlib import AbstractContextManager

SLICE_SECONDS = 5.0
STOPPING_WAIT_SECONDS = 1.0
DEFAULT_POOL_TIMEOUT_SECONDS = 30.0

_stop: ContextVar[threading.Event | None] = ContextVar("stabilize_processor_stop", default=None)


def current_stop() -> threading.Event | None:
    return _stop.get()


def bound_to(event: threading.Event, target: Callable[..., Any]) -> Callable[..., Any]:
    """*target*, run with *event* as the stop event its borrows watch."""

    def run(*args: Any, **kwargs: Any) -> Any:
        token = _stop.set(event)
        try:
            return target(*args, **kwargs)
        finally:
            _stop.reset(token)

    return run


def acquire(pool: Any) -> AbstractContextManager[Any]:
    """A pooled connection. On a processor's own threads the wait is cut short once that processor is stopping."""
    stop = _stop.get()
    if stop is None:
        connection: AbstractContextManager[Any] = pool.connection()
        return connection
    return _watched(pool, stop)


@contextmanager
def _watched(pool: Any, stop: threading.Event) -> Iterator[Any]:
    from psycopg_pool import PoolTimeout

    total = float(getattr(pool, "timeout", DEFAULT_POOL_TIMEOUT_SECONDS) or DEFAULT_POOL_TIMEOUT_SECONDS)
    deadline = time.monotonic() + total
    while True:
        stopping = stop.is_set()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise PoolTimeout(f"couldn't get a connection after {total:.2f} sec")
        borrowed = pool.connection(timeout=min(STOPPING_WAIT_SECONDS if stopping else SLICE_SECONDS, remaining))
        try:
            conn = borrowed.__enter__()
        except PoolTimeout:
            if stopping:
                raise
            continue
        break
    try:
        yield conn
    except BaseException as error:
        if not borrowed.__exit__(type(error), error, error.__traceback__):
            raise
    else:
        borrowed.__exit__(None, None, None)
