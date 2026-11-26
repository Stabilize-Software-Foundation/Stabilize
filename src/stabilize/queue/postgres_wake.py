"""NOTIFY on a push and a LISTEN connection for a consumer, with a loopback that proves the channel works."""

from __future__ import annotations

import hashlib
import logging
import threading
import time
import uuid
from typing import TYPE_CHECKING, Any

from stabilize.queue.postgres_lookups import DELIVERABLE

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractContextManager
    from datetime import timedelta

logger = logging.getLogger(__name__)

LISTENER_THREAD_NAME = "stabilize-wake-listener"
LISTENER_APPLICATION_SUFFIX = " wake-listener"
PING_PREFIX = "ping:"
PROOF_GRACE_SECONDS = 2.0
MIN_PSYCOPG = (3, 2)

NEXT_DUE_SQL = (
    "SELECT EXTRACT(EPOCH FROM (MIN(GREATEST(deliver_at, COALESCE(locked_until, deliver_at))) - NOW())) AS wait "
    "FROM {table} WHERE stabilize_tenant = %(tenant)s "
    f"AND {DELIVERABLE}"
)


def channel_name(schema: str, table_name: str, tenant: str) -> str:
    digest = hashlib.sha256(f"{schema}\x1f{table_name}\x1f{tenant}".encode()).hexdigest()
    return f"stabilize_wake_{digest[:32]}"


class PostgresWakeMixin:
    table_name: str
    tenant: str
    connection_string: str
    schema: str | None
    max_attempts: int
    store_retry_budget: timedelta
    wake_notify: bool
    _wake_channel_name: str | None
    _pool_options: Any

    if TYPE_CHECKING:

        def _borrow(self) -> AbstractContextManager[Any]: ...

    def wake_channel(self, conn: Any) -> str:
        """The NOTIFY channel of this table and tenant, named from the schema the connection resolves."""
        if self._wake_channel_name is None:
            schema = self.schema
            if schema is None:
                with conn.cursor() as cur:
                    cur.execute("SELECT current_schema() AS schema")
                    row = cur.fetchone()
                schema = str(row["schema"] if isinstance(row, dict) else row[0])
            self._wake_channel_name = channel_name(schema, self.table_name, self.tenant)
        return self._wake_channel_name

    def notify_in(self, conn: Any, payload: str = "") -> None:
        """Queue a notification on *conn*'s transaction; consumers in other processes hear it at commit."""
        if not self.wake_notify:
            return
        channel = self.wake_channel(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT pg_notify(%(channel)s, %(payload)s)", {"channel": channel, "payload": payload})

    def notify_now(self, payload: str = "") -> None:
        if not self.wake_notify:
            return
        with self._borrow() as conn:
            self.notify_in(conn, payload)
            conn.commit()

    def next_due_in(self) -> float | None:
        """Seconds until a message of this tenant can next be delivered, by delay or by an expiring lease."""
        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    NEXT_DUE_SQL.format(table=self.table_name),
                    {
                        "tenant": self.tenant,
                        "max_attempts": self.max_attempts,
                        "budget": self.store_retry_budget.total_seconds(),
                    },
                )
                row = cur.fetchone()
            conn.commit()
        wait = row["wait"] if row else None
        return None if wait is None else max(0.0, float(wait))

    def wake_listener(self, on_wake: Callable[[], None], ping_seconds: float) -> WakeListener | None:
        """A listener on this queue's channel, or None when this queue's pushes do not notify."""
        if not self.wake_notify:
            return None
        return WakeListener(self, on_wake, ping_seconds)


def require_psycopg() -> None:
    import psycopg

    found = tuple(int(part) for part in psycopg.__version__.split(".")[:2] if part.isdigit())
    if found < MIN_PSYCOPG:
        raise RuntimeError(
            f"idle_poll_ms needs psycopg {MIN_PSYCOPG[0]}.{MIN_PSYCOPG[1]} or later to wait for notifications "
            f"with a timeout; {psycopg.__version__} is installed"
        )


class WakeListener:
    """One LISTEN connection. It calls *on_wake* for a notification and proves itself with a ping it sends."""

    def __init__(self, queue: Any, on_wake: Callable[[], None], ping_seconds: float) -> None:
        self._queue = queue
        self._on_wake = on_wake
        self._ping_seconds = max(0.2, float(ping_seconds))
        self._token = uuid.uuid4().hex[:12]
        self._sent = 0
        self._proved_at: float | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._conn: Any = None

    @property
    def proven(self) -> bool:
        """Whether a ping sent through the queue's pool arrived on the listener recently enough."""
        proved_at = self._proved_at
        if proved_at is None:
            return False
        return time.monotonic() - proved_at <= self._ping_seconds + PROOF_GRACE_SECONDS

    def start(self) -> None:
        require_psycopg()
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name=LISTENER_THREAD_NAME)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        conn = self._conn
        if conn is not None:
            try:
                conn.close()
            except Exception as error:
                logger.debug("Closing the wake listener connection failed: %s", error)
        if self._thread is not None:
            self._thread.join(timeout=5.0)

    def _connect(self) -> Any:
        import psycopg

        from stabilize.persistence.connection import engine_application_name

        options = getattr(self._queue, "_pool_options", None)
        kwargs = dict(getattr(options, "connect_kwargs", None) or {})
        for name in ("row_factory", "options", "application_name", "autocommit"):
            kwargs.pop(name, None)
        base = engine_application_name()[: 63 - len(LISTENER_APPLICATION_SUFFIX)]
        return psycopg.connect(
            self._queue.connection_string,
            autocommit=True,
            application_name=base + LISTENER_APPLICATION_SUFFIX,
            **kwargs,
        )

    def _channel(self) -> str:
        with self._queue._borrow() as conn:
            channel = str(self._queue.wake_channel(conn))
            conn.commit()
        return channel

    def _ping(self) -> None:
        self._sent += 1
        self._queue.notify_now(f"{PING_PREFIX}{self._token}:{self._sent}")

    def _run(self) -> None:
        backoff = 1.0
        reported = False
        while not self._stop.is_set():
            try:
                channel = self._channel()
                self._conn = self._connect()
                self._conn.execute(f'LISTEN "{channel}"')
                backoff = 1.0
                reported = False
                self._listen()
            except Exception as error:
                self._proved_at = None
                if self._stop.is_set():
                    break
                if not reported:
                    logger.warning(
                        "The wake listener lost its connection; the processor polls at poll_frequency_ms "
                        "until it is back: %s",
                        error,
                    )
                    reported = True
                self._stop.wait(backoff)
                backoff = min(30.0, backoff * 2)
            finally:
                conn, self._conn = self._conn, None
                if conn is not None:
                    try:
                        conn.close()
                    except Exception as error:
                        logger.debug("Closing the wake listener connection failed: %s", error)

    def _listen(self) -> None:
        next_ping = time.monotonic()
        while not self._stop.is_set():
            now = time.monotonic()
            if now >= next_ping:
                self._ping()
                next_ping = now + self._ping_seconds
            wait = max(0.05, min(1.0, next_ping - time.monotonic()))
            for notification in self._conn.notifies(timeout=wait, stop_after=1):
                payload = str(notification.payload or "")
                if payload.startswith(PING_PREFIX):
                    if payload.startswith(f"{PING_PREFIX}{self._token}:"):
                        self._proved_at = time.monotonic()
                else:
                    self._on_wake()
