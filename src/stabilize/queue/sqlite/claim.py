"""Claiming, handing back and settling rows of the SQLite queue."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from stabilize.queue.decode import MessageDecodeError, decode_message
from stabilize.queue.message_ids import delivery_key, row_id

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Hashable
    from datetime import timedelta

    from stabilize.queue.messages import Message

logger = logging.getLogger(__name__)

DELIVERABLE = (
    "(attempts < :max_attempts OR (store_retry_since IS NOT NULL "
    "AND julianday(store_retry_since) > julianday(:now) - :budget_days))"
)


def sqlite_wake_key(manager: Any, connection_string: str, table_name: str, tenant: str) -> Hashable:
    return ("sqlite", manager._parse_sqlite_path(connection_string), table_name, tenant)


class SqliteClaimMixin:
    table_name: str
    tenant: str
    connection_string: str
    lock_duration: timedelta
    max_attempts: int
    _manager: Any
    _pending: dict[int, dict[str, Any]]

    if TYPE_CHECKING:

        def _get_connection(self) -> sqlite3.Connection: ...

        def _budget_days(self) -> float: ...

        def move_to_dlq(self, message_id: int | str, error: str | None = None) -> None: ...

        def announce_ready(self, due_in: float = 0.0) -> None: ...

    def wake_key(self) -> Hashable | None:
        return sqlite_wake_key(self._manager, self.connection_string, self.table_name, self.tenant)

    def poll_one(self) -> Message | None:
        """Poll for a single message, claimed by an optimistic update on the row's version."""
        polled = self.poll_many(1)
        return polled[0] if polled else None

    def poll_many(self, limit: int) -> list[Message]:
        """Claim up to *limit* deliverable messages in one commit, oldest first.

        A candidate another consumer claimed between the read and the update is left to it.
        """
        if limit <= 0:
            return []
        conn = self._get_connection()
        now = datetime.now(UTC)
        candidates = conn.execute(
            f"""
            SELECT id, message_type, payload, attempts, version, store_retries
            FROM {self.table_name}
            WHERE julianday(deliver_at) <= julianday(:now)
            AND stabilize_tenant = :tenant
            AND (locked_until IS NULL OR julianday(locked_until) < julianday(:now))
            AND {DELIVERABLE}
            ORDER BY deliver_at
            LIMIT :limit
            """,
            {
                "max_attempts": self.max_attempts,
                "now": now.isoformat(),
                "tenant": self.tenant,
                "budget_days": self._budget_days(),
                "limit": int(limit),
            },
        ).fetchall()
        if not candidates:
            return []
        locked_until = (now + self.lock_duration).isoformat()
        claimed: list[Any] = []
        for row in candidates:
            cursor = conn.execute(
                f"""
                UPDATE {self.table_name}
                SET locked_until = :locked_until,
                    attempts = attempts + 1,
                    version = version + 1
                WHERE id = :id AND version = :version AND stabilize_tenant = :tenant
                """,
                {"id": row["id"], "locked_until": locked_until, "version": row["version"], "tenant": self.tenant},
            )
            if cursor.rowcount == 1:
                claimed.append(row)
            else:
                logger.debug("Lost race for message %s, will retry", row["id"])
        conn.commit()
        messages: list[Message] = []
        for row in claimed:
            message = self._delivered(row)
            if message is not None:
                messages.append(message)
        return messages

    def _delivered(self, row: Any) -> Message | None:
        msg_id = row["id"]
        msg_type = row["message_type"]
        try:
            message = decode_message(msg_type, row["payload"])
        except MessageDecodeError as exc:
            logger.error("Moving undecodable message %s to the DLQ: %s", msg_id, exc)
            self.move_to_dlq(msg_id, error=f"Deserialization failed: {exc}")
            return None
        message.message_id = delivery_key(self.table_name, msg_id)
        message.attempts = row["attempts"] + 1
        message.store_retries = int(row["store_retries"] or 0)
        self._pending[msg_id] = {"message": message, "type": msg_type}
        logger.debug("Polled %s (id=%s, attempts=%d)", msg_type, msg_id, message.attempts)
        return message

    def release(self, message: Message) -> bool:
        """Hand back a claimed message that no handler received: visible at once, its attempt not spent."""
        if not message.message_id:
            return False
        msg_id = row_id(message.message_id)
        conn = self._get_connection()
        cursor = conn.execute(
            f"UPDATE {self.table_name} SET locked_until = NULL, attempts = MAX(attempts - 1, 0), "
            "version = version + 1 WHERE id = :id AND (:attempts IS NULL OR attempts = :attempts) "
            "AND stabilize_tenant = :tenant",
            {"id": msg_id, "attempts": getattr(message, "attempts", None), "tenant": self.tenant},
        )
        conn.commit()
        self._pending.pop(msg_id, None)
        released = bool(cursor.rowcount == 1)
        if released:
            self.announce_ready()
        return released

    def settle(self, message: Message) -> None:
        """Forget a claimed message whose row a committed transaction already removed."""
        if not message.message_id:
            return
        try:
            self._pending.pop(row_id(message.message_id), None)
        except ValueError:
            return
