"""Claiming, handing back and settling rows of the PostgreSQL queue."""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Any

from stabilize.queue.decode import MessageDecodeError, decode_message
from stabilize.queue.message_ids import delivery_key, row_id
from stabilize.queue.messages import get_message_type_name
from stabilize.queue.postgres_lookups import DELIVERABLE

if TYPE_CHECKING:
    from collections.abc import Hashable
    from contextlib import AbstractContextManager
    from datetime import timedelta

    from stabilize.queue.messages import Message

logger = logging.getLogger(__name__)

CLAIM_SQL = (
    "UPDATE {table} "
    "SET locked_until = NOW() + %(lock_seconds)s * INTERVAL '1 second', attempts = attempts + 1 "
    "WHERE id = ANY(ARRAY("
    "SELECT id FROM {table} "
    "WHERE deliver_at <= NOW() AND stabilize_tenant = %(tenant)s "
    "AND (locked_until IS NULL OR locked_until < NOW()) "
    f"AND {DELIVERABLE} "
    "ORDER BY deliver_at LIMIT %(limit)s FOR UPDATE SKIP LOCKED)) "
    "RETURNING id, message_type, payload::text AS payload, attempts, store_retries, deliver_at, "
    "EXTRACT(EPOCH FROM (NOW() - deliver_at)) AS ready_for"
)

CLAIMED_PUSH_SQL = (
    "INSERT INTO {table} (message_id, message_type, payload, deliver_at, attempts, locked_until, stabilize_tenant) "
    "VALUES (%(message_id)s, %(type)s, %(payload)s::jsonb, NOW(), 1, "
    "NOW() + %(lock_seconds)s * INTERVAL '1 second', %(tenant)s) RETURNING id"
)

RELEASE_SQL = (
    "UPDATE {table} SET locked_until = NULL, attempts = GREATEST(attempts - 1, 0) "
    "WHERE id = %(id)s AND (%(attempts)s::int IS NULL OR attempts = %(attempts)s::int) "
    "AND stabilize_tenant = %(tenant)s"
)


class PostgresClaimMixin:
    counts_deliveries = True

    table_name: str
    tenant: str
    connection_string: str
    schema: str | None
    lock_duration: timedelta
    max_attempts: int
    store_retry_budget: timedelta
    _pending: dict[int, dict[str, Any]]
    _size_cache: int | None

    if TYPE_CHECKING:

        def _borrow(self) -> AbstractContextManager[Any]: ...

        def move_to_dlq(self, message_id: int | str, error: str | None = None) -> None: ...

        def announce_ready(self, due_in: float = 0.0) -> None: ...

        def _serialize_message(self, message: Message) -> str: ...

    def wake_key(self) -> Hashable | None:
        return ("postgres", self.connection_string, self.schema or "", self.table_name, self.tenant)

    def poll_one(self) -> Message | None:
        """Poll for a single message without callback."""
        polled = self.poll_many(1)
        return polled[0] if polled else None

    def poll_many(self, limit: int) -> list[Message]:
        """Claim up to *limit* deliverable messages in one statement, oldest first."""
        if limit <= 0:
            return []
        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    CLAIM_SQL.format(table=self.table_name),
                    {
                        "lock_seconds": self.lock_duration.total_seconds(),
                        "max_attempts": self.max_attempts,
                        "budget": self.store_retry_budget.total_seconds(),
                        "tenant": self.tenant,
                        "limit": int(limit),
                    },
                )
                rows = cur.fetchall()
            conn.commit()
        messages: list[Message] = []
        for row in sorted(rows, key=lambda found: (found["deliver_at"], found["id"])):
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
        message.attempts = row["attempts"]
        message.store_retries = int(row["store_retries"] or 0)
        ready_for = row.get("ready_for") if isinstance(row, dict) else None
        message.__dict__["_ready_for"] = float(ready_for) if ready_for is not None else None
        self._pending[msg_id] = {"message": message, "type": msg_type}
        return message

    def push_claimed(self, message: Message, connection: Any) -> Message | None:
        """Insert *message* on *connection* already leased to this consumer; None when its payload does not decode."""
        message_type = get_message_type_name(message)
        payload = self._serialize_message(message)
        try:
            delivered = decode_message(message_type, payload)
        except MessageDecodeError:
            return None
        with connection.cursor() as cur:
            cur.execute(
                CLAIMED_PUSH_SQL.format(table=self.table_name),
                {
                    "message_id": str(uuid.uuid4()),
                    "type": message_type,
                    "payload": payload,
                    "lock_seconds": self.lock_duration.total_seconds(),
                    "tenant": self.tenant,
                },
            )
            row = cur.fetchone()
        msg_id = row["id"]
        delivered.message_id = delivery_key(self.table_name, msg_id)
        delivered.attempts = 1
        delivered.store_retries = 0
        self._pending[msg_id] = {"message": delivered, "type": message_type}
        self._size_cache = None
        return delivered

    def release(self, message: Message) -> bool:
        """Hand back a claimed message that no handler received: visible at once, its attempt not spent."""
        if not message.message_id:
            return False
        msg_id = row_id(message.message_id)
        with self._borrow() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    RELEASE_SQL.format(table=self.table_name),
                    {"id": msg_id, "attempts": getattr(message, "attempts", None), "tenant": self.tenant},
                )
                released = bool(cur.rowcount == 1)
            conn.commit()
        self._pending.pop(msg_id, None)
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
        self._size_cache = None
