"""Queue interface and common types.

This module provides the abstract Queue interface and common types
used by all queue implementations.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Hashable
    from datetime import timedelta

    from stabilize.queue.messages import Message


class QueueFullError(Exception):
    """Raised when the queue is full and overflow policy is 'raise'."""


@dataclass(frozen=True)
class QueueCounts:
    """What a queue table holds right now: deliverable, leased, and abandoned messages."""

    pending: int = 0
    processing: int = 0
    stuck: int = 0


class Queue(ABC):
    """Abstract queue interface for message handling.

    The queue is the core of the execution engine, managing all messages
    that drive stage and task execution.
    """

    @abstractmethod
    def push(
        self,
        message: Message,
        delay: timedelta | None = None,
        connection: Any | None = None,
    ) -> None:
        """Push a message onto the queue.

        Args:
            message: The message to push
            delay: Optional delay before message is delivered
            connection: Optional database connection for atomic transactions
        """

    @abstractmethod
    def poll(self, callback: Callable[[Message], None]) -> None:
        """Poll for a message and process it with the callback.

        If a message is available, calls callback(message).
        After callback returns, the message is automatically acknowledged.

        Args:
            callback: Function to call with the message
        """

    @abstractmethod
    def poll_one(self) -> Message | None:
        """Poll for a single message without callback.

        Returns the message if available, None otherwise.
        Message must be manually acknowledged.

        Returns:
            The message or None
        """

    def poll_many(self, limit: int) -> list[Message]:
        """Claim up to *limit* deliverable messages; each one must be acknowledged, rescheduled or released."""
        polled: list[Message] = []
        while len(polled) < limit:
            message = self.poll_one()
            if message is None:
                break
            polled.append(message)
        return polled

    def release(self, message: Message) -> bool:
        """Hand back a claimed message that no handler received: visible at once, its attempt not spent.

        Returns False when the lease was taken by another delivery since the claim.
        """
        from datetime import timedelta

        self.reschedule(message, timedelta(0))
        return True

    def settle(self, message: Message) -> None:
        """Forget a claimed message whose row a committed transaction already removed."""
        return

    def push_claimed(self, message: Message, connection: Any) -> Message | None:
        """Insert *message* on *connection* already leased to this consumer and return it as a poll would.

        Returns None when the queue cannot do so; the caller then pushes it normally.
        """
        return None

    def wake_key(self) -> Hashable | None:
        """What identifies this queue's table in this process, for announcements between queue objects."""
        return ("object", id(self))

    def announce_ready(self, due_in: float = 0.0) -> None:
        """Tell this process's consumers of the table that a message is ready now or in *due_in* seconds."""
        from stabilize.queue import wake

        wake.announce(self.wake_key(), due_in)

    def next_due_in(self) -> float | None:
        """Seconds until a message can next be delivered, by delay or by an expiring lease; None when unknown."""
        return None

    def wake_listener(self, on_wake: Callable[[], None], ping_seconds: float) -> Any:
        """A listener that calls *on_wake* when another process pushes, or None when this queue has no such channel."""
        return None

    def live_delivery_keys(self, keys: list[str]) -> set[str]:
        """The keys among keys whose message is still in this queue; every key when the queue cannot tell."""
        return set(keys)

    @abstractmethod
    def ack(self, message: Message) -> None:
        """Acknowledge a message, removing it from the queue.

        Args:
            message: The message to acknowledge
        """

    @abstractmethod
    def ensure(
        self,
        message: Message,
        delay: timedelta,
    ) -> None:
        """Ensure a message is in the queue with the given delay.

        If the message is already in the queue, updates its delay.
        If not, adds it with the given delay.

        Args:
            message: The message to ensure
            delay: Delay before message is delivered
        """

    @abstractmethod
    def reschedule(
        self,
        message: Message,
        delay: timedelta,
        *,
        store_unavailable: bool = False,
    ) -> None:
        """Reschedule a message with a new delay.

        Args:
            message: The message to reschedule
            delay: New delay before message is delivered
            store_unavailable: The handler failed because the store was unreachable;
                the message spends its store-retry budget instead of an attempt.
        """

    @abstractmethod
    def size(self) -> int:
        """Get the number of messages in the queue."""

    def next_ready_in(self) -> float | None:
        """Seconds until the earliest message this consumer can poll; 0 when one is ready, None when none is pending."""
        return 0.0 if self.size() > 0 else None

    def counts(self, stuck_after_seconds: int = 300, *, every_tenant: bool = False) -> QueueCounts:
        """Pending (deliverable now), processing (leased) and stuck (lease expired *stuck_after_seconds* ago or more).

        *every_tenant* drops the tenant predicate for an operator view of the whole table.
        """
        return QueueCounts(pending=self.size())

    def oldest_ready_age_seconds(self) -> float | None:
        """Seconds the oldest message this consumer could poll right now has been waiting; None when none is ready."""
        return None

    @abstractmethod
    def clear(self) -> None:
        """Clear all messages from the queue."""

    def has_pending_message_for_task(self, task_id: str) -> bool:
        """Check if there's already a pending message for a specific task.

        Used by recovery to prevent creating duplicate messages.
        Default implementation returns False (no deduplication).
        Queue implementations should override this for proper recovery behavior.

        Args:
            task_id: The task ID to check for

        Returns:
            True if a pending message exists for this task
        """
        return False

    def has_pending_message_for_execution(self, execution_id: str) -> bool:
        """Whether any queued message targets this workflow. Default: False."""
        return False

    def has_pending_message_for_stage(self, stage_id: str) -> bool:
        """Whether any queued message (including one being handled) names this stage. Default: False."""
        return False

    def has_pending_message_for_config(self, pipeline_config_id: str) -> bool:
        """Whether a queued message names this pipeline config (a buffered wake-up in flight)."""
        return False

    def dead_letter_moved_at_ms(self, message_type: str, execution_id: str, target_id: str | None) -> int | None:
        """When the newest dead-letter row for this message (type, workflow and task or stage) was moved, or None."""
        return None

    def extend_lock(self, message: Message, duration: timedelta | None = None) -> bool:
        """Extend the visibility lock of an in-flight message (heartbeat).

        Called periodically by the processor while a handler is still
        executing, so the message does not become visible to other pollers
        (and get re-executed) just because it outlived lock_duration. The
        lock lapses naturally when the processing worker dies.

        Args:
            message: The in-flight message whose lock to renew
            duration: Lock extension from now (defaults to the queue's
                lock_duration)

        Returns:
            True if the lock was extended; False if unsupported or the
            message no longer exists.
        """
        # Default implementation: heartbeating not supported
        return False

    def purge_workflow(self, execution_id: str) -> int:
        """Delete every queued and dead-lettered message for one workflow.

        Args:
            execution_id: The workflow whose messages to delete

        Returns:
            Number of rows deleted. The default implementation deletes nothing.
        """
        return 0


@dataclass(order=True)
class QueuedMessage:
    """A message with its delivery time for priority queue ordering."""

    deliver_at: float
    message: Message = field(compare=False)
    message_id: str = field(compare=False, default="")
