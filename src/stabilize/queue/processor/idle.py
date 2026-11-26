"""The idle poll interval: stretched while a wake channel across processes is proven, pulled back when it is not."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

from stabilize.queue.processor.poll_loop import offers

if TYPE_CHECKING:
    from stabilize.queue import Queue
    from stabilize.queue.messages import Message
    from stabilize.queue.processor.poll_loop import WakeState

logger = logging.getLogger(__name__)

MIN_LATE_SECONDS = 0.2
READY_NOW_WAIT_SECONDS = 0.01


class IdlePollMixin:
    queue: Queue
    config: Any
    _wake: WakeState
    _listener: Any
    _guard_until: float
    _idle_timed_out: bool
    _stretched_wait: bool

    def _start_listener(self) -> None:
        self._listener = None
        if not getattr(self.config, "idle_poll_ms", None):
            return
        listener = None
        if offers(self.queue, "wake_listener"):
            listener = self.queue.wake_listener(self._wake.signal, float(self.config.wake_ping_seconds))
        if listener is None:
            logger.warning(
                "idle_poll_ms is set, but %s has no wake channel across processes, so the processor polls "
                "every poll_frequency_ms. Use PostgresQueue(wake_notify=True).",
                type(self.queue).__name__,
            )
            return
        listener.start()
        self._listener = listener

    def _stop_listener(self) -> None:
        listener, self._listener = self._listener, None
        if listener is not None:
            listener.stop()

    @property
    def wake_channel_proven(self) -> bool:
        """Whether a notification this processor sent through its pool reached its listener recently."""
        listener = self._listener
        return bool(listener is not None and listener.proven)

    @property
    def idle_interval_ms(self) -> int:
        """How long the processor waits after a poll that found the queue drained, when nothing is announced."""
        idle = getattr(self.config, "idle_poll_ms", None)
        if idle and self.wake_channel_proven and time.monotonic() >= self._guard_until:
            return int(idle)
        return int(self.config.poll_frequency_ms)

    def _idle_seconds(self) -> float:
        interval = self.idle_interval_ms
        self._stretched_wait = interval > int(self.config.poll_frequency_ms)
        if self._stretched_wait:
            due = self.queue.next_due_in() if offers(self.queue, "next_due_in") else None
            if due is not None:
                self._wake.on_announce(max(READY_NOW_WAIT_SECONDS, float(due)))
        return interval / 1000.0

    def _judge_poll(self, polled: list[Message]) -> None:
        timed_out, self._idle_timed_out = self._idle_timed_out, False
        stretched, self._stretched_wait = self._stretched_wait, False
        if not (timed_out and stretched and polled):
            return
        threshold = max(2 * float(self.config.poll_frequency_ms) / 1000.0, MIN_LATE_SECONDS)
        late = [
            float(message.__dict__.get("_ready_for") or 0.0)
            for message in polled
            if getattr(message, "attempts", None) == 1
        ]
        worst = max(late, default=0.0)
        if worst <= threshold:
            return
        hold = float(self.config.wake_guard_hold_seconds)
        self._guard_until = time.monotonic() + hold
        logger.warning(
            "A message was ready for %.2f s and no notification announced it, so a publisher on this queue does "
            "not notify. Set wake_notify=True on every PostgresQueue that pushes to %s, or unset idle_poll_ms. "
            "Polling every poll_frequency_ms for the next %.0f s.",
            worst,
            getattr(self.queue, "table_name", "the queue table"),
            hold,
        )
