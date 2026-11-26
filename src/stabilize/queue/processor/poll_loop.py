"""The processor's poll loop: reserve the free workers, claim that many messages, wait for work to be announced."""

from __future__ import annotations

import logging
import threading
import time
from typing import TYPE_CHECKING, Any

from stabilize.persistence.tenancy import TenancyModeChanged

if TYPE_CHECKING:
    from stabilize.queue import Queue
    from stabilize.queue.messages import Message

logger = logging.getLogger(__name__)


class WakeState:
    """Whether work was announced since the last poll began, and when the next delayed message is due."""

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._ready = False
        self._due: float | None = None

    def begin_poll(self) -> None:
        with self._cond:
            self._ready = False
            if self._due is not None and self._due <= time.monotonic():
                self._due = None

    def signal(self) -> None:
        with self._cond:
            self._ready = True
            self._cond.notify_all()

    def on_announce(self, due_in: float) -> None:
        if due_in <= 0:
            self.signal()
            return
        with self._cond:
            due = time.monotonic() + due_in
            if self._due is None or due < self._due:
                self._due = due
                self._cond.notify_all()

    def wait(self, timeout: float) -> bool:
        """Wait until work is announced, a delayed message is due, or *timeout* passes; True when announced."""
        end = time.monotonic() + timeout
        with self._cond:
            while not self._ready:
                limit = end if self._due is None else min(end, self._due)
                remaining = limit - time.monotonic()
                if remaining <= 0:
                    return False
                self._cond.wait(remaining)
            self._ready = False
            return True


def offers(queue: Any, name: str) -> bool:
    """Whether the queue's class defines *name*; an attribute a test double invents on access does not count."""
    return callable(getattr(type(queue), name, None))


def poll_many(queue: Any, limit: int) -> list[Message]:
    if offers(queue, "poll_many"):
        return list(queue.poll_many(limit))
    polled: list[Message] = []
    while len(polled) < limit:
        message = queue.poll_one()
        if message is None:
            break
        polled.append(message)
    return polled


def release(queue: Any, message: Message) -> None:
    from datetime import timedelta

    if offers(queue, "release"):
        queue.release(message)
    else:
        queue.reschedule(message, timedelta(0))


class PollLoopMixin:
    queue: Queue
    config: Any
    _lock: threading.Lock
    _active_count: int
    _running: bool
    _stopping: bool
    _saturated: bool
    _wake: WakeState
    _stop_event: threading.Event
    _idle_timed_out: bool

    if TYPE_CHECKING:

        def _submit_message_internal(self, message: Message) -> None: ...

        def _latch(self, exc: BaseException) -> None: ...

        def _check_dlq(self) -> None: ...

        def sample_metrics(self) -> None: ...

        def _idle_seconds(self) -> float: ...

        def _judge_poll(self, polled: list[Message]) -> None: ...

    def _reserve(self) -> int:
        with self._lock:
            if self._stopping:
                return 0
            free = int(self.config.max_workers) - self._active_count
            if free <= 0:
                self._saturated = True
                return 0
            self._active_count += free
            return free

    def _claim(self, free: int) -> list[Message]:
        polled: list[Message] = []
        hand_back = False
        try:
            polled = poll_many(self.queue, free)
        finally:
            with self._lock:
                self._active_count -= free - len(polled)
                if polled and self._stopping:
                    self._active_count -= len(polled)
                    hand_back = True
        if not hand_back:
            return polled
        for message in polled:
            try:
                release(self.queue, message)
            except Exception as error:
                logger.warning("A message polled during a stop could not be handed back: %s", error)
        return []

    def _worker_finished(self) -> None:
        with self._lock:
            self._active_count -= 1
            waiting = self._saturated
            self._saturated = False
        if waiting:
            self._wake.signal()

    def _poll_loop(self) -> None:
        poll_interval = float(self.config.poll_frequency_ms) / 1000.0
        dlq_interval = getattr(self.config, "dlq_check_interval_seconds", 0.0)
        last_dlq_check = time.monotonic()
        sample_interval = getattr(self.config, "metrics_sample_interval_seconds", 0.0)
        last_sample = time.monotonic()

        while self._running:
            try:
                if dlq_interval > 0 and time.monotonic() - last_dlq_check >= dlq_interval:
                    last_dlq_check = time.monotonic()
                    self._check_dlq()
                if sample_interval > 0 and time.monotonic() - last_sample >= sample_interval:
                    last_sample = time.monotonic()
                    self.sample_metrics()

                free = self._reserve()
                if free == 0:
                    self._wake.wait(poll_interval)
                    continue
                self._wake.begin_poll()
                polled = self._claim(free)
                self._judge_poll(polled)
                for index, message in enumerate(polled):
                    try:
                        self._submit_message_internal(message)
                    except BaseException:
                        self._abandon(polled[index + 1 :])
                        raise
                if len(polled) < free:
                    self._idle_timed_out = not self._wake.wait(self._idle_seconds())

            except TenancyModeChanged as e:
                self._latch(e)
                break
            except Exception:
                logger.exception("Error in poll loop")
                if self.config.stop_on_error:
                    self._running = False
                    break
                self._stop_event.wait(poll_interval)

    def _abandon(self, messages: list[Message]) -> None:
        if not messages:
            return
        with self._lock:
            self._active_count -= len(messages)
        for message in messages:
            try:
                release(self.queue, message)
            except Exception as error:
                logger.warning("A polled message could not be handed back: %s", error)
