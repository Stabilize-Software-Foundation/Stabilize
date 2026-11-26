"""The processor takes work when a worker frees or a push commits, and holds no lock while it polls (#116)."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from stabilize import QueueProcessor
from stabilize.queue.messages import StartWorkflow
from stabilize.queue.processor.config import QueueProcessorConfig
from tests.event_stack import Stack, from_another_thread, open_stack


@pytest.fixture
def stack(backend: str, tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[Stack]:
    with open_stack(backend, tmp_path, request) as opened:
        yield opened


def _message(index: int) -> StartWorkflow:
    return StartWorkflow(execution_type="PIPELINE", execution_id=f"wake-{index}")


def _processor(stack: Stack, handler: Callable[[StartWorkflow], None], **config: Any) -> QueueProcessor:
    processor = QueueProcessor(stack.queue, config=QueueProcessorConfig(enable_deduplication=False, **config))
    processor.register_handler_func(StartWorkflow, handler)
    return processor


def _wait(condition: Callable[[], bool], seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.005)
    return condition()


def _gate_first_poll(queue: Any) -> tuple[threading.Event, threading.Event]:
    entered = threading.Event()
    go = threading.Event()
    used: list[bool] = []
    guard = threading.Lock()

    def gated(original: Callable[..., Any]) -> Callable[..., Any]:
        def poll(*args: Any, **kwargs: Any) -> Any:
            with guard:
                first = not used
                used.append(True)
            if first:
                entered.set()
                go.wait(30.0)
            return original(*args, **kwargs)

        return poll

    for name in ("poll_many", "poll_one"):
        original = getattr(queue, name, None)
        if callable(original):
            setattr(queue, name, gated(original))
            break
    return entered, go


def test_a_full_pool_takes_the_next_message_when_a_worker_frees(stack: Stack) -> None:
    handled: list[float] = []

    def handler(message: StartWorkflow) -> None:
        time.sleep(0.05)
        handled.append(time.monotonic())

    for index in range(12):
        stack.queue.push(_message(index))
    processor = _processor(stack, handler, max_workers=2, poll_frequency_ms=500)
    started = time.monotonic()
    processor.start()
    try:
        assert _wait(lambda: len(handled) == 12, 20.0), f"only {len(handled)} of 12 messages were handled in 20 s"
    finally:
        processor.stop()
    elapsed = max(handled) - started
    assert elapsed < 1.5, (
        f"12 messages of 50 ms on 2 workers took {elapsed:.2f} s: the processor waited a poll interval "
        "while a worker was free"
    )


def test_a_committed_push_is_taken_without_waiting_for_the_poll_interval(stack: Stack) -> None:
    handled: list[float] = []
    processor = _processor(stack, lambda message: handled.append(time.monotonic()), poll_frequency_ms=1000)
    processor.start()
    try:
        time.sleep(0.3)
        pushed = time.monotonic()
        stack.queue.push(_message(0))
        assert _wait(lambda: bool(handled), 10.0), "the pushed message was never handled"
    finally:
        processor.stop()
    latency = handled[0] - pushed
    assert latency < 0.3, (
        f"a message pushed in the same process was handled after {latency:.2f} s: "
        "the processor waited a poll interval for a pushed message"
    )


def test_without_an_announcement_the_poll_interval_still_finds_the_message(
    stack: Stack, monkeypatch: pytest.MonkeyPatch
) -> None:
    from stabilize.queue import wake

    monkeypatch.setattr(wake, "announce", lambda *args, **kwargs: None)
    handled: list[float] = []
    processor = _processor(stack, lambda message: handled.append(time.monotonic()), poll_frequency_ms=300)
    processor.start()
    try:
        time.sleep(0.1)
        stack.queue.push(_message(0))
        assert _wait(lambda: bool(handled), 5.0), "the poll interval did not find a message nobody announced"
    finally:
        processor.stop()


def test_active_count_answers_while_a_poll_is_in_flight(stack: Stack) -> None:
    entered, go = _gate_first_poll(stack.queue)
    processor = _processor(stack, lambda message: None, poll_frequency_ms=20)
    processor.start()
    try:
        assert entered.wait(10.0), "the processor never polled"
        try:
            from_another_thread(lambda: processor.active_count, timeout=1.0)
        except TimeoutError:
            pytest.fail("active_count waited for the poll to return: the processor lock is held across the poll")
    finally:
        go.set()
        processor.stop()


def test_a_poll_that_raises_leaves_no_worker_slot_reserved(stack: Stack) -> None:
    calls: list[int] = []

    def broken(*args: Any, **kwargs: Any) -> Any:
        calls.append(1)
        raise RuntimeError("injected poll failure")

    for name in ("poll_many", "poll_one"):
        if callable(getattr(stack.queue, name, None)):
            setattr(stack.queue, name, broken)
    processor = _processor(stack, lambda message: None, poll_frequency_ms=20, max_workers=3)
    processor.start()
    try:
        assert _wait(lambda: len(calls) >= 3, 10.0), "the processor stopped polling after a failed poll"
        assert _wait(lambda: processor.active_count == 0, 2.0), (
            f"active_count is {processor.active_count} after failed polls: a reserved worker slot was not returned"
        )
    finally:
        processor.stop()


def test_a_poll_that_returns_after_stop_was_requested_hands_its_message_back(stack: Stack) -> None:
    handled: list[StartWorkflow] = []
    stack.queue.push(_message(0))
    original_poll_one = stack.queue.poll_one
    entered, go = _gate_first_poll(stack.queue)
    processor = _processor(stack, handled.append, poll_frequency_ms=20)
    processor.start()
    try:
        assert entered.wait(10.0), "the processor never polled"
        processor.request_stop()
        go.set()
        time.sleep(0.5)
    finally:
        processor.stop()
    assert not handled, "a message was handled although stop was requested before its poll returned"
    stack.queue.poll_one = original_poll_one
    again = stack.queue.poll_one()
    assert again is not None, "the message polled during the stop is not visible: it was not handed back"
    assert again.attempts == 1, f"the handed-back message is at attempt {again.attempts}: the stop spent an attempt"
    stack.queue.ack(again)


def test_poll_many_claims_at_most_what_was_asked_and_never_the_same_row_twice(stack: Stack) -> None:
    for index in range(5):
        stack.queue.push(_message(index))

    first = stack.queue.poll_many(3)
    second = stack.queue.poll_many(3)
    third = stack.queue.poll_many(3)

    assert len(first) == 3 and len(second) == 2 and third == []
    ids = [m.message_id for m in first + second]
    assert len(set(ids)) == 5, f"poll_many handed out a row twice: {ids}"
    assert {m.attempts for m in first + second} == {1}
    for message in first + second:
        stack.queue.ack(message)
    assert stack.queue.poll_one() is None


def test_poll_many_on_postgres_claims_in_one_statement(stack: Stack, monkeypatch: pytest.MonkeyPatch) -> None:
    if stack.backend != "postgres":
        pytest.skip("counts PostgreSQL statements")
    import psycopg

    for index in range(4):
        stack.queue.push(_message(index))
    claims: list[str] = []
    real = psycopg.Cursor.execute

    def execute(self: Any, query: Any, *args: Any, **kwargs: Any) -> Any:
        text = query.as_string(None) if hasattr(query, "as_string") else str(query)
        if "locked_until" in text and text.lstrip().upper().startswith("UPDATE"):
            claims.append(text)
        return real(self, query, *args, **kwargs)

    monkeypatch.setattr(psycopg.Cursor, "execute", execute)
    polled = stack.queue.poll_many(4)
    monkeypatch.setattr(psycopg.Cursor, "execute", real)

    assert len(polled) == 4
    assert len(claims) == 1, f"poll_many(4) issued {len(claims)} claiming statements for 4 messages"
    for message in polled:
        stack.queue.ack(message)


def test_release_restores_the_attempt_and_keeps_the_store_retry_budget(stack: Stack) -> None:
    from datetime import timedelta

    stack.queue.push(_message(0))
    first = stack.queue.poll_one()
    assert first is not None and first.attempts == 1
    stack.queue.reschedule(first, timedelta(0), store_unavailable=True)
    second = stack.queue.poll_one()
    assert second is not None and second.attempts == 2 and second.store_retries == 1

    assert stack.queue.release(second) is True

    third = stack.queue.poll_one()
    assert third is not None, "a released message is not visible"
    assert third.attempts == 2, f"release left the message at attempt {third.attempts}, not the 2 it was polled at"
    assert third.store_retries == 1, "release reset the store-retry budget"
    stack.queue.ack(third)


def test_reschedule_without_a_store_failure_still_resets_the_budget(stack: Stack) -> None:
    from datetime import timedelta

    stack.queue.push(_message(0))
    first = stack.queue.poll_one()
    assert first is not None
    stack.queue.reschedule(first, timedelta(0), store_unavailable=True)
    second = stack.queue.poll_one()
    assert second is not None and second.store_retries == 1
    stack.queue.reschedule(second, timedelta(0))
    third = stack.queue.poll_one()
    assert third is not None and third.store_retries == 0
    stack.queue.ack(third)


def test_release_refuses_a_lease_that_another_poll_has_taken(stack: Stack) -> None:
    from datetime import timedelta

    stack.queue.push(_message(0))
    stale = stack.queue.poll_one()
    assert stale is not None
    stack.queue.reschedule(stale, timedelta(0))
    current = stack.queue.poll_one()
    assert current is not None and current.attempts == 2

    assert stack.queue.release(stale) is False

    assert stack.queue.poll_one() is None, "a stale release made a leased message visible"
    stack.queue.ack(current)
