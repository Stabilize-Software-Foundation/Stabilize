"""An idle processor stretches its poll only while a notify loopback is proven, and falls back when it is not (#116)."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from stabilize import QueueProcessor
from stabilize.queue.messages import StartWorkflow
from stabilize.queue.processor.config import QueueProcessorConfig
from tests.event_stack import Stack, open_stack
from tests.log_capture import captured


@pytest.fixture
def stack(backend: str, tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[Stack]:
    with open_stack(backend, tmp_path, request) as opened:
        yield opened


def _wait(condition: Callable[[], bool], seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.01)
    return condition()


def _message(name: str) -> StartWorkflow:
    return StartWorkflow(execution_type="PIPELINE", execution_id=name)


def _notifying_queue(request: pytest.FixtureRequest, **fields: Any) -> Any:
    from stabilize.queue.postgres import PostgresQueue

    queue = PostgresQueue(request.getfixturevalue("postgres_url"), table_name="queue_messages", **fields)
    request.addfinalizer(queue.close)
    return queue


def _idle_processor(queue: Any, handled: list[tuple[str, int, float]], **config: Any) -> QueueProcessor:
    settings: dict[str, Any] = {
        "enable_deduplication": False,
        "poll_frequency_ms": 50,
        "idle_poll_ms": 1000,
        "wake_ping_seconds": 1.0,
    }
    settings.update(config)
    processor = QueueProcessor(queue, config=QueueProcessorConfig(**settings))
    processor.register_handler_func(
        StartWorkflow, lambda message: handled.append((message.execution_id, message.attempts, time.monotonic()))
    )
    return processor


def _silence_the_process_hub(monkeypatch: pytest.MonkeyPatch) -> None:
    from stabilize.queue import wake

    monkeypatch.setattr(wake, "announce", lambda *args, **kwargs: None)


def _postgres_only(stack: Stack) -> None:
    if stack.backend != "postgres":
        pytest.skip("LISTEN and NOTIFY are PostgreSQL's")


def test_an_idle_processor_with_a_proven_channel_issues_few_statements(
    stack: Stack, request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> None:
    _postgres_only(stack)
    import psycopg

    queue = _notifying_queue(request, wake_notify=True)
    processor = _idle_processor(queue, [])
    processor.start()
    counted: list[int] = []
    real = psycopg.Cursor.execute

    def execute(self: Any, query: Any, *args: Any, **kwargs: Any) -> Any:
        counted.append(1)
        return real(self, query, *args, **kwargs)

    try:
        assert _wait(lambda: processor.wake_channel_proven, 10.0), "the notify loopback was never proven"
        assert _wait(lambda: processor.idle_interval_ms == 1000, 5.0)
        monkeypatch.setattr(psycopg.Cursor, "execute", execute)
        time.sleep(5.0)
        monkeypatch.setattr(psycopg.Cursor, "execute", real)
    finally:
        monkeypatch.setattr(psycopg.Cursor, "execute", real)
        processor.stop()
    per_second = len(counted) / 5.0
    assert per_second <= 6.0, f"an idle processor with a proven channel issued {per_second:.1f} statements a second"


def test_without_idle_poll_ms_the_processor_polls_at_its_poll_frequency(
    stack: Stack, request: pytest.FixtureRequest
) -> None:
    _postgres_only(stack)
    queue = _notifying_queue(request, wake_notify=True)
    processor = _idle_processor(queue, [], idle_poll_ms=None)
    processor.start()
    try:
        time.sleep(1.5)
        assert processor.wake_channel_proven is False
        assert processor.idle_interval_ms == 50
    finally:
        processor.stop()


def test_on_sqlite_the_idle_poll_never_stretches(stack: Stack) -> None:
    if stack.backend != "sqlite":
        pytest.skip("the SQLite queue has no channel across processes")
    with captured() as records:
        processor = _idle_processor(stack.queue, [])
        processor.start()
        try:
            time.sleep(1.5)
            assert processor.wake_channel_proven is False
            assert processor.idle_interval_ms == 50
        finally:
            processor.stop()
    assert any("idle_poll_ms" in r.getMessage() for r in records), "the unusable idle_poll_ms was not reported"


def test_a_notifying_push_from_outside_the_process_hub_wakes_an_idle_processor(
    stack: Stack, request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> None:
    _postgres_only(stack)
    queue = _notifying_queue(request, wake_notify=True)
    handled: list[tuple[str, int, float]] = []
    processor = _idle_processor(queue, handled, idle_poll_ms=10_000)
    processor.start()
    try:
        assert _wait(lambda: processor.wake_channel_proven and processor.idle_interval_ms == 10_000, 10.0)
        _silence_the_process_hub(monkeypatch)
        latencies: list[float] = []
        for index in range(3):
            time.sleep(0.7)
            pushed = time.monotonic()
            queue.push(_message(f"notified-{index}"))
            assert _wait(lambda index=index: len(handled) == index + 1, 15.0), "a notified push was never handled"
            latencies.append(handled[index][2] - pushed)
    finally:
        processor.stop()
    assert max(latencies) < 1.0, (
        f"a notified push waited {max(latencies):.2f} s on a processor idling at 10 s: the notification did not wake it"
    )


def test_a_publisher_that_does_not_notify_trips_the_guard_and_the_stretch_comes_back(
    stack: Stack, request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> None:
    _postgres_only(stack)
    queue = _notifying_queue(request, wake_notify=True)
    silent = _notifying_queue(request)
    handled: list[tuple[str, int, float]] = []
    processor = _idle_processor(queue, handled, wake_guard_hold_seconds=3.0)
    processor.start()
    try:
        assert _wait(lambda: processor.wake_channel_proven and processor.idle_interval_ms == 1000, 10.0)
        _silence_the_process_hub(monkeypatch)
        with captured() as records:
            silent.push(_message("silent-0"))
            time.sleep(0.5)
            silent.push(_message("silent-1"))
            assert _wait(lambda: len(handled) == 2, 10.0), "the timeout poll did not find the unannounced messages"
            assert _wait(lambda: processor.idle_interval_ms == 50, 2.0), (
                "a message sat ready with no notification and the processor kept its stretched idle poll"
            )
        assert any("wake_notify" in r.getMessage() for r in records), "the remedy was not logged"
        assert _wait(lambda: processor.idle_interval_ms == 1000, 10.0), (
            "the stretched idle poll did not come back after the guard's hold"
        )
    finally:
        processor.stop()


def test_a_dead_listener_drops_the_processor_to_its_poll_frequency_and_a_new_one_restores_it(
    stack: Stack, request: pytest.FixtureRequest
) -> None:
    _postgres_only(stack)
    import psycopg

    queue = _notifying_queue(request, wake_notify=True)
    processor = _idle_processor(queue, [])
    processor.start()
    try:
        assert _wait(lambda: processor.wake_channel_proven and processor.idle_interval_ms == 1000, 10.0)
        with psycopg.connect(request.getfixturevalue("postgres_url"), autocommit=True) as conn:
            killed = conn.execute(
                "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity "
                "WHERE datname = current_database() AND application_name LIKE %s",
                ("% wake-listener",),
            ).fetchone()
        assert killed is not None and killed[0] >= 1, "no listener session was found to terminate"
        assert _wait(lambda: processor.idle_interval_ms == 50, 2.0), (
            "the listener was terminated and the processor kept its stretched idle poll for more than 2 s"
        )
        assert _wait(lambda: processor.wake_channel_proven and processor.idle_interval_ms == 1000, 30.0), (
            "the listener did not reconnect and prove the channel again"
        )
    finally:
        processor.stop()


def test_a_proof_that_stops_arriving_ends_the_stretch(
    stack: Stack, request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> None:
    _postgres_only(stack)
    from stabilize.queue.postgres_wake import WakeListener

    queue = _notifying_queue(request, wake_notify=True)
    processor = _idle_processor(queue, [])
    processor.start()
    try:
        assert _wait(lambda: processor.wake_channel_proven and processor.idle_interval_ms == 1000, 10.0)
        monkeypatch.setattr(WakeListener, "_ping", lambda self: None)
        assert _wait(lambda: not processor.wake_channel_proven, 8.0), (
            "no proof arrived for longer than the ping interval plus its grace and the channel still counts as proven"
        )
        assert processor.idle_interval_ms == 50
    finally:
        processor.stop()


def test_a_delayed_message_is_handled_at_its_time_while_the_idle_poll_is_stretched(
    stack: Stack, request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> None:
    _postgres_only(stack)
    queue = _notifying_queue(request, wake_notify=True)
    handled: list[tuple[str, int, float]] = []
    processor = _idle_processor(queue, handled, idle_poll_ms=10_000)
    processor.start()
    try:
        assert _wait(lambda: processor.wake_channel_proven and processor.idle_interval_ms == 10_000, 10.0)
        pushed = time.monotonic()
        queue.push(_message("delayed-in-process"), delay=timedelta(seconds=0.6))
        assert _wait(lambda: len(handled) == 1, 8.0), "a delayed message pushed in this process was not handled"
        in_process = handled[0][2] - pushed
        _silence_the_process_hub(monkeypatch)
        pushed = time.monotonic()
        queue.push(_message("delayed-elsewhere"), delay=timedelta(seconds=0.6))
        assert _wait(lambda: len(handled) == 2, 8.0), "a delayed message announced only by NOTIFY was not handled"
        elsewhere = handled[1][2] - pushed
    finally:
        processor.stop()
    for waited in (in_process, elsewhere):
        assert 0.5 <= waited < 2.0, f"a message delayed 0.6 s was handled after {waited:.2f} s on a 10 s idle poll"


def test_an_expired_lease_is_redelivered_at_its_expiry_while_the_idle_poll_is_stretched(
    stack: Stack, request: pytest.FixtureRequest
) -> None:
    _postgres_only(stack)
    queue = _notifying_queue(request, wake_notify=True, lock_duration=timedelta(seconds=3))
    queue.push(_message("abandoned"))
    abandoned = queue.poll_one()
    assert abandoned is not None and abandoned.attempts == 1
    leased_at = time.monotonic()
    handled: list[tuple[str, int, float]] = []
    processor = _idle_processor(queue, handled, idle_poll_ms=10_000)
    processor.start()
    try:
        assert _wait(lambda: processor.wake_channel_proven and processor.idle_interval_ms == 10_000, 10.0)
        assert _wait(lambda: len(handled) == 1, 8.0), (
            "a message whose lease expired was not redelivered while the processor idled at 10 s"
        )
    finally:
        processor.stop()
    assert handled[0][1] == 2
    assert handled[0][2] - leased_at < 8.0


def test_stop_ends_the_listener(stack: Stack, request: pytest.FixtureRequest) -> None:
    _postgres_only(stack)
    queue = _notifying_queue(request, wake_notify=True)
    processor = _idle_processor(queue, [])
    processor.start()
    assert _wait(lambda: processor.wake_channel_proven, 10.0)
    processor.stop()
    assert _wait(
        lambda: not [t for t in threading.enumerate() if t.name == "stabilize-wake-listener" and t.is_alive()], 5.0
    ), "the wake listener thread outlived stop()"
