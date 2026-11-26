"""A processor with a worker id releases its predecessor's leases at start, unless another holder is live (#116)."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator
from datetime import timedelta

import pytest

pytest.importorskip("psycopg")

from stabilize import QueueProcessor  # noqa: E402
from stabilize.persistence.pool_options import PoolOptions  # noqa: E402
from stabilize.persistence.tenancy import EngineSchemaError  # noqa: E402
from stabilize.queue.messages import StartWorkflow  # noqa: E402
from stabilize.queue.postgres import PostgresQueue  # noqa: E402
from stabilize.queue.processor.config import QueueProcessorConfig  # noqa: E402

LEASE = timedelta(seconds=10)


def _wait(condition: Callable[[], bool], seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.01)
    return condition()


def _message(name: str) -> StartWorkflow:
    return StartWorkflow(execution_type="PIPELINE", execution_id=name)


@pytest.fixture
def queues(postgres_url: str) -> Iterator[Callable[..., PostgresQueue]]:
    opened: list[PostgresQueue] = []

    def open_queue(worker_id: str | None = None, table_name: str = "queue_messages") -> PostgresQueue:
        options = PoolOptions(worker_id=worker_id) if worker_id else None
        queue = PostgresQueue(postgres_url, table_name=table_name, lock_duration=LEASE, options=options)
        opened.append(queue)
        return queue

    first = open_queue()
    first.clear()
    yield open_queue
    first.clear()
    for queue in opened:
        queue.close()


def _processor(queue: PostgresQueue, handled: list[tuple[str, int]]) -> QueueProcessor:
    processor = QueueProcessor(queue, config=QueueProcessorConfig(enable_deduplication=False, poll_frequency_ms=20))
    processor.register_handler_func(
        StartWorkflow, lambda message: handled.append((message.execution_id, message.attempts))
    )
    return processor


def _lease(queue: PostgresQueue, name: str) -> None:
    queue.push(_message(name))
    leased = queue.poll_one()
    assert leased is not None and leased.attempts == 1 and leased.execution_id == name


def test_a_restarted_worker_releases_what_its_predecessor_held(queues: Callable[..., PostgresQueue]) -> None:
    queue = queues("w1")
    _lease(queue, "held-by-w1")
    handled: list[tuple[str, int]] = []
    processor = _processor(queue, handled)
    processor.start()
    try:
        assert _wait(lambda: bool(handled), 3.0), (
            "a message leased under this worker id was not redelivered within 3 s of start(): "
            "it waits for its 10 s lease"
        )
    finally:
        processor.stop()
    assert handled == [("held-by-w1", 2)]
    assert processor.last_reclaim.released == 1


def test_without_a_worker_id_the_lease_runs_out_as_before(queues: Callable[..., PostgresQueue]) -> None:
    queue = queues()
    _lease(queue, "held-by-nobody")
    handled: list[tuple[str, int]] = []
    processor = _processor(queue, handled)
    processor.start()
    try:
        time.sleep(2.0)
    finally:
        processor.stop()
    assert not handled, "a processor with no worker id released a lease it could not know was abandoned"
    assert processor.last_reclaim.released == 0


def test_only_the_leases_of_the_same_worker_id_are_released(queues: Callable[..., PostgresQueue]) -> None:
    other = queues("w2")
    _lease(other, "held-by-w2")
    queue = queues("w1")
    _lease(queue, "held-by-w1")
    handled: list[tuple[str, int]] = []
    processor = _processor(queue, handled)
    processor.start()
    try:
        assert _wait(lambda: bool(handled), 3.0)
        time.sleep(1.0)
    finally:
        processor.stop()
    assert handled == [("held-by-w1", 2)], f"a lease of another worker id was released: {handled}"


def test_a_live_session_of_another_incarnation_blocks_the_release(
    queues: Callable[..., PostgresQueue], postgres_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    import psycopg

    queue = queues("w1")
    _lease(queue, "held-by-a-live-twin")
    handled: list[tuple[str, int]] = []
    processor = _processor(queue, handled)
    with psycopg.connect(postgres_url, application_name="stabilize/0.0.0 w=w1 i=feedfacecafe") as twin:
        twin.execute("SELECT 1")
        with caplog.at_level(logging.WARNING):
            processor.start()
            try:
                time.sleep(2.0)
            finally:
                processor.stop()
    assert not handled, "the processor released leases while another incarnation of its worker id had a session"
    assert processor.last_reclaim.released == 0
    assert "another incarnation" in processor.last_reclaim.reason
    assert any("worker id" in r.getMessage() for r in caplog.records), "the withheld release was not logged"


def test_a_session_of_another_worker_id_does_not_block_the_release(
    queues: Callable[..., PostgresQueue], postgres_url: str
) -> None:
    import psycopg

    queue = queues("w1")
    _lease(queue, "held-by-w1")
    handled: list[tuple[str, int]] = []
    processor = _processor(queue, handled)
    with psycopg.connect(postgres_url, application_name="stabilize/0.0.0 w=w10 i=feedfacecafe") as other:
        other.execute("SELECT 1")
        processor.start()
        try:
            assert _wait(lambda: bool(handled), 3.0), "a session of worker id w10 blocked the release for w1"
        finally:
            processor.stop()


def test_a_check_that_cannot_see_its_own_session_releases_nothing(
    queues: Callable[..., PostgresQueue], monkeypatch: pytest.MonkeyPatch
) -> None:
    from stabilize.queue.processor import worker_identity

    queue = queues("w1")
    _lease(queue, "held-while-blind")
    monkeypatch.setattr(worker_identity, "engine_sessions", lambda conn: [])
    handled: list[tuple[str, int]] = []
    processor = _processor(queue, handled)
    processor.start()
    try:
        time.sleep(2.0)
    finally:
        processor.stop()
    assert not handled, "leases were released by a check that could not see its own session"
    assert "own session" in processor.last_reclaim.reason


def test_a_queue_table_without_locked_by_is_refused_with_the_remedy(
    queues: Callable[..., PostgresQueue], postgres_url: str
) -> None:
    import psycopg

    with psycopg.connect(postgres_url, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS queue_noid, queue_noid_dlq")
        conn.execute("CREATE TABLE queue_noid (LIKE queue_messages INCLUDING ALL)")
        conn.execute("ALTER TABLE queue_noid DROP COLUMN locked_by")
        conn.execute("CREATE TABLE queue_noid_dlq (LIKE queue_messages_dlq INCLUDING ALL)")
    try:
        queue = queues("w1", table_name="queue_noid")
        processor = _processor(queue, [])
        with pytest.raises(EngineSchemaError, match=r"locked_by.*mg-up"):
            processor.start()
        assert processor.is_running is False
        plain = queues(table_name="queue_noid")
        plain.push(_message("no-id-needed"))
        polled = plain.poll_one()
        assert polled is not None, "a queue table without locked_by stopped working for a host with no worker id"
        plain.ack(polled)
    finally:
        with psycopg.connect(postgres_url, autocommit=True) as conn:
            conn.execute("DROP TABLE IF EXISTS queue_noid, queue_noid_dlq")


@pytest.mark.parametrize("worker_id", ["", "has space", "x" * 25, "under_score"])
def test_a_malformed_worker_id_is_refused(worker_id: str) -> None:
    if worker_id == "":
        assert PoolOptions(worker_id=None).worker_id is None
        return
    with pytest.raises(ValueError, match="worker_id"):
        PoolOptions(worker_id=worker_id)


def test_a_worker_id_with_a_host_application_name_is_refused(postgres_url: str) -> None:
    options = PoolOptions(worker_id="w1", connect_kwargs={"application_name": "host-app"})
    with pytest.raises(ValueError, match="application_name"):
        PostgresQueue(postgres_url, table_name="queue_messages", options=options)
