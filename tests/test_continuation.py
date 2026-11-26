"""A worker that commits one undelayed successor handles it itself, and hands it back when it cannot (#116)."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from stabilize import Orchestrator, QueueProcessor
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import CompleteWorkflow, StartWorkflow, get_message_type_name
from stabilize.queue.processor.config import QueueProcessorConfig
from stabilize.queue.processor.handler_base import MessageHandler
from tests.event_stack import Stack, one_stage_workflow, open_stack, registry


@pytest.fixture
def stack(backend: str, tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[Stack]:
    with open_stack(backend, tmp_path, request) as opened:
        opened.queue.clear_dlq()
        yield opened
        opened.queue.clear_dlq()


def _wait(condition: Callable[[], bool], seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.005)
    return condition()


def _count_polled(queue: Any) -> list[str]:
    polled: list[str] = []
    guard = threading.Lock()
    name = "poll_many" if callable(getattr(queue, "poll_many", None)) else "poll_one"
    original = getattr(queue, name)

    def poll(*args: Any, **kwargs: Any) -> Any:
        result = original(*args, **kwargs)
        messages = result if isinstance(result, list) else ([result] if result is not None else [])
        with guard:
            polled.extend(get_message_type_name(m) for m in messages)
        return result

    setattr(queue, name, poll)
    return polled


def _record_threads(processor: QueueProcessor) -> list[tuple[str, int]]:
    handled: list[tuple[str, int]] = []
    handle = processor._handle_message

    def recording(message: Any) -> None:
        handled.append((get_message_type_name(message), threading.get_ident()))
        handle(message)

    processor._handle_message = recording  # type: ignore[method-assign]
    return handled


def _run_one_stage(stack: Stack, **config: Any) -> tuple[list[str], list[tuple[str, int]]]:
    workflow = one_stage_workflow()
    stack.store.store(workflow)
    Orchestrator(stack.queue, store=stack.store).start(workflow)
    polled = _count_polled(stack.queue)
    processor = QueueProcessor(
        stack.queue,
        config=QueueProcessorConfig(poll_frequency_ms=20, max_workers=4, **config),
        store=stack.store,
        task_registry=registry(),
    )
    handled = _record_threads(processor)
    processor.start()
    try:
        assert _wait(lambda: stack.store.retrieve(workflow.id).status == WorkflowStatus.SUCCEEDED, 30.0), (
            f"the workflow did not finish; handled {handled}"
        )
    finally:
        processor.stop()
    return polled, handled


def test_a_one_stage_workflow_is_polled_once_and_runs_on_one_worker(stack: Stack) -> None:
    polled, handled = _run_one_stage(stack)

    assert len(handled) == 7, f"expected the 7 messages of a one-stage workflow, handled {handled}"
    assert len(polled) == 1, (
        f"{len(polled)} polls returned a message for one one-stage workflow ({polled}): "
        "each successor waited for a poll"
    )
    assert len({thread for _, thread in handled}) == 1, f"the chain moved between workers: {handled}"


def test_with_no_hops_every_message_is_polled(stack: Stack) -> None:
    polled, handled = _run_one_stage(stack, continuation_hops=0)

    assert len(handled) == 7
    assert len(polled) == 7, f"continuation_hops=0 still continued: only {polled} were polled"


def test_a_chain_yields_after_its_hop_limit(stack: Stack) -> None:
    polled, handled = _run_one_stage(stack, continuation_hops=2)

    assert len(handled) == 7
    assert len(polled) == 3, f"with 2 hops a chain is 3 messages, so 7 messages need 3 polls; polled {polled}"


class _Scripted(MessageHandler[Any]):
    def __init__(self, kind: type[Any], body: Callable[[Any], None]) -> None:
        self._kind = kind
        self._body = body

    @property
    def message_type(self) -> type[Any]:
        return self._kind

    def handle(self, message: Any) -> None:
        self._body(message)


def _successor(execution_id: str, **fields: Any) -> CompleteWorkflow:
    return CompleteWorkflow(execution_type="PIPELINE", execution_id=execution_id, **fields)


def _mark_and_push(stack: Stack, message: Any, *pushes: tuple[Any, float]) -> None:
    with stack.store.transaction(stack.queue) as txn:
        txn.mark_message_processed(
            message_id=message.message_id, handler_type="StartWorkflow", execution_id=message.execution_id
        )
        for successor, delay in pushes:
            txn.push_message(successor, delay)


def _scripted_processor(
    stack: Stack, on_start: Callable[[Any], None], on_complete: Callable[[Any], None]
) -> QueueProcessor:
    processor = QueueProcessor(
        stack.queue,
        config=QueueProcessorConfig(poll_frequency_ms=20, max_workers=3),
        store=stack.store,
    )
    processor.config.retry_delay = timedelta(milliseconds=50)
    processor.register_handler(_Scripted(StartWorkflow, on_start))
    processor.register_handler(_Scripted(CompleteWorkflow, on_complete))
    return processor


def test_a_successor_claimed_by_a_delivery_that_then_fails_is_handed_back(stack: Stack) -> None:
    completed: list[tuple[str, int, float]] = []
    failed_at: list[float] = []

    def on_start(message: Any) -> None:
        _mark_and_push(stack, message, (_successor(message.execution_id), 0))
        failed_at.append(time.monotonic())
        raise RuntimeError("failed after its transaction committed")

    def on_complete(message: Any) -> None:
        completed.append((message.execution_id, message.attempts, time.monotonic()))

    polled = _count_polled(stack.queue)
    processor = _scripted_processor(stack, on_start, on_complete)
    stack.queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="fails-after-commit"))
    processor.start()
    try:
        assert _wait(lambda: bool(completed), 5.0), (
            "the successor of a delivery that failed after its commit was not handled within 5 s: "
            "it was left leased to a worker that gave it up"
        )
    finally:
        processor.stop()
    assert completed[0][1] == 1, f"the handed-back successor was delivered at attempt {completed[0][1]}"
    assert polled == ["StartWorkflow", "CompleteWorkflow"], f"the successor was not taken by a poll: {polled}"
    assert len(completed) == 1


def test_a_successor_claimed_before_a_stop_is_handed_back_and_not_handled(stack: Stack) -> None:
    completed: list[str] = []
    box: dict[str, QueueProcessor] = {}

    def on_start(message: Any) -> None:
        _mark_and_push(stack, message, (_successor(message.execution_id), 0))
        box["processor"].request_stop()

    processor = _scripted_processor(stack, on_start, lambda message: completed.append(message.execution_id))
    box["processor"] = processor
    stack.queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="stops-mid-chain"))
    processor.start()
    try:
        assert _wait(
            lambda: stack.queue.has_pending_message_for_execution("stops-mid-chain") and processor.active_count == 0,
            5.0,
        )
        time.sleep(0.3)
    finally:
        processor.stop()
    assert not completed, "a claimed successor was handled after stop was requested"
    again = stack.queue.poll_one()
    assert again is not None, "the claimed successor is not visible after the stop: it was not handed back"
    assert again.attempts == 1, f"the handed-back successor is at attempt {again.attempts}"
    stack.queue.ack(again)


def test_a_delayed_successor_is_never_claimed(stack: Stack) -> None:
    completed: list[float] = []
    pushed: list[float] = []

    def on_start(message: Any) -> None:
        _mark_and_push(stack, message, (_successor(message.execution_id), 0.4))
        pushed.append(time.monotonic())

    polled = _count_polled(stack.queue)
    processor = _scripted_processor(stack, on_start, lambda message: completed.append(time.monotonic()))
    stack.queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="delayed"))
    processor.start()
    try:
        assert _wait(lambda: bool(completed), 10.0), "the delayed successor was never handled"
    finally:
        processor.stop()
    assert completed[0] - pushed[0] >= 0.3, (
        f"a successor pushed with a 0.4 s delay was handled after {completed[0] - pushed[0]:.2f} s"
    )
    assert polled == ["StartWorkflow", "CompleteWorkflow"]


def test_of_a_fan_out_one_successor_is_continued_and_the_others_are_polled(stack: Stack) -> None:
    completed: list[str] = []
    guard = threading.Lock()

    def on_start(message: Any) -> None:
        _mark_and_push(stack, message, *[(_successor(f"fan-{i}"), 0) for i in range(3)])

    def on_complete(message: Any) -> None:
        with guard:
            completed.append(message.execution_id)

    polled = _count_polled(stack.queue)
    processor = _scripted_processor(stack, on_start, on_complete)
    stack.queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="fan"))
    processor.start()
    try:
        assert _wait(lambda: len(completed) >= 3, 10.0), f"only {completed} of 3 successors were handled"
        time.sleep(0.2)
    finally:
        processor.stop()
    assert sorted(completed) == ["fan-0", "fan-1", "fan-2"], f"a successor was handled twice or lost: {completed}"
    assert sorted(polled) == ["CompleteWorkflow", "CompleteWorkflow", "StartWorkflow"], (
        f"of 3 successors exactly 1 is continued and 2 are polled; polled {polled}"
    )


def test_a_successor_that_does_not_decode_is_pushed_unclaimed_and_dead_lettered_by_the_poll(stack: Stack) -> None:
    completed: list[str] = []
    started: list[str] = []

    def on_start(message: Any) -> None:
        _mark_and_push(stack, message, (_successor(message.execution_id, retry_count="not-a-number"), 0))
        started.append(message.execution_id)

    processor = _scripted_processor(stack, on_start, lambda message: completed.append(message.execution_id))
    stack.queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="undecodable"))
    processor.start()
    try:
        assert _wait(lambda: stack.queue.dlq_size() == 1, 10.0), (
            "a successor whose payload does not decode never reached the dead-letter table"
        )
    finally:
        processor.stop()
    assert started == ["undecodable"]
    assert not completed


def test_process_one_handles_exactly_one_message(stack: Stack) -> None:
    workflow = one_stage_workflow()
    stack.store.store(workflow)
    Orchestrator(stack.queue, store=stack.store).start(workflow)
    processor = QueueProcessor(stack.queue, store=stack.store, task_registry=registry())
    handled = _record_threads(processor)

    assert processor.process_one() is True

    assert [kind for kind, _ in handled] == ["StartWorkflow"]
    successor = stack.queue.poll_one()
    assert successor is not None and successor.attempts == 1, "the synchronous path left its successor leased"
    stack.queue.ack(successor)
