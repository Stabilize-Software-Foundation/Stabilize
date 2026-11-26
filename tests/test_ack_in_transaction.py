"""The transaction that marks a message removes its queue row, and a first delivery is not looked up (#116)."""

from __future__ import annotations

import time
from collections.abc import Iterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from stabilize import Orchestrator, QueueProcessor
from stabilize.events import EventType
from stabilize.models.status import WorkflowStatus
from stabilize.queue.messages import StartWorkflow, get_message_type_name
from stabilize.queue.processor.config import QueueProcessorConfig
from tests.event_stack import Stack, events_of, one_stage_workflow, open_stack, registry


@pytest.fixture
def stack(backend: str, tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[Stack]:
    with open_stack(backend, tmp_path, request) as opened:
        yield opened


def _processor(stack: Stack) -> QueueProcessor:
    return QueueProcessor(stack.queue, store=stack.store, task_registry=registry())


def _started(stack: Stack) -> str:
    workflow = one_stage_workflow()
    stack.store.store(workflow)
    Orchestrator(stack.queue, store=stack.store).start(workflow)
    return str(workflow.id)


def test_the_queue_row_is_gone_when_the_handler_returns(stack: Stack) -> None:
    workflow_id = _started(stack)
    processor = _processor(stack)
    outlived: list[str] = []
    seen: list[str] = []
    handle = processor._handle_message

    def handle_then_look(message: Any) -> None:
        handle(message)
        seen.append(get_message_type_name(message))
        if stack.queue.live_delivery_keys([message.message_id]):
            outlived.append(get_message_type_name(message))

    processor._handle_message = handle_then_look  # type: ignore[method-assign]
    processor.process_all(timeout=30.0)

    assert stack.store.retrieve(workflow_id).status == WorkflowStatus.SUCCEEDED
    assert len(seen) == 7, f"expected the 7 messages of a one-stage workflow, handled {seen}"
    assert not outlived, (
        f"the queue row outlived the transaction that marked it for {outlived}: "
        "the acknowledgement is a transaction of its own"
    )
    assert stack.queue.poll_one() is None


def test_a_first_delivery_is_not_looked_up_in_processed_messages(stack: Stack) -> None:
    workflow_id = _started(stack)
    processor = _processor(stack)
    lookups: list[str] = []
    is_processed = stack.store.is_message_processed

    def counted(message_id: str) -> bool:
        lookups.append(message_id)
        return bool(is_processed(message_id))

    stack.store.is_message_processed = counted
    processor.process_all(timeout=30.0)

    assert stack.store.retrieve(workflow_id).status == WorkflowStatus.SUCCEEDED
    assert not lookups, (
        f"{len(lookups)} durable duplicate check(s) on a first delivery: a row polled once cannot have been marked"
    )


def test_a_lease_taken_by_another_delivery_is_left_to_it_and_the_work_happens_once(
    backend: str, tmp_path: Path, request: pytest.FixtureRequest
) -> None:
    with open_stack(backend, tmp_path, request) as stack:
        queue = _short_lease_queue(stack, request)
        try:
            workflow = one_stage_workflow()
            stack.store.store(workflow)
            Orchestrator(queue, store=stack.store).start(workflow)
            processor = QueueProcessor(queue, store=stack.store, task_registry=registry())
            stale = queue.poll_one()
            assert isinstance(stale, StartWorkflow) and stale.attempts == 1
            time.sleep(0.3)
            current = queue.poll_one()
            assert isinstance(current, StartWorkflow) and current.attempts == 2
            assert current.message_id == stale.message_id
            lookups: list[str] = []
            is_processed = stack.store.is_message_processed

            def counted(message_id: str) -> bool:
                lookups.append(message_id)
                return bool(is_processed(message_id))

            stack.store.is_message_processed = counted

            processor._handle_message(stale)

            assert queue.live_delivery_keys([current.message_id]), (
                "the stale delivery deleted a row whose lease another delivery holds"
            )

            processor._handle_message(current)
            queue.ack(current)

            assert lookups == [current.message_id], "a redelivery was not checked against processed_messages"
            started = events_of(stack, workflow.id, EventType.WORKFLOW_STARTED)
            assert len(started) == 1, f"{len(started)} workflow.started events for one workflow"
            assert not queue.live_delivery_keys([current.message_id])
        finally:
            queue.clear()
            if queue is not stack.queue:
                queue.close()


def _short_lease_queue(stack: Stack, request: pytest.FixtureRequest) -> Any:
    lease = timedelta(milliseconds=100)
    if stack.backend == "sqlite":
        from stabilize.queue.sqlite import SqliteQueue

        return SqliteQueue(stack.queue.connection_string, lock_duration=lease)
    from stabilize.queue.postgres import PostgresQueue

    return PostgresQueue(request.getfixturevalue("postgres_url"), table_name="queue_messages", lock_duration=lease)


def test_a_handler_that_marks_nothing_is_still_acknowledged(stack: Stack) -> None:
    handled: list[str] = []
    processor = QueueProcessor(stack.queue, config=QueueProcessorConfig(enable_deduplication=False))
    processor.register_handler_func(StartWorkflow, lambda message: handled.append(message.execution_id))
    stack.queue.push(StartWorkflow(execution_type="PIPELINE", execution_id="unmarked"))

    assert processor.process_one() is True

    assert handled == ["unmarked"]
    assert stack.queue.poll_one() is None


def test_a_handler_called_with_no_delivery_bound_leaves_the_row_to_the_caller(stack: Stack) -> None:
    from stabilize.handlers import StartWorkflowHandler

    workflow = one_stage_workflow()
    stack.store.store(workflow)
    Orchestrator(stack.queue, store=stack.store).start(workflow)
    message = stack.queue.poll_one()
    assert isinstance(message, StartWorkflow)

    StartWorkflowHandler(stack.queue, stack.store, event_recorder=stack.recorder).handle(message)

    assert stack.queue.live_delivery_keys([message.message_id]), (
        "a handler called outside a processor delivery deleted the queue row its caller still holds"
    )
    stack.queue.ack(message)
