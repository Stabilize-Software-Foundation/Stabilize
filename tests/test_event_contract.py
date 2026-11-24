"""An engine event appended while a message is handled, outside any store transaction, is a delivery defect (#111)."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from stabilize.events.base import EntityType
from stabilize.models.status import WorkflowStatus
from stabilize.models.workflow import Workflow
from stabilize.persistence.committed_marks import recent_defects
from stabilize.queue.messages import StartWorkflow
from stabilize.queue.processor import QueueProcessor
from stabilize.queue.processor.config import QueueProcessorConfig
from stabilize.queue.processor.handler_base import MessageHandler
from stabilize.streaming import emit_progress
from tests.event_stack import Stack, one_stage_workflow, open_stack

OUTSIDE = "workflow.started was recorded outside any store transaction"
DROPPED = "1 held event(s) never reached a store transaction"


@pytest.fixture
def stack(backend: str, tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[Stack]:
    with open_stack(backend, tmp_path, request) as opened:
        yield opened


class _Handler(MessageHandler[StartWorkflow]):
    def __init__(self, body: Callable[[StartWorkflow], None]) -> None:
        self._body = body

    @property
    def message_type(self) -> type[StartWorkflow]:
        return StartWorkflow

    def handle(self, message: StartWorkflow) -> None:
        self._body(message)


def _deliver(stack: Stack, workflow: Workflow, body: Callable[[StartWorkflow], None]) -> Any:
    processor = QueueProcessor(stack.queue, config=QueueProcessorConfig(enable_deduplication=False))
    processor.register_handler(_Handler(body))
    stack.queue.push(StartWorkflow(execution_type="PIPELINE", execution_id=workflow.id))
    assert processor.process_one() is True
    return processor


def _stored(stack: Stack) -> Workflow:
    workflow = one_stage_workflow()
    workflow.status = WorkflowStatus.RUNNING
    stack.store.store(workflow)
    return workflow


def _reasons(processor: Any) -> list[str]:
    return [reason for defect in processor.delivery_defects for reason in defect.reasons]


@pytest.mark.delivery_defects
def test_an_event_recorded_outside_a_transaction_is_reported(stack: Stack) -> None:
    workflow = _stored(stack)

    processor = _deliver(
        stack, workflow, lambda _m: stack.recorder.record_workflow_started(workflow, source_handler="Bare")
    )

    assert _reasons(processor) == [OUTSIDE]
    assert [r for d in recent_defects() for r in d.reasons] == [OUTSIDE]


def test_an_event_recorded_inside_a_transaction_is_not_reported(stack: Stack) -> None:
    workflow = _stored(stack)

    def body(message: StartWorkflow) -> None:
        with stack.store.transaction(stack.queue) as txn:
            txn.mark_message_processed(message_id=message.message_id, handler_type="Inside", execution_id=workflow.id)
            stack.recorder.record_workflow_started(workflow, source_handler="Inside")

    processor = _deliver(stack, workflow, body)

    assert _reasons(processor) == []
    assert recent_defects() == []


def test_a_task_progress_event_is_not_reported(stack: Stack) -> None:
    workflow = _stored(stack)

    processor = _deliver(
        stack, workflow, lambda _m: emit_progress(workflow.stages[0], "half way", recorder=stack.recorder)
    )

    assert _reasons(processor) == []


@pytest.mark.delivery_defects
def test_a_held_event_that_reaches_no_transaction_is_reported(stack: Stack) -> None:
    workflow = _stored(stack)

    def body(_message: StartWorkflow) -> None:
        stack.recorder.record_status_change(
            EntityType.WORKFLOW,
            workflow.id,
            workflow.id,
            WorkflowStatus.RUNNING,
            WorkflowStatus.SUCCEEDED,
            version=1,
            source_handler="Held",
            held=True,
        )

    processor = _deliver(stack, workflow, body)

    assert _reasons(processor) == [DROPPED]


def test_recording_outside_any_delivery_is_not_reported(stack: Stack) -> None:
    workflow = _stored(stack)

    stack.recorder.record_workflow_started(workflow, source_handler="NoDelivery")

    assert recent_defects() == []
