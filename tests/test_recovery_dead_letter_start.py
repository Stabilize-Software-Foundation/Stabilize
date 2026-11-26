"""Recovery leaves a never-started workflow alone while its StartWorkflow sits in the dead-letter table (#116)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from stabilize import Orchestrator, QueueProcessor
from stabilize.models.status import WorkflowStatus
from stabilize.recovery import WorkflowRecovery
from tests.event_stack import Stack, one_stage_workflow, open_stack, registry


@pytest.fixture
def stack(backend: str, tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[Stack]:
    with open_stack(backend, tmp_path, request) as opened:
        opened.queue.clear_dlq()
        yield opened
        opened.queue.clear_dlq()


def _dead_lettered_start(stack: Stack) -> str:
    workflow = one_stage_workflow()
    stack.store.store(workflow)
    Orchestrator(stack.queue, store=stack.store).start(workflow)
    message = stack.queue.poll_one()
    assert message is not None and message.message_id
    stack.queue.move_to_dlq(message.message_id, error="given_up")
    assert stack.queue.dlq_size() == 1
    assert not stack.queue.has_pending_message_for_execution(workflow.id)
    return str(workflow.id)


def test_recovery_does_not_queue_a_start_that_was_dead_lettered(stack: Stack) -> None:
    workflow_id = _dead_lettered_start(stack)

    results = WorkflowRecovery(stack.store, stack.queue).recover_pending_workflows()

    assert not stack.queue.has_pending_message_for_execution(workflow_id), (
        "recovery queued StartWorkflow again although its message sits in the dead-letter table"
    )
    assert [r.status for r in results if r.workflow_id == workflow_id] == ["dead_lettered"]
    assert stack.store.retrieve(workflow_id).status == WorkflowStatus.NOT_STARTED
    assert stack.queue.dlq_size() == 1


def test_a_replayed_start_runs_the_workflow(stack: Stack) -> None:
    workflow_id = _dead_lettered_start(stack)
    entry = stack.queue.list_dlq()[0]

    assert stack.queue.replay_dlq(entry["id"]) is True
    QueueProcessor(stack.queue, store=stack.store, task_registry=registry()).process_all(timeout=30.0)

    assert stack.store.retrieve(workflow_id).status == WorkflowStatus.SUCCEEDED
    assert stack.queue.dlq_size() == 0


def test_recovery_still_starts_a_workflow_that_has_no_message_anywhere(stack: Stack) -> None:
    workflow = one_stage_workflow()
    stack.store.store(workflow)

    results = WorkflowRecovery(stack.store, stack.queue).recover_pending_workflows()

    assert stack.queue.has_pending_message_for_execution(workflow.id)
    assert [r.status for r in results if r.workflow_id == workflow.id] == ["recovered"]
