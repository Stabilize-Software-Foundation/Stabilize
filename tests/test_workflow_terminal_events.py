"""A workflow that ends or reopens outside CompleteWorkflow leaves the matching event in the log (#111)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from stabilize.events import EventType
from stabilize.events.replay import EventReplayer
from stabilize.handlers import RestartStageHandler, StartWaitingWorkflowsHandler, StartWorkflowHandler
from stabilize.models.status import WorkflowStatus
from stabilize.models.workflow import Workflow
from stabilize.queue.messages import RestartStage, StartWaitingWorkflows, StartWorkflow
from tests.event_stack import Stack, events_of, one_stage_workflow, open_stack, registry, workflow_status_changes


@pytest.fixture
def stack(backend: str, tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[Stack]:
    with open_stack(backend, tmp_path, request) as opened:
        yield opened


def _start(stack: Stack, workflow: Workflow) -> None:
    StartWorkflowHandler(stack.queue, stack.store, event_recorder=stack.recorder).handle(
        StartWorkflow(execution_type="PIPELINE", execution_id=workflow.id, message_id=f"end-sw-{workflow.id}")
    )


def test_a_workflow_canceled_before_it_starts_records_its_cancellation(stack: Stack) -> None:
    workflow = one_stage_workflow()
    stack.store.store(workflow)
    stack.store.cancel(workflow.id, "ops", "no longer needed")

    _start(stack, workflow)

    assert stack.store.retrieve(workflow.id).status == WorkflowStatus.CANCELED
    canceled = events_of(stack, workflow.id, EventType.WORKFLOW_CANCELED)
    assert len(canceled) == 1, f"{len(canceled)} workflow.canceled events for a workflow canceled before its start"


def test_a_workflow_without_initial_stages_records_its_failure(stack: Stack) -> None:
    workflow = Workflow.create(application="end", name="empty", stages=[])
    stack.store.store(workflow)

    _start(stack, workflow)

    assert stack.store.retrieve(workflow.id).status == WorkflowStatus.TERMINAL
    failed = events_of(stack, workflow.id, EventType.WORKFLOW_FAILED)
    assert len(failed) == 1, f"{len(failed)} workflow.failed events for a workflow that ended TERMINAL at its start"


def _waiting(config_id: str, **fields: object) -> Workflow:
    workflow = one_stage_workflow(is_limit_concurrent=True, max_concurrent_executions=1, **fields)
    workflow.pipeline_config_id = config_id
    workflow.status = WorkflowStatus.BUFFERED
    return workflow


def _promote(stack: Stack, config_id: str, purge: bool) -> None:
    StartWaitingWorkflowsHandler(stack.queue, stack.store, event_recorder=stack.recorder).handle(
        StartWaitingWorkflows(pipeline_config_id=config_id, purge_queue=purge, message_id=f"end-sww-{config_id}")
    )


def test_a_waiting_workflow_purged_from_the_queue_records_its_cancellation(stack: Stack) -> None:
    running = one_stage_workflow(is_limit_concurrent=True, max_concurrent_executions=1)
    config_id = f"cfg-{running.id}"
    running.pipeline_config_id = config_id
    running.status = WorkflowStatus.RUNNING
    stack.store.store(running)
    waiting = _waiting(config_id)
    stack.store.store(waiting)

    _promote(stack, config_id, purge=True)

    assert stack.store.retrieve(waiting.id).status == WorkflowStatus.CANCELED
    canceled = events_of(stack, waiting.id, EventType.WORKFLOW_CANCELED)
    assert len(canceled) == 1, f"{len(canceled)} workflow.canceled events for a workflow purged while waiting"
    assert (canceled[0].data["canceled_by"], canceled[0].data["reason"]) == (
        "system",
        "Queue purged by keepWaitingPipelines=False policy",
    )


def test_a_waiting_workflow_past_its_start_expiry_records_its_cancellation(stack: Stack) -> None:
    waiting = _waiting("pending", start_time_expiry=1)
    waiting.pipeline_config_id = f"cfg-{waiting.id}"
    stack.store.store(waiting)

    _promote(stack, waiting.pipeline_config_id, purge=False)

    assert stack.store.retrieve(waiting.id).status == WorkflowStatus.CANCELED
    canceled = events_of(stack, waiting.id, EventType.WORKFLOW_CANCELED)
    assert len(canceled) == 1, f"{len(canceled)} workflow.canceled events for a workflow that expired while waiting"
    assert (canceled[0].data["canceled_by"], canceled[0].data["reason"]) == (
        "system",
        "Could not begin execution before start time expiry",
    )
    assert stack.store.retrieve(waiting.id).cancellation_reason == "Could not begin execution before start time expiry"


def test_a_waiting_workflow_a_user_canceled_keeps_who_canceled_it(stack: Stack) -> None:
    waiting = _waiting("pending")
    waiting.pipeline_config_id = f"cfg-{waiting.id}"
    stack.store.store(waiting)
    stack.store.cancel(waiting.id, "alice", "changed my mind")

    _promote(stack, waiting.pipeline_config_id, purge=True)

    stored = stack.store.retrieve(waiting.id)
    assert stored.status == WorkflowStatus.CANCELED
    assert (stored.canceled_by, stored.cancellation_reason) == ("alice", "changed my mind"), (
        f"the cancellation is stored as by {stored.canceled_by!r}: {stored.cancellation_reason!r}"
    )
    canceled = events_of(stack, waiting.id, EventType.WORKFLOW_CANCELED)
    assert [(e.data["canceled_by"], e.data["reason"]) for e in canceled] == [("alice", "changed my mind")]


def test_a_restarted_workflow_is_running_in_the_log_and_in_replay(stack: Stack) -> None:
    workflow = one_stage_workflow()
    workflow.status = WorkflowStatus.SUCCEEDED
    workflow.start_time, workflow.end_time = 1, 2
    stage = workflow.stages[0]
    stage.status = WorkflowStatus.SUCCEEDED
    stage.tasks[0].status = WorkflowStatus.SUCCEEDED
    stack.store.store(workflow)
    stack.recorder.record_workflow_completed(workflow, source_handler="test")

    RestartStageHandler(stack.queue, stack.store, event_recorder=stack.recorder, task_registry=registry()).handle(
        RestartStage(
            execution_type="PIPELINE", execution_id=workflow.id, stage_id=stage.id, message_id=f"end-rs-{stage.id}"
        )
    )

    assert stack.store.retrieve(workflow.id).status == WorkflowStatus.RUNNING
    changes = workflow_status_changes(stack, workflow.id)
    assert changes == [("SUCCEEDED", "RUNNING")], f"the reopening of the workflow was recorded as {changes}"
    replayed = EventReplayer(stack.event_store).rebuild_workflow_state(workflow.id)
    assert replayed["status"] == "RUNNING", f"replay reads the restarted workflow as {replayed['status']}"
