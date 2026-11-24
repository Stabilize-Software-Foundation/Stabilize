"""A workflow's status is written against the status the writer read; two writers at once make one transition (#111)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from stabilize.errors import ConcurrencyError
from stabilize.events import EventType
from stabilize.handlers import CompleteWorkflowHandler, ResumeStageHandler, StartWorkflowHandler
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.queue.messages import CompleteWorkflow, ResumeStage, StartStage, StartWaitingWorkflows, StartWorkflow
from tests.event_stack import (
    Stack,
    drain,
    events_of,
    one_stage_workflow,
    open_stack,
    read_together,
    run_together,
    set_statuses,
    workflow_status_changes,
)


@pytest.fixture
def stack(backend: str, tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[Stack]:
    with open_stack(backend, tmp_path, request) as opened:
        yield opened


def _complete(stack: Stack, workflow_id: str, n: int) -> None:
    CompleteWorkflowHandler(stack.queue, stack.store, event_recorder=stack.recorder).handle(
        CompleteWorkflow(execution_type="PIPELINE", execution_id=workflow_id, message_id=f"guard-cw-{workflow_id}-{n}")
    )


@pytest.mark.parametrize("handlers", [2, 4])
def test_handlers_completing_one_workflow_at_once_complete_it_once(stack: Stack, handlers: int) -> None:
    workflow = one_stage_workflow()
    workflow.pipeline_config_id = f"cfg-{workflow.id}"
    set_statuses(workflow, WorkflowStatus.RUNNING, WorkflowStatus.SUCCEEDED)
    stack.store.store(workflow)
    read_together(stack.store, handlers)

    outcomes = run_together([lambda n=n: _complete(stack, workflow.id, n) for n in range(handlers)])

    assert outcomes == [None] * handlers, f"a handler raised: {outcomes}"
    assert stack.store.retrieve(workflow.id).status == WorkflowStatus.SUCCEEDED
    completed = events_of(stack, workflow.id, EventType.WORKFLOW_COMPLETED)
    assert len(completed) == 1, f"{len(completed)} workflow.completed events for one workflow"
    changes = workflow_status_changes(stack, workflow.id)
    assert changes == [("RUNNING", "SUCCEEDED")], f"the workflow's completion was recorded as {changes}"
    waiting = [m for m in drain(stack.queue) if isinstance(m, StartWaitingWorkflows)]
    assert len(waiting) == 1, f"{len(waiting)} StartWaitingWorkflows messages for one completion"


def _start(stack: Stack, workflow_id: str, n: int) -> None:
    StartWorkflowHandler(stack.queue, stack.store, event_recorder=stack.recorder).handle(
        StartWorkflow(execution_type="PIPELINE", execution_id=workflow_id, message_id=f"guard-sw-{workflow_id}-{n}")
    )


def test_two_deliveries_starting_one_workflow_at_once_start_it_once(stack: Stack) -> None:
    workflow = one_stage_workflow()
    stack.store.store(workflow)
    read_together(stack.store, 2)

    outcomes = run_together([lambda n=n: _start(stack, workflow.id, n) for n in range(2)])

    assert outcomes == [None, None], f"a handler raised: {outcomes}"
    assert stack.store.retrieve(workflow.id).status == WorkflowStatus.RUNNING
    started = events_of(stack, workflow.id, EventType.WORKFLOW_STARTED)
    assert len(started) == 1, f"{len(started)} workflow.started events for one workflow"
    stages = [m for m in drain(stack.queue) if isinstance(m, StartStage)]
    assert len(stages) == 1, f"{len(stages)} StartStage messages for one initial stage"


def _paused_stage(ref_id: str) -> StageExecution:
    task = TaskExecution.create(name="t", implementing_class="success", stage_start=True, stage_end=True)
    task.status = WorkflowStatus.PAUSED
    stage = StageExecution(ref_id=ref_id, type="work", name=ref_id.upper(), tasks=[task])
    stage.status = WorkflowStatus.PAUSED
    return stage


def test_two_stages_resuming_at_once_resume_their_workflow_once(stack: Stack) -> None:
    workflow = Workflow.create(application="guard", name="w", stages=[_paused_stage("a"), _paused_stage("b")])
    workflow.status = WorkflowStatus.RUNNING
    workflow.start_time = 1
    stack.store.store(workflow)
    stack.store.pause(workflow.id, "ops")
    assert stack.store.retrieve(workflow.id).status == WorkflowStatus.PAUSED
    read_together(stack.store, 2, method="retrieve_stage")

    def resume(stage_id: str, n: int) -> None:
        ResumeStageHandler(stack.queue, stack.store, event_recorder=stack.recorder).handle(
            ResumeStage(
                execution_type="PIPELINE",
                execution_id=workflow.id,
                stage_id=stage_id,
                message_id=f"guard-rs-{workflow.id}-{n}",
            )
        )

    outcomes = run_together([lambda s=stage.id, n=n: resume(s, n) for n, stage in enumerate(workflow.stages)])

    assert outcomes == [None, None], f"a handler raised: {outcomes}"
    fresh = stack.store.retrieve(workflow.id)
    assert fresh.status == WorkflowStatus.RUNNING
    assert [s.status for s in fresh.stages] == [WorkflowStatus.RUNNING, WorkflowStatus.RUNNING]
    resumed = events_of(stack, workflow.id, EventType.WORKFLOW_RESUMED)
    assert len(resumed) == 1, f"{len(resumed)} workflow.resumed events for one resume"


def test_a_status_write_against_a_status_the_row_no_longer_holds_changes_nothing(stack: Stack) -> None:
    workflow = one_stage_workflow()
    set_statuses(workflow, WorkflowStatus.RUNNING, WorkflowStatus.SUCCEEDED)
    stack.store.store(workflow)

    workflow.status = WorkflowStatus.SUCCEEDED
    workflow.end_time = 5
    with pytest.raises(ConcurrencyError):
        with stack.store.transaction(stack.queue) as txn:
            txn.update_workflow_status(workflow, expected_status="PAUSED")
    fresh = stack.store.retrieve(workflow.id)
    assert (fresh.status, fresh.end_time) == (WorkflowStatus.RUNNING, None), "the refused write changed the row"

    with stack.store.transaction(stack.queue) as txn:
        txn.update_workflow_status(workflow, expected_status="RUNNING")
    fresh = stack.store.retrieve(workflow.id)
    assert (fresh.status, fresh.end_time) == (WorkflowStatus.SUCCEEDED, 5), "the matching write did not apply"
