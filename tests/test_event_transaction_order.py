"""A lifecycle event is appended inside the transaction that writes its state and queues its successor (#111)."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from stabilize.errors import TransientError
from stabilize.events import EventType, get_event_bus
from stabilize.events.base import EntityType, Event
from stabilize.events.replay import EventReplayer
from stabilize.handlers import (
    CancelStageHandler,
    CompleteWorkflowHandler,
    SkipStageHandler,
    StartStageHandler,
    StartTaskHandler,
    StartWaitingWorkflowsHandler,
    StartWorkflowHandler,
)
from stabilize.models.status import WorkflowStatus
from stabilize.models.workflow import Workflow
from stabilize.queue.messages import (
    CancelStage,
    CompleteWorkflow,
    SkipStage,
    StartStage,
    StartTask,
    StartWaitingWorkflows,
    StartWorkflow,
)
from tests.event_stack import (
    Stack,
    events_of,
    from_another_thread,
    hooked_recorder,
    one_stage_workflow,
    open_stack,
    registry,
    set_statuses,
)

Case = tuple[Workflow, Callable[[Any], Any], Any, EventType]


@pytest.fixture
def stack(backend: str, tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[Stack]:
    with open_stack(backend, tmp_path, request) as opened:
        yield opened


def _start_stage(stack: Stack) -> Case:
    workflow = one_stage_workflow()
    set_statuses(workflow, WorkflowStatus.RUNNING, WorkflowStatus.NOT_STARTED)
    stack.store.store(workflow)
    message = StartStage(
        execution_type="PIPELINE",
        execution_id=workflow.id,
        stage_id=workflow.stages[0].id,
        message_id=f"order-ss-{workflow.id}",
    )
    return (
        workflow,
        lambda rec: StartStageHandler(stack.queue, stack.store, event_recorder=rec),
        message,
        (EventType.STAGE_STARTED),
    )


def _start_task(stack: Stack) -> Case:
    workflow = one_stage_workflow()
    set_statuses(workflow, WorkflowStatus.RUNNING, WorkflowStatus.RUNNING)
    stack.store.store(workflow)
    stage = workflow.stages[0]
    message = StartTask(
        execution_type="PIPELINE",
        execution_id=workflow.id,
        stage_id=stage.id,
        task_id=stage.tasks[0].id,
        message_id=f"order-st-{workflow.id}",
    )
    return (
        workflow,
        lambda rec: StartTaskHandler(stack.queue, stack.store, registry(), event_recorder=rec),
        message,
        (EventType.TASK_STARTED),
    )


def _start_workflow(stack: Stack) -> Case:
    workflow = one_stage_workflow()
    stack.store.store(workflow)
    message = StartWorkflow(execution_type="PIPELINE", execution_id=workflow.id, message_id=f"order-sw-{workflow.id}")
    return (
        workflow,
        lambda rec: StartWorkflowHandler(stack.queue, stack.store, event_recorder=rec),
        message,
        (EventType.WORKFLOW_STARTED),
    )


def _cancel_stage(stack: Stack) -> Case:
    workflow = one_stage_workflow()
    set_statuses(workflow, WorkflowStatus.RUNNING, WorkflowStatus.RUNNING)
    stack.store.store(workflow)
    message = CancelStage(
        execution_type="PIPELINE",
        execution_id=workflow.id,
        stage_id=workflow.stages[0].id,
        message_id=f"order-cs-{workflow.id}",
    )
    return (
        workflow,
        lambda rec: CancelStageHandler(stack.queue, stack.store, event_recorder=rec, task_registry=registry()),
        message,
        EventType.STAGE_CANCELED,
    )


def _start_waiting(stack: Stack) -> Case:
    workflow = one_stage_workflow(is_limit_concurrent=True, max_concurrent_executions=1)
    workflow.pipeline_config_id = f"cfg-{workflow.id}"
    workflow.status = WorkflowStatus.BUFFERED
    stack.store.store(workflow)
    message = StartWaitingWorkflows(
        pipeline_config_id=workflow.pipeline_config_id, message_id=f"order-sww-{workflow.id}"
    )
    return (
        workflow,
        lambda rec: StartWaitingWorkflowsHandler(stack.queue, stack.store, event_recorder=rec),
        message,
        (EventType.WORKFLOW_STARTED),
    )


AFTER_COMMIT_CASES = {
    "start_stage": _start_stage,
    "start_task": _start_task,
    "start_workflow": _start_workflow,
    "cancel_stage": _cancel_stage,
    "start_waiting": _start_waiting,
}


@pytest.mark.parametrize("case", sorted(AFTER_COMMIT_CASES))
def test_the_event_is_appended_before_another_connection_can_see_the_successor(stack: Stack, case: str) -> None:
    workflow, make_handler, message, kind = AFTER_COMMIT_CASES[case](stack)
    seen: list[bool] = []

    def before_append(event: Event) -> None:
        if event.event_type == kind:
            seen.append(from_another_thread(lambda: stack.queue.has_pending_message_for_execution(workflow.id)))

    make_handler(hooked_recorder(stack, before_append)).handle(message)

    assert stack.queue.has_pending_message_for_execution(workflow.id), "the handler queued no successor to look for"
    assert len(seen) == 1, f"{kind.value} was appended {len(seen)} times"
    assert seen == [False], f"the successor was already visible to another connection when {kind.value} was appended"


@pytest.mark.parametrize("case", sorted(AFTER_COMMIT_CASES))
def test_a_refused_append_commits_neither_the_state_nor_the_successor(stack: Stack, case: str) -> None:
    workflow, make_handler, message, kind = AFTER_COMMIT_CASES[case](stack)
    refused: list[str] = []

    def before_append(event: Event) -> None:
        if event.event_type == kind and not refused:
            refused.append(event.event_id)
            raise TransientError("injected append failure")

    handler = make_handler(hooked_recorder(stack, before_append))
    with pytest.raises(TransientError, match="injected append failure"):
        handler.handle(message)

    assert events_of(stack, workflow.id, kind) == []
    assert not stack.queue.has_pending_message_for_execution(workflow.id), (
        f"a successor was committed although {kind.value} was never recorded"
    )

    handler.handle(message)
    assert len(events_of(stack, workflow.id, kind)) == 1, f"the redelivery did not record {kind.value} once"
    assert stack.queue.has_pending_message_for_execution(workflow.id), "the redelivery queued no successor"


def _fail_pushes_of(monkeypatch: pytest.MonkeyPatch, message_type: type) -> None:
    from stabilize.persistence.sqlite.transaction import AtomicTransaction

    classes: list[Any] = [AtomicTransaction]
    try:
        from stabilize.persistence.postgres.transaction import PostgresTransaction

        classes.append(PostgresTransaction)
    except ImportError:
        pass
    for cls in classes:
        original = cls.push_message

        def failing(self: Any, msg: Any, delay: float = 0, _original: Any = original) -> None:
            if isinstance(msg, message_type):
                raise RuntimeError("injected failure before commit")
            return _original(self, msg, delay)

        monkeypatch.setattr(cls, "push_message", failing)


def test_a_skip_that_rolls_back_leaves_no_event(stack: Stack, monkeypatch: pytest.MonkeyPatch) -> None:
    workflow = one_stage_workflow()
    set_statuses(workflow, WorkflowStatus.RUNNING, WorkflowStatus.NOT_STARTED)
    stack.store.store(workflow)
    published: list[Event] = []
    get_event_bus().subscribe("order-skip", published.append)
    _fail_pushes_of(monkeypatch, CompleteWorkflow)

    handler = SkipStageHandler(stack.queue, stack.store, event_recorder=stack.recorder, task_registry=registry())
    with pytest.raises(RuntimeError, match="injected failure before commit"):
        handler.handle(
            SkipStage(
                execution_type="PIPELINE",
                execution_id=workflow.id,
                stage_id=workflow.stages[0].id,
                message_id=f"order-skip-{workflow.id}",
            )
        )

    assert stack.store.retrieve(workflow.id).stages[0].status == WorkflowStatus.NOT_STARTED
    assert events_of(stack, workflow.id, EventType.STAGE_SKIPPED) == [], (
        "stage.skipped stayed in the log although the skip rolled back"
    )
    assert [e for e in published if e.event_type == EventType.STAGE_SKIPPED] == [], (
        "stage.skipped reached the bus although the skip rolled back"
    )


def test_a_completion_that_rolls_back_leaves_no_event(stack: Stack, monkeypatch: pytest.MonkeyPatch) -> None:
    workflow = one_stage_workflow()
    workflow.pipeline_config_id = f"cfg-{workflow.id}"
    set_statuses(workflow, WorkflowStatus.RUNNING, WorkflowStatus.SUCCEEDED)
    stack.store.store(workflow)
    published: list[Event] = []
    get_event_bus().subscribe("order-complete", published.append)
    _fail_pushes_of(monkeypatch, StartWaitingWorkflows)

    handler = CompleteWorkflowHandler(stack.queue, stack.store, event_recorder=stack.recorder)
    with pytest.raises(RuntimeError, match="injected failure before commit"):
        handler.handle(
            CompleteWorkflow(execution_type="PIPELINE", execution_id=workflow.id, message_id=f"order-cw-{workflow.id}")
        )

    assert stack.store.retrieve(workflow.id).status == WorkflowStatus.RUNNING
    assert events_of(stack, workflow.id, EventType.WORKFLOW_COMPLETED) == [], (
        "workflow.completed stayed in the log although the completion rolled back"
    )
    assert [e for e in published if e.event_type == EventType.WORKFLOW_COMPLETED] == [], (
        "workflow.completed reached the bus although the completion rolled back"
    )


def test_replay_names_a_stage_whose_first_event_carries_no_identity(stack: Stack) -> None:
    workflow = one_stage_workflow()
    set_statuses(workflow, WorkflowStatus.RUNNING, WorkflowStatus.RUNNING)
    stack.store.store(workflow)
    stage = workflow.stages[0]
    task = stage.tasks[0]

    stack.recorder.record_outputs_updated(
        EntityType.STAGE, stage.id, workflow.id, {"k": 1}, version=stage.version, source_handler="test"
    )
    stack.recorder.record_stage_started(stage, source_handler="test")
    task.status = WorkflowStatus.SUCCEEDED
    stack.recorder.record_task_completed(task, workflow_id=workflow.id, outputs={}, source_handler="test")
    stack.recorder.record_task_started(task, workflow.id, source_handler="test")

    state = EventReplayer(stack.event_store).rebuild_workflow_state(workflow.id)
    replayed_stage = state["stages"][stage.id]
    assert (replayed_stage["ref_id"], replayed_stage["type"], replayed_stage["name"]) == ("s1", "work", "Work"), (
        f"replay lost the stage's identity: {replayed_stage}"
    )
    replayed_task = state["tasks"][task.id]
    assert (replayed_task["name"], replayed_task["stage_id"]) == ("t", stage.id), (
        f"replay lost the task's identity: {replayed_task}"
    )
