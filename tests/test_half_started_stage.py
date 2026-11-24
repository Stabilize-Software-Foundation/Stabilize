"""A stage that is claimed but whose plan transaction did not commit is planned by the next delivery (#111)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from stabilize.errors import TransientError
from stabilize.events import EventType
from stabilize.handlers import StartStageHandler
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.queue.messages import StartStage, StartTask
from stabilize.recovery_plan import plan_running_workflow
from tests.event_stack import Stack, drain, events_of, open_stack


@pytest.fixture
def stack(backend: str, tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[Stack]:
    with open_stack(backend, tmp_path, request) as opened:
        yield opened


def _task(name: str) -> TaskExecution:
    return TaskExecution.create(name=name, implementing_class="success", stage_start=True, stage_end=True)


def _joined_workflow(stack: Stack) -> tuple[Workflow, StageExecution]:
    upstream = StageExecution(ref_id="up", type="work", name="Up", tasks=[_task("u")])
    upstream.status = WorkflowStatus.SUCCEEDED
    upstream.outputs = {"from_upstream": 42}
    stage = StageExecution(ref_id="s1", type="work", name="Work", requisite_stage_ref_ids={"up"}, tasks=[_task("t")])
    workflow = Workflow.create(application="half", name="w", stages=[upstream, stage])
    workflow.status = WorkflowStatus.RUNNING
    stack.store.store(workflow)
    return workflow, stage


def _fail_first_start_task_push(monkeypatch: pytest.MonkeyPatch) -> None:
    from stabilize.persistence.sqlite.transaction import AtomicTransaction

    classes: list[Any] = [AtomicTransaction]
    try:
        from stabilize.persistence.postgres.transaction import PostgresTransaction

        classes.append(PostgresTransaction)
    except ImportError:
        pass
    failed: list[bool] = []
    for cls in classes:
        original = cls.push_message

        def flaky(self: Any, msg: Any, delay: float = 0, _original: Any = original) -> None:
            if isinstance(msg, StartTask) and not failed:
                failed.append(True)
                raise TransientError("injected transient failure in the plan transaction")
            return _original(self, msg, delay)

        monkeypatch.setattr(cls, "push_message", flaky)


def _half_start(stack: Stack, monkeypatch: pytest.MonkeyPatch) -> tuple[Workflow, StageExecution, Any, StartStage]:
    workflow, stage = _joined_workflow(stack)
    _fail_first_start_task_push(monkeypatch)
    handler = StartStageHandler(stack.queue, stack.store, event_recorder=stack.recorder)
    message = StartStage(
        execution_type="PIPELINE", execution_id=workflow.id, stage_id=stage.id, message_id=f"half-{stage.id}"
    )
    with pytest.raises(TransientError, match="plan transaction"):
        handler.handle(message)
    claimed = stack.store.retrieve_stage(stage.id)
    assert claimed.status == WorkflowStatus.RUNNING, "the claim did not commit, so this is not the half-started case"
    assert not stack.queue.has_pending_message_for_stage(stage.id)
    return workflow, stage, handler, message


def _assert_planned_once(stack: Stack, workflow: Workflow, stage: StageExecution) -> None:
    planned = stack.store.retrieve_stage(stage.id)
    assert planned.context.get("from_upstream") == 42, "the stage was not planned: its context holds no upstream output"
    started = events_of(stack, workflow.id, EventType.STAGE_STARTED)
    assert len(started) == 1, f"{len(started)} stage.started events for one start"
    tasks = [m for m in drain(stack.queue) if isinstance(m, StartTask)]
    assert len(tasks) == 1, f"{len(tasks)} StartTask messages for one start"


def test_the_redelivery_of_a_start_whose_plan_failed_plans_the_stage(
    stack: Stack, monkeypatch: pytest.MonkeyPatch
) -> None:
    workflow, stage, handler, message = _half_start(stack, monkeypatch)

    handler.handle(message)

    assert stack.queue.has_pending_message_for_stage(stage.id), (
        "the redelivery queued nothing: the stage is RUNNING with no message"
    )
    _assert_planned_once(stack, workflow, stage)


def test_recovery_restarts_the_plan_of_a_half_started_stage(stack: Stack, monkeypatch: pytest.MonkeyPatch) -> None:
    workflow, stage, handler, _message = _half_start(stack, monkeypatch)

    reissued = plan_running_workflow(stack.store.retrieve(workflow.id), stack.queue, stack.store)

    kinds = [type(m).__name__ for m in reissued]
    assert kinds == ["StartStage"], f"recovery re-issued {kinds} for a stage that was never planned"
    handler.handle(reissued[0])
    _assert_planned_once(stack, workflow, stage)


def test_another_start_of_a_stage_being_planned_is_ignored(stack: Stack, monkeypatch: pytest.MonkeyPatch) -> None:
    workflow, stage, handler, _message = _half_start(stack, monkeypatch)

    handler.handle(
        StartStage(
            execution_type="PIPELINE", execution_id=workflow.id, stage_id=stage.id, message_id=f"half-other-{stage.id}"
        )
    )

    assert not stack.queue.has_pending_message_for_stage(stage.id), "a second start planned a stage it had not claimed"
    assert events_of(stack, workflow.id, EventType.STAGE_STARTED) == []
