"""A deferred-choice member that finishes and then loses passes nothing downstream (#123)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any, ClassVar

import pytest

from stabilize import Orchestrator, QueueProcessor, Task, TaskRegistry, TaskResult
from stabilize.handlers.skip_stage import SkipStageHandler
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.queue.messages import SkipStage
from tests.event_stack import Stack, open_stack


class Member(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"member": stage.ref_id, f"from_{stage.ref_id}": True})


class After(Task):
    seen: ClassVar[list[dict[str, Any]]] = []

    def execute(self, stage: StageExecution) -> TaskResult:
        After.seen.append({key: stage.context.get(key) for key in ("member", "from_a", "from_b")})
        return TaskResult.success()


@pytest.fixture
def stack(tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[Stack]:
    with open_stack("sqlite", tmp_path, request) as opened:
        yield opened


def _stage(ref_id: str, task: str, **extra: Any) -> StageExecution:
    return StageExecution(
        ref_id=ref_id,
        type="work",
        name=ref_id,
        tasks=[TaskExecution.create(name=ref_id, implementing_class=task, stage_start=True, stage_end=True)],
        **extra,
    )


def _started(stack: Stack) -> tuple[QueueProcessor, Workflow]:
    After.seen = []
    registry = TaskRegistry()
    registry.register("member", Member)
    registry.register("after", After)
    claim = {"deferredChoiceClaim": "on_complete"}
    workflow = Workflow.create(
        application="choice",
        name="loser-outputs",
        stages=[
            _stage("a", "member", deferred_choice_group="g", context=dict(claim)),
            _stage("b", "member", deferred_choice_group="g", context=dict(claim)),
            _stage("after", "after", requisite_stage_ref_ids={"a", "b"}),
        ],
    )
    stack.store.store(workflow)
    Orchestrator(stack.queue, store=stack.store).start(workflow)
    return QueueProcessor(stack.queue, store=stack.store, task_registry=registry), workflow


def _members(stack: Stack, workflow: Workflow) -> tuple[StageExecution, StageExecution]:
    stages = {s.ref_id: s for s in stack.store.retrieve(workflow.id).stages}
    return stages["a"], stages["b"]


def _winner_and_loser(stack: Stack, workflow: Workflow) -> tuple[StageExecution, StageExecution]:
    a, b = _members(stack, workflow)
    assert {a.status, b.status} == {WorkflowStatus.SUCCEEDED, WorkflowStatus.SKIPPED}, (a.status, b.status)
    return (a, b) if a.status == WorkflowStatus.SUCCEEDED else (b, a)


def test_a_member_that_finished_and_then_lost_passes_nothing_downstream(stack: Stack) -> None:
    processor, workflow = _started(stack)

    processor.process_all(timeout=60.0)

    assert stack.store.retrieve(workflow.id).status == WorkflowStatus.SUCCEEDED
    winner, loser = _winner_and_loser(stack, workflow)
    assert loser.outputs == {}, f"a member that lost the deferred choice kept its outputs: {loser.outputs}"
    assert After.seen == [
        {"member": winner.ref_id, f"from_{winner.ref_id}": True, f"from_{loser.ref_id}": None}
    ], f"the stage after the choice received the loser's outputs: {After.seen}"


def test_the_winner_of_the_choice_passes_its_outputs_downstream(stack: Stack) -> None:
    processor, workflow = _started(stack)

    processor.process_all(timeout=60.0)

    winner, _ = _winner_and_loser(stack, workflow)
    assert winner.outputs == {"member": winner.ref_id, f"from_{winner.ref_id}": True}
    assert After.seen and After.seen[0][f"from_{winner.ref_id}"] is True


def test_a_finished_member_pruned_by_the_winner_passes_nothing_downstream(stack: Stack) -> None:
    processor, workflow = _started(stack)
    for _ in range(200):
        a, b = _members(stack, workflow)
        if a.status.is_complete or b.status.is_complete:
            break
        assert processor.process_one(), "the queue ran dry before the choice was settled"
    a, b = _members(stack, workflow)
    winner, other = (a, b) if a.status.is_complete else (b, a)
    assert winner.status == WorkflowStatus.SUCCEEDED
    assert other.status == WorkflowStatus.RUNNING and other.outputs.get("member") == other.ref_id, (
        "the other member had not finished its task when the choice was settled",
        other.status,
        other.outputs,
    )

    SkipStageHandler(stack.queue, stack.store).handle(
        SkipStage(execution_type=workflow.type.value, execution_id=workflow.id, stage_id=other.id, pruned=True)
    )
    processor.process_all(timeout=60.0)

    _, loser = _winner_and_loser(stack, workflow)
    assert loser.id == other.id
    assert loser.outputs == {}, f"a finished member pruned by the winner kept its outputs: {loser.outputs}"
    assert After.seen == [
        {"member": winner.ref_id, f"from_{winner.ref_id}": True, f"from_{loser.ref_id}": None}
    ], f"the stage after the choice received the loser's outputs: {After.seen}"
