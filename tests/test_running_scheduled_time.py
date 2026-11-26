"""A task that returns RUNNING with a scheduled_time is not run again before it (#116)."""

from __future__ import annotations

import time
from collections.abc import Iterator
from pathlib import Path

import pytest

from stabilize import Orchestrator, QueueProcessor, Task, TaskRegistry, TaskResult
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from tests.event_stack import Stack, open_stack

CALLS: list[float] = []
DUE: list[int] = []


class _AsksToWait(Task):
    wait_ms = 3000

    def execute(self, stage: StageExecution) -> TaskResult:
        CALLS.append(time.time())
        if len(CALLS) == 1:
            DUE.append(int(time.time() * 1000) + self.wait_ms)
            return TaskResult.running(scheduled_time=DUE[0])
        return TaskResult.success()


class _AsksForLessThanTheBackoff(_AsksToWait):
    wait_ms = 100


@pytest.fixture
def stack(backend: str, tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[Stack]:
    CALLS.clear()
    DUE.clear()
    with open_stack(backend, tmp_path, request) as opened:
        yield opened


def _run(stack: Stack, task: type[Task]) -> None:
    registry = TaskRegistry()
    registry.register("asks_to_wait", task)
    stage = StageExecution(
        ref_id="s1",
        type="work",
        name="Work",
        tasks=[TaskExecution.create(name="t", implementing_class="asks_to_wait", stage_start=True, stage_end=True)],
    )
    workflow = Workflow.create(application="schedule", name="w", stages=[stage])
    stack.store.store(workflow)
    Orchestrator(stack.queue, store=stack.store).start(workflow)
    QueueProcessor(stack.queue, store=stack.store, task_registry=registry).process_all(timeout=30.0)
    assert stack.store.retrieve(workflow.id).status == WorkflowStatus.SUCCEEDED
    assert len(CALLS) == 2, f"the task ran {len(CALLS)} times"


def test_a_task_is_not_run_again_before_the_time_it_asked_for(stack: Stack) -> None:
    _run(stack, _AsksToWait)

    early_ms = DUE[0] - int(CALLS[1] * 1000)
    assert early_ms <= 50, (
        f"the task ran again {early_ms} ms before its scheduled_time: "
        "the engine re-queued it with its backoff and ignored the time it asked for"
    )


def test_a_scheduled_time_sooner_than_the_backoff_does_not_shorten_the_backoff(stack: Stack) -> None:
    _run(stack, _AsksForLessThanTheBackoff)

    waited = CALLS[1] - CALLS[0]
    assert waited >= 0.9, f"the task ran again after {waited:.2f} s, sooner than its 1 s backoff"
