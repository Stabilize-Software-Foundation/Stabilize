"""Registering a task under a built-in stage type's name says that the stage keeps its own task (#116)."""

from __future__ import annotations

import logging

import pytest

from stabilize import Task, TaskRegistry, TaskResult
from stabilize.models.stage import StageExecution


class _Mine(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success()


def test_a_task_named_like_the_wait_stage_type_is_reported(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING):
        TaskRegistry().register("wait", _Mine)

    assert any("stage of type 'wait'" in record.getMessage() for record in caplog.records), (
        "a task registered as 'wait' was accepted in silence, although a stage of type wait runs the built-in task"
    )


def test_a_task_with_a_name_of_its_own_is_not_reported(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.WARNING):
        TaskRegistry().register("rules.wait", _Mine)

    assert not any("stage of type" in record.getMessage() for record in caplog.records)
