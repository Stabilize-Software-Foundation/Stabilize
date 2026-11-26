"""A header or body a task sets in execute() is sent downstream of a stage that outputs those keys (#118)."""

from __future__ import annotations

import copy
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from stabilize import HTTPTask, Orchestrator, QueueProcessor, TaskRegistry, TaskResult
from stabilize.handlers import StartStageHandler
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from tests.event_stack import Stack, open_stack
from tests.http_task.conftest import MockHTTPHandler

KEY = "Bearer set-in-execute"


class HeaderAtExecute(HTTPTask):
    def execute(self, stage: StageExecution) -> TaskResult:
        shadow = copy.copy(stage)
        shadow.context = {**stage.context, "headers": {"Authorization": KEY}}
        return super().execute(shadow)


class AddedHeaderAtExecute(HTTPTask):
    def execute(self, stage: StageExecution) -> TaskResult:
        shadow = copy.copy(stage)
        headers = dict(stage.context.get("headers") or {})
        headers["Authorization"] = KEY
        shadow.context = {**stage.context, "headers": headers}
        return super().execute(shadow)


class BodyAtExecute(HTTPTask):
    def execute(self, stage: StageExecution) -> TaskResult:
        shadow = copy.copy(stage)
        shadow.context = {**stage.context, "method": "POST", "body": "set in execute"}
        return super().execute(shadow)


@pytest.fixture
def stack(tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[Stack]:
    with open_stack("sqlite", tmp_path, request) as opened:
        yield opened


def _stage(ref_id: str, task: str, url: str, after: str | None = None, **context: Any) -> StageExecution:
    return StageExecution(
        ref_id=ref_id,
        type="http",
        name=ref_id,
        requisite_stage_ref_ids={after} if after else set(),
        tasks=[TaskExecution.create(name=ref_id, implementing_class=task, stage_start=True, stage_end=True)],
        context={"url": url, "allow_private_urls": True, **context},
    )


def _run(stack: Stack, stages: list[StageExecution]) -> Workflow:
    registry = TaskRegistry()
    registry.register("http", HTTPTask)
    registry.register("header_at_execute", HeaderAtExecute)
    registry.register("added_header_at_execute", AddedHeaderAtExecute)
    registry.register("body_at_execute", BodyAtExecute)
    workflow = Workflow.create(application="http", name="execute-time", stages=stages)
    stack.store.store(workflow)
    Orchestrator(stack.queue, store=stack.store).start(workflow)
    QueueProcessor(stack.queue, store=stack.store, task_registry=registry).process_all(timeout=60.0)
    finished = stack.store.retrieve(workflow.id)
    assert finished.status == WorkflowStatus.SUCCEEDED, [
        (s.ref_id, s.status, s.context.get("error")) for s in finished.stages
    ]
    return finished


def _sent_headers() -> dict[str, str]:
    return {name.lower(): value for name, value in MockHTTPHandler.last_request["headers"].items()}


def _stored(workflow: Workflow, ref_id: str) -> str:
    stage = next(s for s in workflow.stages if s.ref_id == ref_id)
    return json.dumps([stage.context, stage.outputs], default=str)


def test_a_header_set_in_execute_is_sent_downstream_of_an_http_stage(stack: Stack, http_server: str) -> None:
    finished = _run(
        stack,
        [
            _stage("first", "http", f"{http_server}/json"),
            _stage("second", "header_at_execute", http_server, "first"),
        ],
    )

    headers = _sent_headers()
    assert headers.get("authorization") == KEY, (
        f"a header the task set in execute() was not sent downstream of a stage that output headers: {sorted(headers)}"
    )
    assert "server" not in headers
    assert KEY not in _stored(finished, "second"), "the value set on a copy at execute time was persisted"


def test_a_header_added_in_execute_to_declared_headers_is_sent(stack: Stack, http_server: str) -> None:
    _run(
        stack,
        [
            _stage("first", "http", f"{http_server}/json"),
            _stage("second", "added_header_at_execute", http_server, "first", headers={"X-Own": "declared"}),
        ],
    )

    headers = _sent_headers()
    assert headers.get("x-own") == "declared"
    assert headers.get("authorization") == KEY, (
        f"a header the task added in execute() to the stage's declared headers was not sent: {sorted(headers)}"
    )


def test_a_body_set_in_execute_is_sent_downstream_of_an_http_stage(stack: Stack, http_server: str) -> None:
    _run(
        stack,
        [
            _stage("first", "http", f"{http_server}/json"),
            _stage("second", "body_at_execute", f"{http_server}/echo", "first"),
        ],
    )

    sent = MockHTTPHandler.last_request
    assert sent["method"] == "POST"
    assert sent["body"] == b"set in execute", (
        f"a body the task set in execute() was not sent downstream of a stage that output a body: {sent['body']!r}"
    )


def test_the_same_task_sends_its_header_with_no_upstream_stage(stack: Stack, http_server: str) -> None:
    _run(stack, [_stage("only", "header_at_execute", http_server)])

    assert _sent_headers().get("authorization") == KEY


def test_the_planner_digests_headers_and_body_and_no_other_key(stack: Stack) -> None:
    upstream = StageExecution(ref_id="up", type="work", name="Up")
    upstream.status = WorkflowStatus.SUCCEEDED
    upstream.outputs = {"headers": {"Server": "upstream"}, "body": "upstream body", "only_upstream": 1}
    stage = StageExecution(ref_id="s1", type="work", name="Work", requisite_stage_ref_ids={"up"})
    workflow = Workflow.create(application="http", name="digests", stages=[upstream, stage])
    workflow.status = WorkflowStatus.RUNNING
    stack.store.store(workflow)
    handler = StartStageHandler(stack.queue, stack.store, event_recorder=stack.recorder)
    loaded = stack.store.retrieve_stage(stage.id)

    handler._plan_stage(loaded)

    digests = loaded.context.get("_planned_digests")
    assert isinstance(digests, dict), f"the planner left no planning digest: {sorted(loaded.context)}"
    assert sorted(digests) == ["body", "headers"]
    assert all(isinstance(value, str) and len(value) == 64 for value in digests.values())


def test_an_upstream_value_left_as_planned_is_still_not_sent(stack: Stack) -> None:
    from stabilize.tasks.http.request import build_request

    upstream = StageExecution(ref_id="up", type="work", name="Up")
    upstream.status = WorkflowStatus.SUCCEEDED
    upstream.outputs = {"headers": {"Server": "upstream"}, "body": "upstream body"}
    stage = StageExecution(ref_id="s1", type="work", name="Work", requisite_stage_ref_ids={"up"})
    workflow = Workflow.create(application="http", name="unchanged", stages=[upstream, stage])
    workflow.status = WorkflowStatus.RUNNING
    stack.store.store(workflow)
    handler = StartStageHandler(stack.queue, stack.store, event_recorder=stack.recorder)
    loaded = stack.store.retrieve_stage(stage.id)
    handler._plan_stage(loaded)
    stack.store.store_stage(loaded)
    reloaded = stack.store.retrieve_stage(stage.id)

    request, body = build_request("POST", "http://example.invalid/", reloaded.context, [])

    assert reloaded.context["headers"] == {"Server": "upstream"}
    assert request.get_header("Server") is None and body is None, (
        f"an upstream value the planner left in the context was sent: {request.header_items()} {body!r}"
    )
    changed = {**reloaded.context, "body": "set after planning"}
    _, sent = build_request("POST", "http://example.invalid/", changed, [])
    assert sent == b"set after planning", f"a body changed after planning was not sent: {sent!r}"
