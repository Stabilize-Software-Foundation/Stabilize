"""An HTTP stage sends its own headers and body, never an upstream response's, and names its response outputs (#106)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from stabilize import HTTPTask, Orchestrator, QueueProcessor, TaskRegistry
from stabilize.handlers import StartStageHandler
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from tests.event_stack import Stack, open_stack
from tests.http_task.conftest import MockHTTPHandler


@pytest.fixture
def stack(tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[Stack]:
    with open_stack("sqlite", tmp_path, request) as opened:
        yield opened


def _http_stage(ref_id: str, url: str, after: str | None = None, **context: Any) -> StageExecution:
    return StageExecution(
        ref_id=ref_id,
        type="http",
        name=ref_id,
        requisite_stage_ref_ids={after} if after else set(),
        tasks=[TaskExecution.create(name=ref_id, implementing_class="http", stage_start=True, stage_end=True)],
        context={"url": url, "allow_private_urls": True, **context},
    )


def _run(stack: Stack, stages: list[StageExecution]) -> Workflow:
    registry = TaskRegistry()
    registry.register("http", HTTPTask)
    workflow = Workflow.create(application="http", name="chain", stages=stages)
    stack.store.store(workflow)
    Orchestrator(stack.queue, store=stack.store).start(workflow)
    QueueProcessor(stack.queue, store=stack.store, task_registry=registry).process_all(timeout=60.0)
    finished = stack.store.retrieve(workflow.id)
    assert finished.status == WorkflowStatus.SUCCEEDED, [
        (s.ref_id, s.status, s.context.get("error")) for s in finished.stages
    ]
    return finished


def _sent() -> dict[str, Any]:
    return dict(MockHTTPHandler.last_request)


def test_a_downstream_request_carries_none_of_the_upstream_response(stack: Stack, http_server: str) -> None:
    _run(
        stack,
        [_http_stage("first", f"{http_server}/json", parse_json=True), _http_stage("second", http_server, "first")],
    )

    sent = _sent()
    headers = {name.lower(): value for name, value in sent["headers"].items()}
    assert sent["path"] == "/"
    assert "server" not in headers and "date" not in headers, (
        f"the second stage sent an upstream response's headers as its own: {sorted(headers)}"
    )
    assert headers.get("content-length", "0") == "0", (
        f"the second stage sent an upstream response's body: content-length {headers.get('content-length')}"
    )


def test_a_downstream_stage_keeps_the_headers_it_declared(stack: Stack, http_server: str) -> None:
    _run(
        stack,
        [
            _http_stage("first", f"{http_server}/json"),
            _http_stage("second", http_server, "first", headers={"X-Own": "declared"}),
        ],
    )

    headers = {name.lower(): value for name, value in _sent()["headers"].items()}
    assert headers.get("x-own") == "declared", f"the stage's own header was not sent: {sorted(headers)}"
    assert "server" not in headers


def test_the_response_is_written_under_its_own_names_and_the_old_ones(stack: Stack, http_server: str) -> None:
    finished = _run(stack, [_http_stage("first", f"{http_server}/json", parse_json=True)])

    outputs = finished.stages[0].outputs
    assert outputs.get("response_status") == 200, f"no response_status in {sorted(outputs)}"
    assert outputs["response_body_json"] == {"message": "hello", "count": 42}
    assert outputs["response_headers"] == outputs["headers"]
    assert outputs["response_body"] == outputs["body"]
    assert outputs["status_code"] == 200 and outputs["body_json"] == outputs["response_body_json"]


def test_a_stage_planned_again_still_sends_the_headers_it_declared(stack: Stack) -> None:
    from stabilize.tasks.http.request import build_request

    upstream = StageExecution(ref_id="up", type="work", name="Up")
    upstream.status = WorkflowStatus.SUCCEEDED
    upstream.outputs = {"headers": {"Server": "upstream"}, "only_upstream": 1}
    stage = StageExecution(
        ref_id="s1",
        type="work",
        name="Work",
        requisite_stage_ref_ids={"up"},
        context={"headers": {"X-Own": "declared"}},
    )
    workflow = Workflow.create(application="http", name="replan", stages=[upstream, stage])
    workflow.status = WorkflowStatus.RUNNING
    stack.store.store(workflow)
    handler = StartStageHandler(stack.queue, stack.store, event_recorder=stack.recorder)
    loaded = stack.store.retrieve_stage(stage.id)

    handler._plan_stage(loaded)
    first, _ = build_request("GET", "http://example.invalid/", loaded.context, [])
    assert first.get_header("X-own") == "declared"

    handler._plan_stage(loaded)
    assert loaded.context["headers"] == {"Server": "upstream"}, (
        "on a second plan the ancestor's output is the context value, which loops rely on"
    )
    assert loaded.context["only_upstream"] == 1
    again, _ = build_request("GET", "http://example.invalid/", loaded.context, [])
    assert again.get_header("X-own") == "declared" and again.get_header("Server") is None, (
        f"planned again, the stage sent the upstream headers in place of its own: {again.header_items()}"
    )


def test_a_context_planned_before_the_declared_values_existed_is_sent_as_it_stands() -> None:
    from stabilize.tasks.http.request import build_request

    old_context = {"headers": {"X-Own": "declared"}, "_hydrated_keys": ["headers", "body"], "body": "kept"}
    request, body = build_request("POST", "http://example.invalid/", old_context, [])
    assert request.get_header("X-own") == "declared"
    assert body == b"kept"

    new_context = {
        "headers": {"Server": "upstream"},
        "body": "upstream body",
        "_hydrated_keys": ["headers", "body"],
        "_declared_values": {"headers": {"X-Own": "declared"}},
    }
    request, body = build_request("POST", "http://example.invalid/", new_context, [])
    assert request.get_header("X-own") == "declared" and request.get_header("Server") is None
    assert body is None, "a body that only arrived from an upstream stage was sent"
