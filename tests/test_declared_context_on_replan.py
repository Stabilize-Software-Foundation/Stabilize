"""A stage planned again keeps the values it declared for keys an ancestor also outputs (#122)."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, ClassVar

import pytest

from stabilize import HTTPTask, Orchestrator, QueueProcessor, Task, TaskRegistry, TaskResult
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from tests.event_stack import Stack, open_stack


class TwoRoutes(BaseHTTPRequestHandler):
    paths: ClassVar[list[str]] = []

    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_GET(self) -> None:
        TwoRoutes.paths.append(self.path)
        status, body = (200, b"upstream ok") if self.path == "/upstream" else (500, b"downstream failed")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class Upstream(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"target": "from upstream", "only_upstream": "fresh"})


class FailsOnce(Task):
    seen: ClassVar[list[tuple[Any, Any]]] = []

    def execute(self, stage: StageExecution) -> TaskResult:
        FailsOnce.seen.append((stage.context.get("target"), stage.context.get("only_upstream")))
        if len(FailsOnce.seen) == 1:
            return TaskResult.terminal(error="first run fails")
        return TaskResult.success()


@pytest.fixture
def two_routes() -> Iterator[str]:
    server = HTTPServer(("127.0.0.1", 0), TwoRoutes)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    TwoRoutes.paths = []
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()


@pytest.fixture
def stack(tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[Stack]:
    with open_stack("sqlite", tmp_path, request) as opened:
        yield opened


def _stage(ref_id: str, task: str, after: str | None = None, **context: Any) -> StageExecution:
    return StageExecution(
        ref_id=ref_id,
        type="work",
        name=ref_id,
        requisite_stage_ref_ids={after} if after else set(),
        tasks=[TaskExecution.create(name=ref_id, implementing_class=task, stage_start=True, stage_end=True)],
        context=context,
    )


def _fail_then_restart(stack: Stack, stages: list[StageExecution]) -> Workflow:
    registry = TaskRegistry()
    registry.register("http", HTTPTask)
    registry.register("upstream", Upstream)
    registry.register("fails_once", FailsOnce)
    workflow = Workflow.create(application="replan", name="replan", stages=stages)
    stack.store.store(workflow)
    orchestrator = Orchestrator(stack.queue, store=stack.store)
    processor = QueueProcessor(stack.queue, store=stack.store, task_registry=registry)
    orchestrator.start(workflow)
    processor.process_all(timeout=60.0)
    failed = stack.store.retrieve(workflow.id)
    assert failed.status == WorkflowStatus.TERMINAL, [(s.ref_id, s.status) for s in failed.stages]
    second = next(s for s in failed.stages if s.ref_id == "second")
    orchestrator.restart(failed, second.id)
    processor.process_all(timeout=60.0)
    return stack.store.retrieve(workflow.id)


def test_a_restarted_stage_keeps_the_value_it_declared_for_a_key_its_ancestor_outputs(stack: Stack) -> None:
    FailsOnce.seen = []

    finished = _fail_then_restart(
        stack, [_stage("first", "upstream"), _stage("second", "fails_once", "first", target="declared")]
    )

    assert finished.status == WorkflowStatus.SUCCEEDED
    assert [target for target, _ in FailsOnce.seen] == ["declared", "declared"], (
        f"planned again, the stage lost the value it declared to its ancestor's output: {FailsOnce.seen}"
    )


def test_a_restarted_stage_still_takes_a_key_only_its_ancestor_outputs(stack: Stack) -> None:
    FailsOnce.seen = []

    _fail_then_restart(stack, [_stage("first", "upstream"), _stage("second", "fails_once", "first", target="declared")])

    assert [only for _, only in FailsOnce.seen] == ["fresh", "fresh"]


def test_a_restarted_http_stage_downstream_of_an_http_stage_requests_its_own_url(
    stack: Stack, two_routes: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("stabilize.tasks.http.task._validate_url_safety", lambda url: None)
    monkeypatch.setattr("stabilize.tasks.http.task._is_blocked", lambda addr: False)
    own = {"allow_private_urls": True, "retries": 0}

    finished = _fail_then_restart(
        stack,
        [
            _stage("first", "http", url=f"{two_routes}/upstream", **own),
            _stage("second", "http", "first", url=f"{two_routes}/downstream", **own),
        ],
    )

    assert TwoRoutes.paths == ["/upstream", "/downstream", "/downstream"], (
        f"the restarted stage did not request the url it declared: {TwoRoutes.paths}"
    )
    assert finished.status == WorkflowStatus.TERMINAL
