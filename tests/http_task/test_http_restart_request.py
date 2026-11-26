"""A restarted HTTP stage sends the request it declared, with nothing of the response that failed it (#119)."""

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


class FailingServer(BaseHTTPRequestHandler):
    requests: ClassVar[list[dict[str, Any]]] = []

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _record(self) -> None:
        length = int(self.headers.get("Content-Length", 0) or 0)
        FailingServer.requests.append(
            {
                "method": self.command,
                "path": self.path,
                "headers": {name.lower(): value for name, value in self.headers.items()},
                "body": self.rfile.read(length) if length else b"",
            }
        )

    def _answer(self, status: int, body: bytes, **headers: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        for name, value in headers.items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        self._record()
        if self.path == "/moved":
            self.send_response(302)
            self.send_header("Location", f"http://{self.headers['Host']}/fail")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self._answer(500, b"response of the failed attempt", **{"X-Failed-Response": "yes"})

    def do_POST(self) -> None:
        self._record()
        self._answer(400, b"response of the failed attempt", **{"X-Failed-Response": "yes"})


@pytest.fixture
def failing_server() -> Iterator[str]:
    server = HTTPServer(("127.0.0.1", 0), FailingServer)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    FailingServer.requests = []
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()


@pytest.fixture
def stack(tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[Stack]:
    with open_stack("sqlite", tmp_path, request) as opened:
        yield opened


class StateForTheNextRun(Task):
    seen: ClassVar[list[Any]] = []

    def execute(self, stage: StageExecution) -> TaskResult:
        StateForTheNextRun.seen.append(stage.context.get("body"))
        return TaskResult.terminal(error="failed", context={"body": "left for the next run"})


def _fail(stack: Stack, task: str = "http", **context: Any) -> tuple[Orchestrator, QueueProcessor, Workflow]:
    registry = TaskRegistry()
    registry.register("http", HTTPTask)
    registry.register("state_for_the_next_run", StateForTheNextRun)
    stage = StageExecution(
        ref_id="only",
        type="http",
        name="only",
        tasks=[TaskExecution.create(name="only", implementing_class=task, stage_start=True, stage_end=True)],
        context={"allow_private_urls": True, "retries": 0, **context},
    )
    workflow = Workflow.create(application="http", name="restart", stages=[stage])
    stack.store.store(workflow)
    orchestrator = Orchestrator(stack.queue, store=stack.store)
    processor = QueueProcessor(stack.queue, store=stack.store, task_registry=registry)
    orchestrator.start(workflow)
    processor.process_all(timeout=60.0)
    failed = stack.store.retrieve(workflow.id)
    assert failed.status == WorkflowStatus.TERMINAL, [(s.status, s.context.get("error")) for s in failed.stages]
    return orchestrator, processor, failed


def _fail_then_restart(
    stack: Stack, task: str = "http", edit: dict[str, Any] | None = None, **context: Any
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    orchestrator, processor, failed = _fail(stack, task, **context)
    first = list(FailingServer.requests)
    after_failure = dict(failed.stages[0].context)
    if edit:
        edited = stack.store.retrieve_stage(failed.stages[0].id)
        edited.context.update(edit)
        stack.store.store_stage(edited)

    FailingServer.requests = []
    orchestrator.restart(failed, failed.stages[0].id)
    processor.process_all(timeout=60.0)

    second = list(FailingServer.requests)
    if task == "http":
        assert first, "the first attempt made no request"
        assert second, "the restarted stage made no request"
    return (first[0] if first else {}), after_failure, second


def test_a_restarted_http_stage_sends_its_declared_headers_and_not_the_failed_response(
    stack: Stack, failing_server: str
) -> None:
    first, after_failure, second = _fail_then_restart(
        stack, url=f"{failing_server}/fail", headers={"X-Own": "declared"}
    )

    assert first["headers"].get("x-own") == "declared"
    assert after_failure["response_headers"].get("X-Failed-Response") == "yes"
    sent = second[0]
    assert "x-failed-response" not in sent["headers"] and sent["body"] == b"", (
        f"the response of the failed attempt was sent as the restarted request: {sorted(sent['headers'])} "
        f"{sent['body']!r}"
    )
    assert sent["headers"].get("x-own") == "declared", (
        f"the restarted request lost the header the stage declared: {sorted(sent['headers'])}"
    )


def test_a_restarted_http_stage_with_no_declared_headers_sends_none_of_the_failed_response(
    stack: Stack, failing_server: str
) -> None:
    first, _, second = _fail_then_restart(stack, url=f"{failing_server}/fail")

    sent = second[0]
    assert sorted(sent["headers"]) == sorted(first["headers"]) and sent["body"] == b"", (
        f"the response of the failed attempt was sent as the restarted request: {sorted(sent['headers'])} "
        f"{sent['body']!r}"
    )


def test_a_restarted_http_stage_sends_its_declared_body(stack: Stack, failing_server: str) -> None:
    first, after_failure, second = _fail_then_restart(
        stack, url=f"{failing_server}/reject", method="POST", body="declared body"
    )

    assert first["body"] == b"declared body"
    assert after_failure["response_body"] == "response of the failed attempt"
    assert second[0]["body"] == b"declared body", (
        f"the restarted request did not carry the body the stage declared: {second[0]['body']!r}"
    )


def test_a_restarted_http_stage_requests_the_url_it_declared(stack: Stack, failing_server: str) -> None:
    first, after_failure, second = _fail_then_restart(stack, url=f"{failing_server}/moved")

    assert first["path"] == "/moved"
    assert after_failure["status_code"] == 500
    assert [request["path"] for request in second] == ["/moved", "/fail"], (
        f"the restarted stage did not request the url it declared: {[request['path'] for request in second]}"
    )


def test_a_failed_http_stage_still_shows_the_response_in_its_context(stack: Stack, failing_server: str) -> None:
    _, after_failure, _ = _fail_then_restart(stack, url=f"{failing_server}/fail", headers={"X-Own": "declared"})

    assert after_failure["status_code"] == 500
    assert after_failure["body"] == "response of the failed attempt"
    assert after_failure["headers"].get("X-Failed-Response") == "yes"
    assert after_failure["url"] == f"{failing_server}/fail"


def test_the_record_of_a_failed_attempt_holds_only_the_request_keys_the_response_replaced(
    stack: Stack, failing_server: str
) -> None:
    _, after_failure, _ = _fail_then_restart(stack, url=f"{failing_server}/fail", headers={"X-Own": "declared"})

    record = after_failure.get("_displaced_request")
    assert isinstance(record, dict), f"the failed attempt left no record of the request it replaced: {record!r}"
    assert sorted(record) == ["body", "headers"]
    assert record["headers"]["was"] == {"X-Own": "declared"}
    assert "was" not in record["body"]


def test_a_request_value_changed_between_the_failure_and_the_restart_is_sent(
    stack: Stack, failing_server: str
) -> None:
    _, _, second = _fail_then_restart(
        stack,
        edit={"body": "set after the failure"},
        url=f"{failing_server}/reject",
        method="POST",
        body="declared body",
    )

    assert second[0]["body"] == b"set after the failure", (
        f"a body set between the failure and the restart was not sent: {second[0]['body']!r}"
    )


def test_a_task_that_is_not_an_http_task_keeps_the_context_its_failure_left(stack: Stack) -> None:
    StateForTheNextRun.seen = []

    _, after_failure, _ = _fail_then_restart(stack, task="state_for_the_next_run", body="declared")

    assert "_displaced_request" not in after_failure
    assert StateForTheNextRun.seen == ["declared", "left for the next run"], (
        f"a restart took away the context a task's failure left for its next run: {StateForTheNextRun.seen}"
    )


def test_a_jump_into_a_failed_http_stage_sends_the_declared_headers_and_the_body_the_jump_sets(
    stack: Stack, failing_server: str
) -> None:
    from stabilize.handlers.jump_to_stage.target import apply_jump_to_target
    from stabilize.queue.messages import JumpToStage
    from stabilize.tasks.http.request import build_request

    _, _, failed = _fail(
        stack, url=f"{failing_server}/reject", method="POST", headers={"X-Own": "declared"}, body="declared body"
    )
    stage = stack.store.retrieve_stage(failed.stages[0].id)
    assert stage.context["body"] == "response of the failed attempt"
    message = JumpToStage(
        execution_type=failed.type.value,
        execution_id=failed.id,
        stage_id=stage.id,
        target_stage_ref_id="only",
        jump_context={"body": "set by the jump"},
    )

    apply_jump_to_target(stage, message=message, jump_count=1, history=[])

    request, body = build_request("POST", stage.context["url"], stage.context, [])
    assert request.get_header("X-own") == "declared" and request.get_header("X-failed-response") is None, (
        f"a jump into a failed stage built its request from the failed response: {request.header_items()}"
    )
    assert body == b"set by the jump"
