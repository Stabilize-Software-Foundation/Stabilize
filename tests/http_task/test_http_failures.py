"""How HTTPTask reports a request that fails: a code beside the text, a result and no raise (#121, #124, #125, #126)."""

from __future__ import annotations

import socket
import threading
import time
from collections.abc import Iterator
from http.client import HTTPException
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, ClassVar

import pytest

from stabilize import HTTPTask, Orchestrator, QueueProcessor, TaskRegistry, TaskResult
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from tests.event_stack import Stack, open_stack

CODES = {
    "invalid_request",
    "blocked_address",
    "timeout",
    "connection_failed",
    "response_too_large",
    "http_status",
    "no_response",
}


class Routes(BaseHTTPRequestHandler):
    flaky_calls: ClassVar[int] = 0

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _answer(self, status: int, body: bytes, declared: int | None = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body) if declared is None else declared))
        self.end_headers()
        self.wfile.write(body)
        self.wfile.flush()

    def do_GET(self) -> None:
        if self.path == "/fail":
            self._answer(500, b"failed")
        elif self.path == "/big":
            self._answer(200, b"x" * 100)
        elif self.path == "/slow":
            time.sleep(1.0)
            self._answer(200, b"late")
        elif self.path == "/short":
            self._answer(200, b"0123456789", declared=100)
        elif self.path == "/stall":
            self._answer(200, b"0123456789", declared=100)
            time.sleep(1.0)
        elif self.path == "/garbage":
            self.wfile.write(b"garbage\r\n\r\n")
        elif self.path == "/flaky":
            Routes.flaky_calls += 1
            if Routes.flaky_calls == 1:
                self._answer(500, b"failed once")
            else:
                self._answer(200, b"recovered")
        else:
            self._answer(200, b"ok")


class CountingServer(ThreadingHTTPServer):
    daemon_threads = True
    accepted = 0

    def get_request(self) -> Any:
        self.accepted += 1
        return super().get_request()


@pytest.fixture
def server() -> Iterator[CountingServer]:
    instance = CountingServer(("127.0.0.1", 0), Routes)
    threading.Thread(target=instance.serve_forever, daemon=True).start()
    Routes.flaky_calls = 0
    yield instance
    instance.shutdown()
    instance.server_close()


@pytest.fixture
def stack(tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[Stack]:
    with open_stack("sqlite", tmp_path, request) as opened:
        yield opened


def _connections(server: CountingServer) -> int:
    deadline = time.monotonic() + 3.0
    seen, since = server.accepted, time.monotonic()
    while time.monotonic() < deadline:
        time.sleep(0.05)
        if server.accepted != seen:
            seen, since = server.accepted, time.monotonic()
        elif seen and time.monotonic() - since >= 0.4:
            break
    return seen


def _url(server: CountingServer, path: str) -> str:
    return f"http://127.0.0.1:{server.server_address[1]}{path}"


def _execute(**context: Any) -> TaskResult:
    stage = StageExecution(ref_id="call", type="http", name="call", context={"retries": 0, **context})
    try:
        return HTTPTask().execute(stage)
    except (OSError, HTTPException) as error:
        pytest.fail(f"a failed request escaped execute() as an exception and not as a result: {error!r}")


def _code(result: TaskResult) -> Any:
    assert result.status in (WorkflowStatus.TERMINAL, WorkflowStatus.FAILED_CONTINUE), result.status
    assert isinstance(result.context.get("error"), str) and result.context["error"]
    code = result.context.get("error_code")
    assert code in CODES, f"the failed result carries no error code from the documented set: {code!r}"
    return code


def test_a_request_with_no_url_is_an_invalid_request() -> None:
    assert _code(_execute()) == "invalid_request"


def test_an_address_refused_before_the_request_is_a_blocked_address(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(url: str) -> None:
        raise ValueError(f"SSRF blocked: {url}")

    monkeypatch.setattr("stabilize.tasks.http.task._validate_url_safety", refuse)

    result = _execute(url="http://203.0.113.1/")

    assert _code(result) == "blocked_address"
    assert result.context["error"].startswith("SSRF blocked")


def test_a_peer_refused_at_connect_time_is_a_blocked_address_and_is_not_tried_again(
    server: CountingServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("stabilize.tasks.http.task._is_blocked", lambda addr: True)

    result = _execute(url=_url(server, "/ok"), retries=2, retry_delay=0.01)

    connections = _connections(server)
    assert connections == 1, f"a connect-time refusal was tried again: {connections} connections"
    assert _code(result) == "blocked_address"
    assert "SSRF blocked" in result.context["error"]


def test_a_timeout_is_still_tried_again(server: CountingServer) -> None:
    result = _execute(url=_url(server, "/slow"), allow_private_urls=True, timeout=0.2, retries=2, retry_delay=0.01)

    assert _connections(server) == 3
    assert result.status == WorkflowStatus.TERMINAL


def test_a_body_over_the_cap_is_response_too_large(server: CountingServer) -> None:
    result = _execute(url=_url(server, "/big"), allow_private_urls=True, max_response_size=10)

    assert _code(result) == "response_too_large"
    assert result.context["error"].startswith("Response exceeds max size")


def test_a_status_the_stage_does_not_accept_is_http_status(server: CountingServer) -> None:
    result = _execute(url=_url(server, "/fail"), allow_private_urls=True)

    assert _code(result) == "http_status"
    assert result.context["status_code"] == 500


def test_a_failure_the_workflow_continues_past_carries_the_code_too(server: CountingServer) -> None:
    result = _execute(url=_url(server, "/fail"), allow_private_urls=True, continue_on_failure=True)

    assert result.status == WorkflowStatus.FAILED_CONTINUE
    assert _code(result) == "http_status"
    assert result.outputs["status_code"] == 500 and "error_code" not in result.outputs


def test_no_answer_within_the_timeout_is_a_timeout(server: CountingServer) -> None:
    assert _code(_execute(url=_url(server, "/slow"), allow_private_urls=True, timeout=0.2)) == "timeout"


def test_a_port_with_no_listener_is_connection_failed() -> None:
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()

    assert _code(_execute(url=f"http://127.0.0.1:{port}/", allow_private_urls=True)) == "connection_failed"


def test_a_body_that_stalls_past_the_timeout_is_a_result_and_not_a_raise(server: CountingServer) -> None:
    result = _execute(url=_url(server, "/stall"), allow_private_urls=True, timeout=0.3, continue_on_failure=True)

    assert result.status == WorkflowStatus.FAILED_CONTINUE
    assert _code(result) == "timeout"


def test_a_reply_that_is_not_http_is_a_result_and_not_a_raise(server: CountingServer) -> None:
    assert _code(_execute(url=_url(server, "/garbage"), allow_private_urls=True)) == "connection_failed"


def test_a_body_shorter_than_its_content_length_is_a_failure(server: CountingServer) -> None:
    result = _execute(url=_url(server, "/short"), allow_private_urls=True)

    assert result.status == WorkflowStatus.TERMINAL, (
        f"a body of 10 bytes was accepted for a Content-Length of 100: {result.status} {result.outputs.get('body')!r}"
    )
    assert _code(result) == "connection_failed"
    assert "10 of 100 bytes" in result.context["error"]


def test_a_complete_body_is_still_a_success(server: CountingServer) -> None:
    result = _execute(url=_url(server, "/big"), allow_private_urls=True)

    assert result.status == WorkflowStatus.SUCCEEDED
    assert result.outputs["body"] == "x" * 100 and result.outputs["content_length"] == 100
    assert "error_code" not in result.context and "error_code" not in result.outputs


@pytest.mark.parametrize(
    ("registered", "implementing_class"),
    [("http", "http"), ("HTTPTask", "stabilize.tasks.http.HTTPTask")],
    ids=["by-name", "by-class-path"],
)
def test_a_restarted_stage_that_succeeds_shows_nothing_of_its_failed_attempt(
    stack: Stack, server: CountingServer, registered: str, implementing_class: str
) -> None:
    registry = TaskRegistry()
    registry.register(registered, HTTPTask)
    stage = StageExecution(
        ref_id="only",
        type="http",
        name="only",
        tasks=[
            TaskExecution.create(name="only", implementing_class=implementing_class, stage_start=True, stage_end=True)
        ],
        context={"url": _url(server, "/flaky"), "allow_private_urls": True, "retries": 0},
    )
    workflow = Workflow.create(application="http", name="flaky", stages=[stage])
    stack.store.store(workflow)
    orchestrator = Orchestrator(stack.queue, store=stack.store)
    processor = QueueProcessor(stack.queue, store=stack.store, task_registry=registry)
    orchestrator.start(workflow)
    processor.process_all(timeout=60.0)
    failed = stack.store.retrieve(workflow.id)
    assert failed.status == WorkflowStatus.TERMINAL
    assert failed.stages[0].context["error"] == "HTTP 500" and failed.stages[0].context["status_code"] == 500

    orchestrator.restart(failed, failed.stages[0].id)
    processor.process_all(timeout=60.0)

    finished = stack.store.retrieve(workflow.id)
    assert finished.status == WorkflowStatus.SUCCEEDED
    context = finished.stages[0].context
    residue = ("error", "error_code", "status_code", "response_status", "response_body")
    left = sorted(key for key in residue if key in context)
    assert left == [], f"a stage that succeeded after a restart still shows its failed attempt: {left}"
    assert finished.stages[0].outputs["status_code"] == 200
    assert context["url"] == _url(server, "/flaky")


def test_the_codes_these_tests_expect_are_the_ones_the_module_lists() -> None:
    from stabilize.tasks.http.failure import ERROR_CODES

    assert ERROR_CODES == CODES
