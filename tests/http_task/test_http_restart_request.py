"""A restarted HTTP stage sends the request it declared, with nothing of the response that failed it (#119)."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, ClassVar

import pytest

from stabilize import HTTPTask, Orchestrator, QueueProcessor, TaskRegistry
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


def _fail_then_restart(stack: Stack, **context: Any) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    registry = TaskRegistry()
    registry.register("http", HTTPTask)
    stage = StageExecution(
        ref_id="only",
        type="http",
        name="only",
        tasks=[TaskExecution.create(name="only", implementing_class="http", stage_start=True, stage_end=True)],
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
    first = list(FailingServer.requests)
    assert first, "the first attempt made no request"
    after_failure = dict(failed.stages[0].context)

    FailingServer.requests = []
    orchestrator.restart(failed, failed.stages[0].id)
    processor.process_all(timeout=60.0)

    second = list(FailingServer.requests)
    assert second, "the restarted stage made no request"
    return first[0], after_failure, second


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
