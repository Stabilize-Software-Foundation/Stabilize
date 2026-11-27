"""With keep_alive, HTTPTask sends requests to a host on a kept connection, and only on a clean one (#127)."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, ClassVar
from urllib.request import Request

import pytest

from stabilize import HTTPTask, TaskResult
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus


class Routes(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    posts: ClassVar[int] = 0

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _answer(self, status: int, body: bytes, declared: int | None = None, **headers: str) -> None:
        self.send_response(status)
        self.send_header("Content-Length", str(len(body) if declared is None else declared))
        for name, value in headers.items():
            self.send_header(name, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)
        self.wfile.flush()

    def do_HEAD(self) -> None:
        self._answer(200, b"12345")

    def do_POST(self) -> None:
        Routes.posts += 1
        self._answer(200, self.rfile.read(int(self.headers.get("Content-Length", 0))))

    def do_GET(self) -> None:
        if self.path.startswith("/echo/"):
            self._answer(200, self.path.removeprefix("/echo/").encode())
        elif self.path == "/big":
            self._answer(200, b"x" * 100)
        elif self.path == "/short":
            self._answer(200, b"0123456789", declared=100)
            self.close_connection = True
        elif self.path == "/chunked":
            self.send_response(200)
            self.send_header("Transfer-Encoding", "chunked")
            self.end_headers()
            self.wfile.write(b"3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n")
        elif self.path == "/says-close":
            self._answer(200, b"closing", Connection="close")
        elif self.path == "/silent-close":
            self._answer(200, b"gone")
            self.close_connection = True
        elif self.path == "/moved":
            self._answer(302, b"", Location=f"http://{self.headers['Host']}/echo/moved")
        elif self.path == "/fail":
            self._answer(500, b"failed")
        elif self.path == "/slow":
            time.sleep(0.4)
            self._answer(200, b"slow")
        else:
            self._answer(200, b"ok")


class CountingServer(ThreadingHTTPServer):
    daemon_threads = True
    accepted = 0

    def get_request(self) -> Any:
        self.accepted += 1
        return super().get_request()


@pytest.fixture(autouse=True)
def _empty_pool() -> Iterator[None]:
    try:
        from stabilize.tasks.http.kept import POOL
    except ImportError:
        yield
        return
    POOL.clear()
    yield
    POOL.clear()


@pytest.fixture
def server() -> Iterator[CountingServer]:
    instance = CountingServer(("127.0.0.1", 0), Routes)
    threading.Thread(target=instance.serve_forever, daemon=True).start()
    Routes.posts = 0
    yield instance
    instance.shutdown()
    instance.server_close()


def _url(server: CountingServer, path: str) -> str:
    return f"http://127.0.0.1:{server.server_address[1]}{path}"


def _call(server: CountingServer, path: str, **context: Any) -> TaskResult:
    settings = {"allow_private_urls": True, "retries": 0, "keep_alive": True, **context}
    stage = StageExecution(ref_id="call", type="http", name="call", context={"url": _url(server, path), **settings})
    return HTTPTask().execute(stage)


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


def _opened(server: CountingServer, expected: int) -> None:
    connections = _connections(server)
    assert connections == expected, f"with keep_alive the requests opened {connections} connections, not {expected}"


def _bodies(results: list[TaskResult]) -> list[Any]:
    assert [result.status for result in results] == [WorkflowStatus.SUCCEEDED] * len(results), [
        (result.status, result.context.get("error")) for result in results
    ]
    return [result.outputs["body"] for result in results]


def test_five_requests_to_one_host_use_one_connection(server: CountingServer) -> None:
    results = [_call(server, f"/echo/{number}") for number in range(5)]

    assert _bodies(results) == ["0", "1", "2", "3", "4"]
    _opened(server, 1)
    from stabilize.tasks.http.kept import POOL

    assert POOL.idle_count() == 1


def test_without_keep_alive_each_request_opens_a_connection(server: CountingServer) -> None:
    results = [_call(server, f"/echo/{number}", keep_alive=False) for number in range(5)]

    assert _bodies(results) == ["0", "1", "2", "3", "4"]
    assert _connections(server) == 5


def test_a_post_after_a_get_gets_its_own_answer_on_the_kept_connection(server: CountingServer) -> None:
    results = [
        _call(server, "/echo/first"),
        _call(server, "/post", method="POST", body="second"),
        _call(server, "/echo/third"),
    ]

    assert _bodies(results) == ["first", "second", "third"]
    _opened(server, 1)


def test_a_chunked_answer_and_a_head_answer_leave_the_connection_kept(server: CountingServer) -> None:
    results = [_call(server, "/chunked"), _call(server, "/head", method="HEAD"), _call(server, "/chunked")]

    assert _bodies(results) == ["abcde", "", "abcde"]
    _opened(server, 1)


def test_a_failed_status_whose_body_was_read_leaves_the_connection_kept(server: CountingServer) -> None:
    first, second = _call(server, "/fail"), _call(server, "/echo/after")

    assert first.status == WorkflowStatus.TERMINAL and first.context["status_code"] == 500
    assert _bodies([second]) == ["after"]
    _opened(server, 1)


def test_two_redirected_requests_open_two_connections_and_not_four(server: CountingServer) -> None:
    results = [_call(server, "/moved"), _call(server, "/moved")]

    assert _bodies(results) == ["moved", "moved"]
    _opened(server, 2)


def test_a_connection_the_server_said_it_would_close_is_not_kept(server: CountingServer) -> None:
    from stabilize.tasks.http.kept import POOL

    first = _call(server, "/says-close")
    kept_after_first = POOL.idle_count()
    second = _call(server, "/post", method="POST", body="after")

    assert _bodies([first, second]) == ["closing", "after"]
    assert kept_after_first == 0
    assert _connections(server) == 2


def test_a_connection_the_server_closed_without_saying_is_not_used_again(server: CountingServer) -> None:
    first = _call(server, "/silent-close")
    time.sleep(0.3)
    second = _call(server, "/post", method="POST", body="after")

    assert _bodies([first, second]) == ["gone", "after"]
    assert _connections(server) == 2


def test_a_get_on_a_dead_kept_connection_is_sent_again_on_a_new_one(
    server: CountingServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = _call(server, "/silent-close")
    time.sleep(0.3)
    monkeypatch.setattr("stabilize.tasks.http.kept.peer_is_quiet", lambda connection: True)

    second = _call(server, "/echo/again")

    assert _bodies([first, second]) == ["gone", "again"]
    assert _connections(server) == 2


def test_a_post_on_a_dead_kept_connection_is_reported_and_not_sent_again(
    server: CountingServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _call(server, "/silent-close")
    time.sleep(0.3)
    monkeypatch.setattr("stabilize.tasks.http.kept.peer_is_quiet", lambda connection: True)

    result = _call(server, "/post", method="POST", body="once")

    assert result.status == WorkflowStatus.TERMINAL, "a POST was sent again after its kept connection failed"
    assert result.context["error_code"] == "connection_failed"
    assert Routes.posts == 0


def test_a_body_over_the_cap_closes_its_connection_and_the_next_answer_is_clean(server: CountingServer) -> None:
    from stabilize.tasks.http.kept import POOL

    first = _call(server, "/big", max_response_size=10)
    kept_after_first = POOL.idle_count()
    second = _call(server, "/echo/clean")

    assert first.context["error_code"] == "response_too_large"
    assert kept_after_first == 0, "a connection with an unread body was kept"
    assert _bodies([second]) == ["clean"]
    assert _connections(server) == 2


def test_a_truncated_body_closes_its_connection(server: CountingServer) -> None:
    from stabilize.tasks.http.kept import POOL

    first = _call(server, "/short")

    assert first.context["error_code"] == "connection_failed"
    assert POOL.idle_count() == 0
    assert _bodies([_call(server, "/echo/clean")]) == ["clean"]


def test_a_kept_connection_made_without_the_address_check_never_serves_a_checked_request(
    server: CountingServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    unchecked = _call(server, "/echo/unchecked")
    monkeypatch.setattr("stabilize.tasks.http.task._is_blocked", lambda addr: True)

    checked = _call(server, "/echo/checked", allow_private_urls=False)

    assert _bodies([unchecked]) == ["unchecked"]
    assert checked.status == WorkflowStatus.TERMINAL, (
        f"a request that must pass the address check was served on a connection made without it: {checked.outputs}"
    )
    assert checked.context["error_code"] == "blocked_address"


def test_requests_in_threads_each_get_their_own_answer(server: CountingServer) -> None:
    answers: dict[str, list[str]] = {}

    def worker(name: str) -> None:
        answers[name] = _bodies([_call(server, f"/echo/{name}-{number}") for number in range(6)])

    threads = [threading.Thread(target=worker, args=(f"t{index}",)) for index in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)

    assert answers == {f"t{index}": [f"t{index}-{number}" for number in range(6)] for index in range(6)}
    connections = _connections(server)
    assert connections <= 6, f"with keep_alive the requests opened {connections} connections, not 6 or fewer"


def test_a_connection_idle_for_longer_than_the_limit_is_not_used_again(
    server: CountingServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("stabilize.tasks.http.kept.MAX_IDLE_SECONDS", 0.1)
    first = _call(server, "/echo/first")
    time.sleep(0.3)

    second = _call(server, "/echo/second")

    assert _bodies([first, second]) == ["first", "second"]
    assert _connections(server) == 2


def test_no_more_idle_connections_are_kept_than_the_limit(
    server: CountingServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    from stabilize.tasks.http.kept import POOL

    monkeypatch.setattr("stabilize.tasks.http.kept.MAX_IDLE_PER_KEY", 1)
    threads = [threading.Thread(target=_call, args=(server, "/slow")) for _ in range(3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)

    assert _connections(server) == 3
    assert POOL.idle_count() == 1


def test_a_forked_process_does_not_use_its_parents_kept_connections(
    server: CountingServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os
    from types import SimpleNamespace

    import stabilize.tasks.http.kept as kept

    first = _call(server, "/echo/parent")
    monkeypatch.setattr(kept, "os", SimpleNamespace(getpid=lambda: os.getpid() + 1))

    second = _call(server, "/echo/child")

    assert _bodies([first, second]) == ["parent", "child"]
    assert _connections(server) == 2


def test_a_request_through_a_proxy_is_opened_as_without_keep_alive() -> None:
    from stabilize.tasks.http.kept import KeptSession

    class Stock:
        calls: ClassVar[list[Any]] = []

        def do_open(self, connection_class: Any, request: Request, **arguments: Any) -> str:
            Stock.calls.append((connection_class, request.host))
            return "opened by urllib"

    request = Request("http://origin.example/path")
    request.set_proxy("proxy.example:3128", "http")

    answer = KeptSession(("scope",)).open(Stock(), "connection class", request)

    assert answer == "opened by urllib"
    assert Stock.calls == [("connection class", "proxy.example:3128")]
