from __future__ import annotations

import http.client
import os
import selectors
import threading
import time
from typing import TYPE_CHECKING, Any
from urllib.error import URLError
from urllib.request import HTTPHandler, HTTPSHandler

from stabilize.tasks.http.guarded_opener import guarded_connections

if TYPE_CHECKING:
    from collections.abc import Callable
    from urllib.request import Request

MAX_IDLE_PER_KEY = 8
MAX_IDLE_SECONDS = 30.0
REPLAYED_METHODS = frozenset({"GET", "HEAD", "PUT", "DELETE", "OPTIONS"})

Key = tuple[Any, ...]


def peer_is_quiet(connection: http.client.HTTPConnection) -> bool:
    sock = connection.sock
    if sock is None:
        return False
    pending = getattr(sock, "pending", None)
    try:
        if pending is not None and pending():
            return False
        with selectors.DefaultSelector() as selector:
            selector.register(sock, selectors.EVENT_READ)
            return not selector.select(0)
    except (OSError, ValueError):
        return False


def dropped(error: BaseException) -> bool:
    reason = error.reason if isinstance(error, URLError) else error
    return isinstance(reason, (ConnectionResetError, BrokenPipeError, ConnectionAbortedError))


def spent(response: http.client.HTTPResponse, *, chunked_counts: bool) -> bool:
    if not response.isclosed() or response.will_close:
        return False
    return response.length == 0 or (chunked_counts and bool(response.chunked))


class KeptConnections:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._idle: dict[Key, list[tuple[float, http.client.HTTPConnection]]] = {}
        self._pid = os.getpid()

    def _own_process(self) -> None:
        if self._pid != os.getpid():
            self._lock = threading.Lock()
            self._idle = {}
            self._pid = os.getpid()

    def take(self, key: Key) -> http.client.HTTPConnection | None:
        self._own_process()
        while True:
            with self._lock:
                idle = self._idle.get(key)
                if not idle:
                    return None
                since, connection = idle.pop()
            if time.monotonic() - since <= MAX_IDLE_SECONDS and peer_is_quiet(connection):
                return connection
            connection.close()

    def give(self, key: Key, connection: http.client.HTTPConnection) -> None:
        self._own_process()
        with self._lock:
            idle = self._idle.setdefault(key, [])
            if len(idle) < MAX_IDLE_PER_KEY:
                idle.append((time.monotonic(), connection))
                return
        connection.close()

    def idle_count(self) -> int:
        self._own_process()
        with self._lock:
            return sum(len(idle) for idle in self._idle.values())

    def clear(self) -> None:
        self._own_process()
        with self._lock:
            idle, self._idle = self._idle, {}
        for entries in idle.values():
            for _, connection in entries:
                connection.close()


POOL = KeptConnections()


class KeptSession:
    def __init__(self, scope: Key) -> None:
        self._scope = scope
        self._opened: list[tuple[Key, http.client.HTTPConnection, http.client.HTTPResponse]] = []
        self._body_read = False

    def body_read(self) -> None:
        self._body_read = True

    def open(self, handler: Any, connection_class: Any, request: Request, **connection_args: Any) -> Any:
        if request.has_proxy() or getattr(request, "_tunnel_host", None):
            return handler.do_open(connection_class, request, **connection_args)
        host = request.host
        if not host:
            raise URLError("no host given")
        headers = dict(request.unredirected_hdrs)
        headers.update({name: value for name, value in request.headers.items() if name not in headers})
        headers = {name.title(): value for name, value in headers.items()}
        headers.setdefault("Connection", "keep-alive")
        key: Key = (request.type, host, *self._scope)
        method = request.get_method()
        may_replay = method in REPLAYED_METHODS and (request.data is None or isinstance(request.data, bytes))
        while True:
            connection = POOL.take(key)
            reused = connection is not None
            if connection is None:
                connection = connection_class(host, timeout=request.timeout, **connection_args)
            else:
                connection.timeout = request.timeout
                if connection.sock is not None:
                    connection.sock.settimeout(request.timeout)
            try:
                try:
                    connection.request(
                        method,
                        request.selector,
                        request.data,
                        headers,
                        encode_chunked=request.has_header("Transfer-encoding"),
                    )
                except OSError as error:
                    raise URLError(error) from error
                response = connection.getresponse()
            except BaseException as error:
                connection.close()
                if reused and may_replay and dropped(error):
                    continue
                raise
            break
        response.url = request.get_full_url()
        response.msg = response.reason  # type: ignore[assignment]
        self._opened.append((key, connection, response))
        return response

    def settle(self) -> None:
        opened, self._opened = self._opened, []
        for index, (key, connection, response) in enumerate(opened):
            final = index == len(opened) - 1
            if (self._body_read or not final) and spent(response, chunked_counts=final):
                POOL.give(key, connection)
            else:
                connection.close()

    def handlers(self, http_class: Any, https_class: Any, ssl_context: Any | None) -> list[HTTPHandler | HTTPSHandler]:
        session = self

        class KeptHTTPHandler(HTTPHandler):
            def http_open(self, req: Any) -> Any:
                return session.open(self, http_class, req)

        class KeptHTTPSHandler(HTTPSHandler):
            def https_open(self, req: Any) -> Any:
                if ssl_context is not None:
                    return session.open(self, https_class, req, context=ssl_context)
                return session.open(self, https_class, req)

        return [KeptHTTPHandler(), KeptHTTPSHandler(context=ssl_context)]


def kept_session(
    context: dict[str, Any],
    *,
    allow_private: bool,
    is_blocked: Callable[[Any], bool],
    ssl_context: Any | None,
) -> tuple[KeptSession, list[HTTPHandler | HTTPSHandler]]:
    tls = tuple(context.get(name) for name in ("verify_ssl", "ca_cert", "client_cert", "client_key"))
    session = KeptSession((not allow_private, tls))
    if allow_private:
        return session, session.handlers(http.client.HTTPConnection, http.client.HTTPSConnection, ssl_context)
    return session, session.handlers(*guarded_connections(is_blocked), ssl_context)
