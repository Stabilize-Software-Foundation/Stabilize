# 127: HTTPTask keeps connections to a host between requests

Ticket #127. Asked for by rodmena-webmail-v2-c53564 on 2026-11-26 (AgentBus thread
01M887C2Y0HHC7BDFTW3TDPYTJ, message 01M8C9XRP9K8NXYCEBRHA9D77F): 12 requests through `HTTPTask` took
80 ms at the median against its mail server, and 12 on one kept httpx connection took 3 ms. The
director put it in this release on 2026-11-27.

## EARS spec

- E1. Where a stage sets `keep_alive` to true, `HTTPTask` shall send the request on an idle kept
  connection to the same scheme, host and port, with the same address check and TLS settings, when the
  process holds one, and shall keep the connection once the response body has been read to its end.
- E2. While `keep_alive` is not set, `HTTPTask` shall open and close a connection for each request, as
  0.35.1 does.
- E3. `HTTPTask` shall keep a connection only when the response was read to its end, the server did not
  ask to close it, and the length of the body is accounted for: `Content-Length` reached, or for the
  final response the last chunk read without error. A response over `max_response_size`, a truncated
  body, a timeout or any error shall close the connection.
- E4. Before it reuses a kept connection `HTTPTask` shall check that the peer has not closed it and has
  sent nothing, and shall close it and open a new one otherwise.
- E5. If a reused connection fails before any response arrives and the method is GET, HEAD, PUT, DELETE
  or OPTIONS with no body or a body of bytes, then `HTTPTask` shall send the request once more on a new
  connection. For POST and PATCH it shall report the failure.
- E6. The connect-time address check shall run for every new connection, and a connection made with
  `allow_private_urls` shall never serve a request made without it.
- E7. The pool shall hold at most 8 idle connections for a key and shall not reuse a connection that
  has been idle for more than 30 s.
- E8. A process forked from one that holds kept connections shall not use them.
- E9. While a proxy applies to the request, `HTTPTask` shall send it as it does without `keep_alive`.
- E10. Two requests in different threads shall never use one connection at the same time.

Quantified in CI: against a local HTTP/1.1 server, 5 requests in a row with `keep_alive` open exactly 1
connection and 5 without it open 5. The latency gain is Webmail's to measure against its TLS server.

## Technical problems

1. Reusing a connection only when nothing of the last response is left on it, so that one request can
   never read another's bytes.
2. Knowing a kept connection is dead before sending on it, and not sending again a request the server
   may have acted on.
3. Keeping the SSRF check tied to the connection across reuse.
4. Sharing connections between threads without two requests on one socket, and not across a fork.
5. Leaving the default path as it is.

## Solution domains

- HTTP/1.1 persistent connections (RFC 9112, section 9.3).
- `http.client` from the standard library: an `HTTPConnection` carries one request after another, and
  `HTTPResponse.isclosed()`, `length`, `will_close` and `chunked` say how a body ended. Checked with a
  throwaway script on Python 3.14.4 before the code was written.
- urllib's handler chain, which `HTTPTask` already uses: redirects, `HTTPError` and proxies stay with
  urllib, and only the handler that opens the connection changes.
- A pool with a liveness check on checkout: an idle socket that is readable has been closed by the peer
  or holds bytes nobody asked for (the check urllib3 makes).

## Alternatives

- A urllib handler that takes its connection from a process-wide pool and gives it back when the
  response is done [CHOSEN]. The guarded connection classes are reused, so the address check is the
  same code.
- A new client on `http.client` alone [REJECTED: redirects, their SSRF check, `HTTPError` and proxy
  handling would be written again].
- httpx or urllib3 as a dependency [REJECTED: `HTTPTask` is standard library only, and the estate takes
  stabilize's dependencies transitively].
- On by default [REJECTED: it changes every consumer's behaviour, and a server that closes idle
  connections without saying so would become an occasional failure for a POST].
- Give a redirect's connection back at once so its target can use it [REJECTED: urllib reads and closes
  the redirect response after the handler has returned, and the handler cannot see that happen].

## Where it is

`tasks/http/kept.py` (pool, session, handlers), `tasks/http/guarded_opener.py::guarded_connections`
(the classes the guarded handlers already used, now shared), 14 lines in `tasks/http/task.py` and a
callback in `tasks/http/response.py` that marks the body as read.

`HTTPTask.execute` now wraps `_execute` to settle the session's connections on every exit.

## Tests

`tests/http_task/test_http_keep_alive.py`: 18 tests against a local HTTP/1.1 server that counts the
connections it accepts. Six are red on 0.35.0, which ignores `keep_alive`. Each has a stored mutant.

## Not exercised

HTTPS: CI has no TLS server, so the handshake saving, the TLS settings in the pool key and the check
for bytes pending inside the TLS layer have not run. A real fork (the test changes the process id the
pool sees). A proxy (the test checks that the request is handed to urllib's own opener). More than six
threads.
