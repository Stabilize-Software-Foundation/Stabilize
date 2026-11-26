# 126: a failure while HTTPTask reads the response is a result

Ticket #126. Found by reading on 2026-11-26 in the audit after #118 and #119, and checked against the
Python 3.14 standard library with a throwaway script before the tests were written.

## EARS spec

- E1. If no more of the body arrives within `timeout` seconds, then `HTTPTask` shall return a failed
  result with `error_code` `timeout` and shall not raise.
- E2. If the reply is not HTTP, then `HTTPTask` shall return a failed result with `error_code`
  `connection_failed`, shall not raise, and shall try again up to `retries` times.
- E3. If the body ends before its `Content-Length`, then `HTTPTask` shall return a failed result with
  `error_code` `connection_failed` whose text gives the bytes received and the bytes declared.
- E4. Where `continue_on_failure` is set, each of these results shall be FAILED_CONTINUE.
- E5. A complete body shall be returned as a success, as before.

## Cause

`tasks/http/response.py` caught `ValueError` around the read and nothing else. A `TimeoutError` from
the socket left `execute`. `tasks/http/task.py` caught `URLError`, `TimeoutError` and `OSError` around
the request, and `http.client.BadStatusLine` is none of them. `HTTPResponse.read(n)` returns the bytes
that arrived when the peer closes early and raises nothing, and the task took them for the body.

## Solution domain and alternatives

The standard library's own accounting: `HTTPResponse.length` holds the bytes still expected and is 0
for a HEAD reply and `None` for a chunked one.

- Catch the transport exceptions at both places and compare `length` after the read [CHOSEN].
- Compare the `Content-Length` header with the bytes read [REJECTED: wrong for HEAD, 204 and 304,
  which declare a length and carry no body].
- Leave the truncated body and add a flag to the outputs [REJECTED: a success with half a body is the
  defect].

## Not exercised

A truncated chunked body, a TLS error in the middle of a body, and an error writing the `download_to`
file. The first raises `http.client.IncompleteRead`, which the new handler catches by its base class.
The other two leave `execute` as exceptions, as before.
