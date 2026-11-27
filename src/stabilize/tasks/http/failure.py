from __future__ import annotations

from typing import Any
from urllib.error import URLError

from stabilize.tasks.http.guarded_opener import BlockedPeerError
from stabilize.tasks.result import TaskResult

ERROR_CODE = "error_code"

INVALID_REQUEST = "invalid_request"
BLOCKED_ADDRESS = "blocked_address"
TIMEOUT = "timeout"
CONNECTION_FAILED = "connection_failed"
RESPONSE_TOO_LARGE = "response_too_large"
HTTP_STATUS = "http_status"
NO_RESPONSE = "no_response"

ERROR_CODES = frozenset(
    {INVALID_REQUEST, BLOCKED_ADDRESS, TIMEOUT, CONNECTION_FAILED, RESPONSE_TOO_LARGE, HTTP_STATUS, NO_RESPONSE}
)


class ResponseTooLargeError(ValueError):
    pass


class TruncatedBodyError(ConnectionError):
    pass


def require_complete(response: Any, received: int) -> None:
    missing = getattr(response, "length", None)
    if isinstance(missing, int) and not isinstance(missing, bool) and missing > 0:
        raise TruncatedBodyError(f"response body ended after {received} of {received + missing} bytes")


def refused(error: BaseException) -> bool:
    if isinstance(error, URLError) and isinstance(error.reason, BaseException):
        return refused(error.reason)
    return isinstance(error, (BlockedPeerError, ValueError))


def transport_code(error: BaseException) -> str:
    if refused(error):
        return BLOCKED_ADDRESS
    reason = error.reason if isinstance(error, URLError) else error
    if isinstance(reason, TimeoutError):
        return TIMEOUT
    return CONNECTION_FAILED


def failure(
    error: str,
    code: str,
    *,
    continue_on_failure: bool = False,
    outputs: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
) -> TaskResult:
    if continue_on_failure:
        return TaskResult.failed_continue(error=error, outputs=outputs, context={ERROR_CODE: code})
    return TaskResult.terminal(error=error, context={**(context or {}), ERROR_CODE: code})
