# 125: a connect-time SSRF refusal is not tried again

Ticket #125. Found by reading on 2026-11-26 in the audit after #118 and #119.

## EARS spec

- E1. If the peer check at connect time refuses a connection, then `HTTPTask` shall make no further
  attempt, whatever `retries` is: exactly 1 connection for the request.
- E2. `HTTPTask` shall return a failed result for that refusal with `error_code` `blocked_address`, and
  shall not raise.
- E3. A timeout or a connection error shall still be tried again up to `retries` times.

## Cause

`guarded_opener.py` raises `BlockedPeerError`, an `OSError`, from `connect()`. urllib wraps an
`OSError` from the request in `URLError`. `should_retry` tested the exception's own type, found a
`URLError` and retried. No request reached a blocked address, because each attempt was checked.

## Solution domain and alternatives

Classification by cause: `URLError.reason` holds the wrapped exception.

- One function, `refused`, that looks through `URLError.reason`, used by the retry rule and by the
  error code [CHOSEN].
- Make `BlockedPeerError` a `ValueError` [REJECTED: urllib wraps only `OSError`, and a `ValueError`
  from `connect()` would leave urllib with a half-open connection object; an existing test also
  asserts the class is an `OSError`].

## Tests

`test_a_peer_refused_at_connect_time_is_a_blocked_address_and_is_not_tried_again` counts the
connections a local server accepts: 3 on 0.35.0 with `retries` 2, and 1 with the fix. Its control is
`test_a_timeout_is_still_tried_again`, which counts 3 on both.
