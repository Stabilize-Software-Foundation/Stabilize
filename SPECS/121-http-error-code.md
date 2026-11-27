# 121: an error code beside HTTPTask's error text

Ticket #121. Asked for by rodmena-webmail-v2-c53564 on 2026-11-26 (AgentBus thread
01M887C2Y0HHC7BDFTW3TDPYTJ). Webmail told a blocked address from a body over the cap by matching the
text in `context["error"]`.

## EARS spec

- E1. When an `HTTPTask` request fails, the result's context shall hold `error_code` beside `error`,
  for a TERMINAL result and for a FAILED_CONTINUE result.
- E2. `error_code` shall be one of `invalid_request`, `blocked_address`, `timeout`, `connection_failed`,
  `response_too_large`, `http_status`, `no_response`, and the set shall be importable as
  `stabilize.tasks.http.failure.ERROR_CODES`.
- E3. The texts under `error` shall be unchanged by this ticket.
- E4. A successful result shall carry no `error_code`, in context or in outputs.
- E5. When a stage is reset, the engine shall remove `error_code` from its context (#124).

No latency threshold applies.

## Technical problem

Classifying a failure at the place it is detected, where the exception type and the raise site are
known, and carrying the class to the caller in a field that does not change with the wording.

## Solution domain

Error taxonomy on a result: a closed set of string codes beside a free text, as HTTP problem types and
SQLSTATE classes do. The code is chosen from the raise site and the exception type, never from the text.

## Alternatives

- A string code in the result's context under `error_code`, set by one helper that every failure path
  of the task calls [CHOSEN]. It sits where `error` already is, which is where Webmail reads.
- Exception classes exposed to the caller [REJECTED: the task returns results and does not raise].
- A code inside the text, as a prefix [REJECTED: it changes every text, and callers match on them today].
- A field on `TaskResult` [REJECTED: a change to every task and to the stored result for one task's
  need; `context` is where a failed result already puts its description].

## Tests

`tests/http_task/test_http_failures.py`: one test per code that a local server or a refused address
can produce, each red on 0.35.0, with a stored mutant each. `no_response` has no test. It is the
branch for a request that ended with neither a response nor an error, which no input I know reaches.
