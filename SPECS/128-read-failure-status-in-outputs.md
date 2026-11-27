# 128: the task guide says what a request that fails during the body read reports

Ticket #128. Reported by Webmail (rodmena-webmail-v2-c53564) on 2026-11-27, bus message
`01M8E9XHHWJFFSFW7M1GPYF6CK`, after it tested 0.36.0 against a raw listener. Its wrapper read
`status_code` before anything else and would have taken a short body for an answer of status 200.

## EARS spec

- E1. The task guide (`docs/guide/tasks.rst`, "Failed requests") shall state that when a request fails
  while the body is read (a timeout, a body shorter than its `Content-Length`, a body over
  `max_response_size`) and `continue_on_failure` is set, the failed result's outputs hold the status
  the server sent under `status_code` and `response_status`, which can be 200, and `url`, and hold no
  `body`.
- E2. The guide shall state that without `continue_on_failure` the TERMINAL result has no outputs for
  these cases.
- E3. The guide shall tell a caller to read the result's status or `error_code` before `status_code`.
- E4. `HTTPTask`'s behaviour shall be unchanged by this ticket.

No quality threshold applies. The change is documentation.

## Where the facts come from

`tasks/http/response.py`: the branch that catches a failure of the body read returns
`failure(..., outputs={"status_code", "response_status", "url"})`. `tasks/http/failure.py`:
`failed_continue` carries the outputs and `terminal` does not. Webmail observed the same on the
published 0.36.0 for a stalled body and for a short body.

## Solution domain and alternatives

The task guide.

- Document it [CHOSEN].
- Drop `status_code` from the outputs of a read failure [REJECTED: the server did send that status and
  a caller may log it, and removing an output key is a behaviour change that needs a release].

## Not exercised

No test of ours asserts the outputs of a read failure. The statement rests on the code named above
and on Webmail's run.
