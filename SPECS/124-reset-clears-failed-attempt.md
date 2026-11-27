# 124: a stage that succeeds after a restart shows no failure in its context

Ticket #124. Found by reading on 2026-11-26 in the audit after #118 and #119.

## EARS spec

- E1. When a stage is reset for a restart, a jump or a re-arm, the engine shall remove `error` from its
  context.
- E2. When a stage whose task is an `HTTPTask` is reset, the engine shall also remove `error_code`,
  `status_code`, `body_json`, `elapsed_ms`, `content_type`, `content_length` and the `response_*` keys.
- E3. The engine shall find a task's `reset_context_keys` when the stage names the task by its
  registered name, by its class name or by its class path, as it finds the task to run it.
- E4. While a stage has failed and has not been reset, its context shall be unchanged by this ticket.

No latency threshold applies.

## Cause

`TaskResult.terminal` and `failed_continue` write `error`. `TASK_RESET_KEYS` cleared `exception`, the
record of a raised error, and not `error`. `HTTPTask` declared no `reset_context_keys`.
`TaskRegistry.reset_context_keys_for` looked a task up by its exact name only, while
`TaskRegistry.get_by_class` also tries the class name and its lower case.

## Solution domain and alternatives

The reset key lists that already exist (codebase pattern).

- Add `error` to `TASK_RESET_KEYS` and list the response keys in `HTTPTask.reset_context_keys` [CHOSEN].
- Clear everything a failed result wrote, for every task [REJECTED: a task may leave state for its next
  run in the context of a failed result (#119, E6)].
- Leave `error` and document it [REJECTED: a SUCCEEDED stage would still read as failed to a consumer
  that checks for the key].

## Behaviour change

A task that read `error` on its next run to learn why the last one failed no longer finds it. The
changelog says so.

## Tests

`tests/http_task/test_http_failures.py::test_a_restarted_stage_that_succeeds_shows_nothing_of_its_failed_attempt`,
once with the task named by its registered name and once by its class path.
