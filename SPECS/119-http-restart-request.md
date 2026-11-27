# 119: a restarted HTTP stage sends the request it declared

Ticket #119. Found by reading the code on 2026-11-26 during the review of #118. No consumer reported
it. The same code path is in 0.34.6.

## EARS spec

- E1. When a task result's context replaces a key that the task class lists in `request_context_keys`,
  the engine shall record the replaced value, or its absence, and a SHA-256 digest of the written value
  under the engine key `_displaced_request`, in the transaction that saves the result.
- E2. When a stage is reset for a restart, a jump or a multi-merge re-arm, the engine shall put each
  recorded key back to the value it had before the result replaced it, and shall remove a key that was
  absent before.
- E3. If the value of a recorded key no longer matches the digest of what the result wrote, then the
  engine shall leave that key as it is at the reset.
- E4. The engine shall remove `_displaced_request` from the context at every reset.
- E5. While a failed HTTP stage has not been reset, its context shall show `status_code`, `headers`,
  `body`, `url` and the `response_*` names as 0.35.1 does.
- E6. While a task class has an empty `request_context_keys`, the engine shall record nothing for its
  results and a reset shall keep the context its failed result left, as 0.35.1 does.
- E7. `HTTPTask.request_context_keys` shall be `url`, `headers` and `body`. These are the three keys
  that `HTTPTask` both reads its request from and writes its response to.
- E8. If a stage failed under 0.35.1 or earlier and carries no record, then a reset under this release
  shall behave as it did under that release.

No latency or throughput threshold applies. The cost is one SHA-256 digest for each request key that a
result's context writes, and nothing for a result with an empty context or a task with no request keys.

## Cause

`tasks/http/response.py` returns `TaskResult.terminal(error=..., context=outputs)` for a status the
stage does not accept. `outputs` holds the response under `url`, `headers` and `body`, and
`handlers/run_task/result.py` merges a result's context into the stage's context. `HTTPTask` declares
no `reset_context_keys`, and `reset_stage_for_retry` kept the three keys. The planner writes
`_declared_values` only for keys an ancestor output, and for any other key `own_value` returns the
context value. A restarted stage therefore built its request from the failed response.

Three effects, each with a test: the response's headers and body were sent, the declared headers and
body were lost, and the request went to the final URL of a redirect.

## Technical problems

1. Keeping the value a request key had before a failed result replaced it, from the stored stage and
   not from what the task saw at execute time. A task may run on a copy of the stage that carries a
   secret (#118), and that copy must not be persisted.
2. Putting the value back at a reset without undoing a change made on purpose after the failure.
3. Leaving every other task's context alone, because a task may leave state for its next run in the
   context of a failed result.

## Solution domains

- Undo record (before-image) kept beside the data it describes: the stage's own context, under an
  engine key listed in `models/stage/engine_keys.py` (codebase pattern, as `_declared_values`).
- Change detection by content digest: `models/stage/planned_digests.py::digest`, added for #118, which
  follows the stores' JSON encoding.
- Per-task-class declaration of context keys: `Task.reset_context_keys` (codebase pattern).

## Alternatives

- Record at the result save and restore at the reset, for keys the task class lists [CHOSEN]. The save
  path holds the stored stage, the reset path is shared by restart, jump and re-arm, and a jump applies
  its `jump_context` after the reset.
- The planner snapshots the declared request keys on the first plan and restores them on every later
  plan [REJECTED: the planner cannot tell a failed response from a value a jump or an operator set for
  the next run, and would overwrite it].
- The failed result stops writing `url`, `headers` and `body` and writes only the `response_*` names
  [REJECTED: consumers read a failed stage's error body from `body`, and the old names are promised
  until 2.0.0].
- `HTTPTask` keeps its own copy of the request values in the context [REJECTED: the task sees
  execute-time values and would persist a header a subclass set on a copy of the stage].
- Restore every key any failed result replaced, for every task [REJECTED: fails E6; a task that leaves
  a cursor for its next run would lose it].

## Call sites of the changed contract

`reset_tasks` now restores recorded request keys before it clears the reset keys.

- `handlers/jump_to_stage/reset.py::reset_stage_for_retry`: the only wrapper. Callers below.
- `handlers/workflow_control.py` RestartStageHandler, the stage and its kept synthetic children: the
  case this ticket fixes.
- `handlers/jump_to_stage/target.py::apply_jump_to_target`: resets, then applies `jump_context`. A
  value the jump sets wins.
- `handlers/jump_to_stage/handler.py` (`self._reset`): the stages a jump passes over. Same effect as
  a restart.
- `handlers/complete_stage/support.py::_rearm_multi_merge`: a re-armed stage succeeded, and a
  successful `HTTPTask` result writes no context. A record can exist only from an earlier failed
  firing, and putting the request back is correct there too.

## Tests

`tests/http_task/test_http_restart_request.py`, through `Orchestrator.start`, `process_all`,
`Orchestrator.restart` and `process_all`, against a local server that records each request. Six tests
are red on 0.35.0 and three are controls. Each has a stored mutant under `audit/redproof/manual`.

The jump test takes the stage a real failed run left in the store, applies `apply_jump_to_target` to
it and builds the request. It does not run a jump through the queue.

## Not exercised

A jump through the queue into a failed HTTP stage, and a multi-merge re-arm of an HTTP stage. Both use
the reset path the restart tests run.
