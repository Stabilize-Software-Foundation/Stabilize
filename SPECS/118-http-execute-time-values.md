# 118: a header or body a task sets in execute() is sent

Ticket #118. Regression in 0.35.0 from #106, reported by provenance-50ca06 on 2026-11-26 at 13:17Z
(AgentBus thread 01M8C01D066CRMDPTNQ85MYC4F) and reproduced on the PyPI wheel.

## EARS spec

- E1. When a task changes `headers` or `body` in a stage's context after the stage was planned, HTTPTask
  shall send the changed value, whether or not an ancestor output a key of that name.
- E2. While `headers` or `body` in a planned stage's context is unchanged since planning and only an
  ancestor supplied it, HTTPTask shall not send it (#106, as 0.35.0).
- E3. While `headers` or `body` is unchanged since planning and the stage declared a value for it,
  HTTPTask shall send the declared value (as 0.35.0).
- E4. If a context carries the 0.35.0 record (`_declared_values`) and no planning digest, then HTTPTask
  shall treat it as 0.35.0 does.
- E5. The planner shall compute a digest only for `headers` and `body`, and only when an ancestor output
  that key: at most 2 SHA-256 digests per stage plan.
- E6. A value a task set on a copy of the stage at execute time shall not be persisted by this change.

## Cause

`tasks/http/request.py::own_value`, added for #106, returned None for a key the planner had hydrated and
the stage had not declared. It could not tell a value the planner copied from an ancestor from one a
task wrote afterwards. The 0.35.0 tests covered a stage that declares its headers. None covered a task
that sets them in `execute()`.

## Technical problem

Telling, at execute time and from the context alone, whether a value is the one the planner left or one
written after planning. The context alone, because a task may hand `HTTPTask.execute` a copy of the
stage.

## Solution domain

Content digests: SHA-256 over canonical JSON (`sort_keys`), recorded at planning under an engine context
key registered in `models/stage/engine_keys.py`.

## Alternatives

- Digest of the planned value for `headers` and `body`, compared at execute time. CHOSEN: it needs only
  the context and costs 2 digests per plan.
- Recompute the ancestor outputs in `HTTPTask.execute` and compare. REJECTED: a store read per HTTP task,
  and a copy of a stage need not carry its workflow.
- A hook method on HTTPTask for execute-time headers. REJECTED as the fix: code written for 0.34 that
  sets context keys would stay broken. It can be added later.
- Treat a 0.35.0-planned context without a digest as 0.34.6 did. REJECTED: it reopens #106 for stages in
  flight and changes a 0.35.0 test.

## Call sites of the changed contract

`own_value(` is called three times, all in `tasks/http/request.py::build_request` (headers once, body
twice). `_plan_stage` is the only writer of `_hydrated_keys`, `_declared_values` and `_planned_digests`;
`handlers/jump_to_stage/target.py` removes jumped keys from `_hydrated_keys`, after which `own_value`
returns the context value as before.

## Tests

`tests/http_task/test_http_execute_time_values.py`, seven tests. Five are listed in
`audit/evaluations/expect/0.35.0.tests` and must fail on published 0.35.0 for their marker. Seven stored
mutants under `audit/redproof/manual`, and the two #106 mutants regenerated for the moved lines.

The digest follows the stores' own serialisation (`json.dumps(..., default=str)`) and treats 1.0 and 1
alike, because PostgreSQL returns a number in its own notation. A terminal HTTP failure writes its
response into the stage context (`tasks/http/response.py`). A stage planned again takes the ancestor's
value for those keys and a fresh digest, and the seventh test holds that the failed response is not sent.
