# 122: a stage planned again keeps the values it declared

Ticket #122. Found by reading the planner on 2026-11-26, in the audit of the HTTP task and the stage
context that followed #118 and #119. No consumer reported it. The same code is in 0.34.6 (since #27).

## EARS spec

- E1. When a stage is planned again after a restart, a jump or a loop, the planner shall keep in the
  stage's context the value the stage declared for a key an ancestor also outputs, as on the first plan.
- E2. When a stage is planned again, the planner shall replace a value it copied from an ancestor on an
  earlier plan with the ancestor's current output (#27, unchanged).
- E3. When a jump sets a key in `jump_context`, the planner shall treat that value as the stage's own
  (unchanged).
- E4. If a stage's context carries `_hydrated_keys` and no `_declared_values` (planned by 0.34.6 or
  earlier), then the planner shall treat every hydrated key as a copy, as those releases did.
- E5. While a list is declared for a key an ancestor also outputs as a list, the planner shall merge
  the declared list into the ancestor's on every plan, as on the first plan.

No latency threshold applies. The change adds no statement and no digest.

## Cause

`handlers/start_stage/planner.py` writes `_hydrated_keys` with every ancestor key, the ones where the
stage's own value won included. On the next plan a previously hydrated key that an ancestor still
outputs was skipped as a stale copy, and the ancestor's output replaced the declared value.
`docs/guide/data_flow.rst` says the opposite: "Keys the caller set directly on the stage still take
precedence." Every `HTTPTask` outputs `url`, and `HTTPTask` reads `url` straight from the context.

## Technical problem

Telling, on a later plan, a value the stage declared from a value the planner copied.

## Solution domain

Provenance of a context value, recorded by the planner. `_declared_values` (0.35.0, #106) already holds
the stage's own value for every key an ancestor also outputs.

## Alternatives

- On a later plan take the stage's value from `_declared_values` for a hydrated key that is in it, and
  drop only the keys that are not [CHOSEN]. No new engine key, and E4 falls out of the missing record.
- List only copied keys in `_hydrated_keys` [REJECTED: `own_value` in 0.35.x reads `_hydrated_keys` to
  decide that an undeclared header is not the stage's, and a mixed fleet would read the two meanings].
- Make `HTTPTask` read `url` through `own_value` [REJECTED: fixes one key of one task; E1 is a planner
  rule for every task].

## Existing test changed

`tests/http_task/test_http_upstream_keys.py::test_a_stage_planned_again_still_sends_the_headers_it_declared`
asserted that the context holds the ancestor's headers after a second plan. It now asserts the declared
headers. The request it checks is unchanged.

## Tests

`tests/test_declared_context_on_replan.py`: two tests red on 0.35.0 and a control, through
`Orchestrator.restart`. One runs two HTTP stages against a local server and checks the paths requested.

## Not exercised

A loop-back and a jump through the queue with a declared key. They reach the planner the same way a
restart does.
