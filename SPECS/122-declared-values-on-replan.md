# 122: a restarted HTTP or SSH stage uses the url, host and user it declared

Ticket #122. Found by reading the planner on 2026-11-26, in the audit of the HTTP task and the stage
context that followed #118 and #119. Reproduced on the published 0.35.0 wheel (pipeline 6658, job
31790). No consumer reported it. The same code is in 0.34.6 (since #27).

## EARS spec

- E1. When an HTTP stage that declared `url` is planned again and an ancestor outputs `url`, `HTTPTask`
  shall request the declared URL.
- E2. When an SSH stage that declared `host` or `user` is planned again and an ancestor outputs that
  key, `SSHTask` shall connect to the declared host as the declared user.
- E3. While a stage declares no `url`, `HTTPTask` shall request the `url` an ancestor outputs, on the
  first plan and on every later one.
- E4. When a task changes one of these keys after planning, in the stored context or in a copy of the
  stage, the task shall use the changed value (#118).
- E5. The planner shall be unchanged: on a later plan an ancestor's output replaces the stage's value
  for a shared key, which loop variables rely on.
- E6. If a stage's context carries no planning digest for the key (planned by 0.35.1 or earlier), then
  the task shall read the key from the context, as those releases did.

The planner computes at most 5 SHA-256 digests per stage plan (`url`, `headers`, `body`, `host`,
`user`), each only when an ancestor outputs the key.

## First fix, withdrawn

Commit fc5e186 changed the planner to keep every declared value on a later plan. Pipeline 6658 failed
`tests/test_structured_loops.py::test_nested_loops_run_the_inner_body_on_every_outer_pass` on both
backends and both Python versions (4 failed, 3195 passed on 3.14). A loop declares its variables with
an initial value and takes the ancestor's output from the second pass. The planner rule is by design
for them, and my reading that it was a planner defect was wrong. The planner change was reverted.

## Cause

On a later plan the planner replaces the stage's value for a key an ancestor also outputs. Every
`HTTPTask` outputs `url`, and `SSHTask` outputs `host` and `user`. `HTTPTask` read `url` and `SSHTask`
read `host` and `user` straight from the context. 0.35.0 gave `headers` and `body` a declared record
(#106) and left these three out.

## Technical problem

Telling a request key the stage declared from one an ancestor's output replaced on a later plan,
inside the task, without changing what the planner puts in the context.

## Solution domain

The planner's own record: `_declared_values` (0.35.0) holds the stage's value for a shared key and
`_planned_digests` (0.35.1) tells whether the context value changed after planning.

## Alternatives

- The task reads the key through `declared_or_current`: the declared value when the context is as the
  planner left it, the context value otherwise [CHOSEN]. One function, three call sites.
- Keep every declared value in the planner [REJECTED: breaks loop variables, shown by pipeline 6658].
- Stop `HTTPTask` and `SSHTask` writing `url`, `host` and `user` to their outputs [REJECTED: downstream
  stages and consumers read them].
- Return nothing for a key that only an ancestor supplied, as `own_value` does for `headers` [REJECTED
  for `url`: a stage that takes its URL from an upstream output is a normal workflow (E3)].

## Call sites

`declared_or_current` is new. Its callers are `tasks/http/task.py` (`url`) and `tasks/ssh.py` (`host`,
`user`). `DIGESTED_KEYS` grew from 2 to 5 keys. Its readers are `planned_digests` (planner) and nothing
else; `changed_since_planning` takes the key from its caller.

## Tests

`tests/test_declared_context_on_replan.py`, through `Orchestrator.restart`: two HTTP stages against a
local server, an SSH stage after a stage that outputs `host` and `user` with `subprocess.run` replaced,
and two controls (a key only the ancestor outputs, and an HTTP stage that declares no `url`).

## Not exercised

A real SSH connection. A loop-back or a jump into an HTTP stage that declared its `url`; they reach
the task through the same plan as a restart.
