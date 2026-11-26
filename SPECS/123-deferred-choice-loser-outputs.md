# 123: a deferred-choice member that finishes and then loses passes nothing downstream

Ticket #123. Seen once in CI on 2026-11-26 (pipeline 6651, job 31738,
`tests/test_deferred_choice.py::test_a_loop_over_a_deferred_choice_is_decided_afresh` on PostgreSQL)
and traced by reading. No consumer reported it.

## EARS spec

- E1. When a member of a deferred choice that settles on completion loses at its own completion, the
  engine shall store it SKIPPED with empty outputs.
- E2. When a stage is skipped as pruned, the engine shall store it with empty outputs.
- E3. The stage after the choice shall receive the winner's outputs and none of a loser's.
- E4. The winner's outputs shall be stored and passed on as before.
- E5. A failed loser shall keep its failure reason in its context, as before.

No latency threshold applies.

## Cause

A task's outputs are written to its stage when the result is saved. The choice is settled two messages
later, at CompleteStage. A member whose result was saved and which then found another owner took the
loser branch of `handlers/complete_stage/deferred.py` with its outputs in place. `handlers/skip_stage.py`
pruned a running sibling whose task had already finished the same way. The ancestor merge takes every
ancestor's outputs whatever its status and breaks a tie by `ref_id`.

## Technical problem

The stored state of a loser is decided in the settlement transaction, after its outputs were written.

## Solution domain

The transaction that marks the stage pruned owns its stored state (codebase pattern: the prune already
cancels the stage's live tasks there).

## Alternatives

- Clear the outputs where the stage is marked pruned, in both paths [CHOSEN].
- Leave the outputs and make the ancestor merge skip pruned stages [REJECTED as the only fix: a reader
  of the stage, the event log and `_jump_outputs` would still see a loser's outputs; it also adds a
  status read to a query both backends share].
- Hold a member's outputs back until the choice is settled [REJECTED: changes where every task result
  is written, for one control-flow pattern].

## Tests

`tests/test_deferred_choice_loser_outputs.py` on SQLite: two tests red on 0.35.0 and a control. The
second test steps the queue until one member has won, then applies the prune to the other, which has
finished its task and is still RUNNING.

## Not exercised

The same race under threads on PostgreSQL, other than through the existing loop test that failed once.
