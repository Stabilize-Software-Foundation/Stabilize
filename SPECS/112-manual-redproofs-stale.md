# 112: manual red-proofs, 9 stored diffs no longer apply and one PostgreSQL mutant does not fail

Ticket: #112. Found 2026-11-24 in CI pipeline 6314, job 28865, while releasing 0.34.6 (#111).

## EARS spec

- The manual red-proofs job shall exit 0 on main: each of the 50 diffs under `audit/redproof/manual` applies to the tree it is run against, makes its named test fail in the call phase, and the unmutated tree passes that test.
- If a source change moves the lines a stored diff targets, then the change that moves them shall update the diff in the same commit.

## What was observed

Job 28865 was the first manual red-proofs run on CI with the PostgreSQL tests reachable. 40 of 50 proofs passed.

`git apply --check` on a clean main `87d258f` rejects 9 diffs, the same 9 the job reported, so they were stale before #111:

| Test | Target the diff no longer matches |
|---|---|
| `tests/test_config_versioning.py::TestWorkflowConfigVersion::test_workflow_has_config_version_field` | `src/stabilize/models/workflow.py:142` |
| `tests/test_critical_corner_cases.py::TestBugDocumentation::test_bug_max_jumps_zero_treated_as_falsy` | `src/stabilize/handlers/jump_to_stage/handler.py:338` |
| `tests/test_mg_up_handoff.py::test_a_failed_migration_in_migrettis_history_stops_mg_up` | `src/stabilize/cli/migrate.py:235` |
| `tests/test_mg_up_handoff.py::test_mg_up_applies_every_migration_through_migretti` | `src/stabilize/cli/migrate.py:232` |
| `tests/test_mg_up_history_schema.py::test_a_history_schema_equal_to_the_target_is_refused` | `src/stabilize/cli/migrate.py:235` |
| `tests/test_mg_up_history_schema.py::test_moving_history_that_already_lives_in_the_target_is_refused` | `src/stabilize/cli/migrate.py:235` |
| `tests/test_purge_query_plan.py::test_purge_and_recovery_lookups_use_an_index[sqlite]` | `src/stabilize/persistence/sqlite/migrations.py:126` |
| `tests/test_schema_and_exists.py::TestExistsDistinguishesAbsenceFromBreakage::test_absent_error_tuple_covers_both_spellings` | `src/stabilize/persistence/store/interface.py:25` |
| `tests/test_timeout_enforcement.py::TestGetTaskTimeout::test_retryable_task_uses_dynamic_timeout` | `src/stabilize/handlers/run_task/handler.py:253` |

The tenth, `tests/test_purge_query_plan.py::test_purge_and_recovery_lookups_use_an_index[postgres]`, applies and its mutant passes (pytest exit 0). It had never run on CI before the cluster-name step added under #111.

## Synthesis

Technical problem: a stored mutant diff goes stale when its target lines move, and the job that would report it had been failing for another reason (the cluster guard).

Solution domain: mutation testing. Concept: one stored mutant diff per test under `audit/redproof/manual`, run by `audit/redproof/manual_proofs.py`. No new component.

Alternatives:

- Regenerate each diff against current source and confirm red in CI [CHOSEN].
- Match mutants by pattern instead of by line context [REJECTED: a second mutation mechanism beside `mutants.py`, for ten files].
- Add the ten tests to `unproven_allowed.txt` [REJECTED: it records that nobody knows whether these tests can fail].

## Scope

Not part of #111 and not a gate on the 0.34.6 wheel. The job measures whether tests can fail, and the product jobs are separate.
