# 111: events inside their transaction, one completion per workflow, constant-cost dedup check

Ticket #111. Target release 0.34.6. Reported by Webmail v2 (rodmena-webmail-v2-c53564, AgentBus thread
01M85TXENQDS1Z7YKV1Y1QVD90, their ticket webmail-apis #8) against 0.34.5 on 2026-11-24.

## EARS spec (ticket and `SPECS/111-events-in-transaction.md`)

- E1. The engine shall append every lifecycle event (`workflow.created/started/completed/failed/canceled`, `stage.started/skipped/canceled`, `task.started`) inside the store transaction that writes the state it describes, as the last statement of that transaction.
- E2. When a handler's transaction enqueues a successor message, the engine shall have appended that handler's lifecycle event before any other connection can poll the successor.
- E3. If a lifecycle event cannot be appended, then the engine shall not commit the state change or the successor message; a redelivery shall record the event once and proceed.
- E4. If a handler's transaction rolls back, then on a same-database event store no event of that attempt shall remain and none shall reach the bus.
- E5. When two or more `CompleteWorkflow` messages for one workflow are handled at the same time, the engine shall complete it once: exactly 1 completion event, 1 workflow `status.changed`, 1 set of successor messages, and the winner's `end_time`.
- E6. Every write of a workflow's status shall state the status it expects to replace; if the row holds another status, then the write shall change nothing and raise `ConcurrencyError`.
- E7. When a workflow reaches CANCELED or TERMINAL by any path, the engine shall record exactly 1 `workflow.canceled` or `workflow.failed` in the transaction that writes that status. (Kept if C9 reproduces.)
- E8. While a message is being handled, if an event with a source handler is appended with no store transaction open on that thread, or a held event is dropped, then the processor shall record a delivery defect naming the event type, log it at ERROR and count it in `stabilize.delivery.defects`.
- E9. When replay meets an event carrying `ref_id`, `type`, `name` or `stage_id` for an entity whose entry lacks it, the replayer shall fill it, whatever the event order.
- E10. `BloomDeduplicator.fill_ratio` shall equal set bits / size exactly after any sequence of `mark_seen`, `hydrate` and `reset`; its per-call time on a 2,000,000-item filter shall be within 20x of a 1,000-item filter (0.34.5: about 2,000x).
- E11. `examples/benchmark-overhead.py` shall exit 0, keep its databases in a temporary directory of its own, and be run by `tests/test_examples_run.py`.
- E12. No schema change, no migration, no change to any event payload or public name.

## Technical problems

1. Event and state atomicity: an event that describes a state change must commit with it and before any
   successor message is visible.
2. Lost update on the workflow row: several handlers read the same status, decide, and write.
3. A per-message cost in the processor that grows with a data structure's size.
4. Replay that depends on event order for an entity's identity.
5. A defect class that is fixed site by site returns at the next site; each class needs a check that fails.

## Solution domains

- Transactional event recording -> the codebase's own scope (`events/txn_scope.py`), as used by CompleteStage
  since audit finding A1-7; `docs/guide/event_sourcing.rst` states the contract.
- Optimistic compare-and-set -> `txn.store_stage(stage, expected_phase=...)` and `pause_execution` in this
  codebase; PostgreSQL READ COMMITTED re-checks the WHERE clause after a row lock is released.
- Run-time contract checks -> the delivery contract in `persistence/committed_marks.py`.
- Counting bloom fill -> an incremental population count maintained at the single writer.

## Claim inventory, ranked by customer harm

CONFIRMED = reproduced live here. SUSPECTED = read from code, not yet run.

| # | Claim | Status | Evidence |
|---|---|---|---|
| C1 | Concurrent `CompleteWorkflow` handlers each complete the workflow: N `workflow.completed` events and bus publications, N `status.changed`, `end_time` overwritten, N `StartWaitingWorkflows` / `CancelStage` pushes | CONFIRMED on SQLite (8 and 9 events; 1 with fsync off, so timing-dependent). SUSPECTED on PostgreSQL | `handlers/complete_workflow.py:81-181`; no guard in `sqlite/transaction.py:204` or `postgres/transaction.py:112` |
| C2 | Lifecycle events recorded outside the transaction that writes their state. After commit: `StartStage` (`start_stage/starting.py:353`), `StartTask` (`start_task.py:158`), `StartWorkflow` (`start_workflow.py:177`), `StartWaitingWorkflows` (`start_waiting_workflows.py:84`), `CancelStage` (`cancel_stage.py:154`). Before it opens: `SkipStage` (`skip_stage.py:123`), `CompleteWorkflow` (`complete_workflow.py:118`) | Order violation CONFIRMED on SQLite (three runs of three). Lost event on a crash or failed append after commit: SUSPECTED. Phantom or duplicate on a failed pre-transaction recorder: SUSPECTED. PostgreSQL: SUSPECTED | Already inside: `CompleteTask`, `CompleteStage`, `JumpToStage`, `SignalStage`, `ResumeStage`, `Orchestrator.unpause`, `store.pause/resume`, `RunTask` |
| C9 | A workflow that ends without reaching `CompleteWorkflow` records no `workflow.canceled` / `workflow.failed`: cancelled before start (`start_workflow.py:197-202`), no initial stages (`start_workflow.py:161-166`, `admission.py:108-113`), purged or expired while waiting (`admission.py:100-105`) | SUSPECTED | the docs list both events as "Workflow failed" / "Workflow was canceled" with no exception |
| C3 | `fill_ratio` walks the whole bit array in Python, under the filter's lock, once per message | CONFIRMED: 179,720 bytes, 4.0 ms per `should_reset()`. Webmail's throughput figures not re-measured | `queue/dedup.py:266`, from `queue/processor/mixins.py:176` |
| C4 | Replay takes a stage's `ref_id`/`type`/`name` and a task's `name`/`stage_id` only from the entity's first event | CONFIRMED ("Missing http_0 in replay") | `events/replay.py:276-282`, `319-324` |
| C8 | The other ten `update_workflow_status` callers read, decide and write with no re-check: start, buffer, promote, terminate, restart, resume | SUSPECTED, none run | `admission.py:57,103,113`, `start_workflow.py:145,165,201`, `orchestrator.py:214,237`, `workflow_control.py:228,314` |
| C10 | Events held for "the next transaction" are dropped with a warning when the handler never opens one | SUSPECTED; frequency unknown | `queue/processor/mixins.py:191-197` |
| C5 | CI never ran the benchmark; the examples test calls it "a module of timing helpers" | CONFIRMED by reading | `tests/test_examples_run.py:30` |
| C6 | The benchmark writes to fixed `/tmp/stabilize_bench*.db` paths | CONFIRMED by reading | `examples/benchmark-overhead.py:168-169` |
| C7 | Benchmark phase 3 takes 2.8 s with fsync off (4.7 s on, Webmail 24 s) | OPEN: fsync 1.9 s, `fill_ratio` about 0.7 s, the rest unprofiled | `/tmp/claude-repro-111/` |

Harm order: C1 > C2 > C9 > C3 > C4 > C8 > C10 > C5, C6.

## Design

### W1. Workflow status is always compare-and-set (C1, C8)

Concept: optimistic compare-and-set on the row, as `txn.store_stage(stage, expected_phase=...)`
does for stages and `pause_execution` does for pausing.

- `persistence/store/transaction.py`, `sqlite/transaction.py`, `postgres/transaction.py`,
  `store/noop_transaction.py`: `update_workflow_status(workflow, *, expected_status: str)`,
  required. The UPDATE adds `AND status = :expected`; no row matched raises `ConcurrencyError`.
  `NoOpTransaction` accepts and ignores it (documented as non-atomic).
- `handlers/base.py`: `claim_workflow_status(txn, workflow, new_status)`: `validate_transition`,
  assign, guarded write with the old status, then `record_status_transition`. The event comes
  after the write wins, so a loser records nothing even with a separate event database.
- All eleven callers go through it or pass the status they read:

| Site | Expected |
|---|---|
| `complete_workflow.py:152` | the status read (RUNNING, or PAUSED with `is_canceled`) |
| `admission.py:57` `start_running` | NOT_STARTED or BUFFERED |
| `admission.py:103,113` promote cancel / no stages | BUFFERED |
| `start_workflow.py:145` buffer, `:165` no stages, `:201` terminate | NOT_STARTED |
| `workflow_control.py:228` RestartStage | the complete status read |
| `workflow_control.py:314` ResumeStage, `orchestrator.py:214,237` resume | PAUSED |

- `handlers/complete_workflow.py`: compute status and `end_time`; inside the transaction:
  `claim_workflow_status`, never-started stage writes, mark processed, pushes, completion event
  last. Metrics, log line and `execution.cleanup()` after commit. The loser's `ConcurrencyError`
  reaches the existing `retry_on_concurrency_error`, which re-reads.
- Handlers already run under `retry_on_concurrency_error`. `Orchestrator` resume runs on the
  caller's thread: a mismatch there means another actor already moved the workflow, so it logs
  and returns, as its existing `status != PAUSED: return` does.

Rejected: the pause lock (`store.pause` does not take it). An optional argument (the first plan):
the next unguarded caller would compile. A version column: a schema change.
Known limit: a status compare cannot see RUNNING -> PAUSED -> RUNNING between read and write. The
decisions here derive from stage rows, which are versioned, so that sequence changes no outcome.

### W2. Lifecycle events inside the transaction (C2, C9)

Pattern: the `record_*` call moves to the end of the `with self.repository.transaction(self.queue)`
block. `store_stage` bumps the in-memory version at once, so the payload is what 0.34.5 wrote.

- `start_stage/starting.py`: into the plan transaction (the one that pushes `StartTask`), so a
  zombie re-plan records it once.
- `start_task.py`, `cancel_stage.py`, `skip_stage.py`: same move.
- `admission.py::start_running`: records `workflow.created` and `workflow.started` after the
  pushes, `source_handler=type(handler).__name__`. Its two callers (`start_workflow._start_in`,
  `admission.promote_waiting`) lose their own recording in `StartWorkflowHandler._after_start` and
  `StartWaitingWorkflowsHandler._handle_with_retry`; `audit()` stays.
- C9, if reproduced: one helper records the terminal event by status; `CompleteWorkflow`,
  `_terminate`, the two no-stages paths and the promote cancel path call it inside their transaction.

Split event database: the append cannot join the transaction, but it now precedes the commit
that makes the successor visible. A rollback after the append still leaves that event
(`events/txn_scope.py:13-15` documents this); recording last keeps the window to the commit.

### W7. The event contract, enforced (class closure for C2, C10)

Concept: the delivery contract in `persistence/committed_marks.py`, which already flags a push
made outside a store transaction and reports it through `_check_delivery`.

- `committed_marks.py`: `record_bare_event(event_type)` and `record_dropped_events(n)`, kept
  between `clear_committed()` and `_check_delivery`, honouring `delivery_exempt()`;
  `delivery_defects()` returns them as reasons; a bounded process-wide log of recent defects.
- `events/recorder/base.py::_record`: no scope, no explicit connection, and
  `event.metadata.source_handler` set -> `record_bare_event`. Task progress events
  (`streaming.py`, no source handler) are not engine events and are not counted.
- `queue/processor/mixins.py`: the existing "held ... never reached a store transaction" branch
  also calls `record_dropped_events`.
- `tests/conftest.py`: an autouse fixture clears the log before each test and fails the test if
  an event defect was logged, with a marker for the tests that provoke one on purpose.

Run on the 0.34.5 handlers, W7 alone must turn the processor-driven tests red; that run is the
proof the check can see (Phase 4). If C10 shows benign dropped events, each case is classified
on the ticket before the fixture is allowed to fail on it.

### W3. Replay identity (C4)

`events/replay.py`: `_apply_stage_event` and `_apply_task_event` fill missing `ref_id`, `type`,
`name` / `name`, `stage_id` from any event that carries them.

### W4. Dedup fill count (C3)

`queue/dedup.py`: `_set_bit` increments `_set_bits` when the bit was 0; `reset()` zeroes it;
`fill_ratio` returns `_set_bits / _size`. `_set_bit` is the array's only writer (`mark_seen`,
`hydrate`, both under the lock). Callers `mixins.py:176,179` see the same exact value.

Rejected: Webmail's `1 - exp(-k*n/m)` (an estimate; `items_added` counts repeats).
`int.from_bytes(...).bit_count()` (0.18 ms, still grows with size, 180 KB allocated per message).

### W5. Benchmark and its test (C5, C6)

`examples/benchmark-overhead.py`: databases in `tempfile.TemporaryDirectory()`.
`tests/test_examples_run.py`: move it from `NEEDS` to `HEADLESS`. If CI's phase 3 time comes near
the script's own 60 s deadline, the deadline is raised and the measured time goes on the ticket.

### W6. Docs and changelog

`docs/guide/event_sourcing.rst` (Transactionality: which events, the split-database case, the
defect metric), `CHANGELOG.md`, both through the `human-voice` skill. Version 0.34.6.

## Tests and probes (written first, committed without the fixes)

New `tests/test_event_transaction_order.py` and `tests/test_workflow_status_guard.py`, both
backends through a `stack` fixture like `tests/test_emitted_events.py:39-78`. State is read
through the store, queue and event-store APIs only.

- T1 two-at-once completion: two threads run `CompleteWorkflowHandler.handle` with distinct
  messages, held at a one-shot barrier after both have read. Asserts E5; a `pipeline_config_id`
  variant asserts one `StartWaitingWorkflows`.
- T2 successor visibility: the handler is called directly on an empty queue; an event-store
  wrapper calls `queue.poll_one()` from another thread when `stage.started`, `task.started`,
  `workflow.started` is appended. Any message returned breaks E2.
- T3 die-here: the event store refuses the lifecycle append once. Asserts E3 for `StartStage`,
  `StartTask`, `StartWorkflow`, `StartWaitingWorkflows`, `CancelStage`.
- T4 rollback: `push_message` raises once (as `tests/test_event_txn_consistency.py:88`). Asserts
  E4 for `SkipStage` and `CompleteWorkflow`.
- T5 contract: in `tests/test_delivery_marks.py`, a handler recording outside a transaction during
  a delivery yields a defect naming the event; inside a transaction yields none; a task progress
  event yields none; a dropped held event yields one.
- T6 two-at-once at the other guarded sites: start, orchestrator resume, `ResumeStage`,
  `RestartStage`, same barrier harness. Plus a direct test of E6 on each backend's transaction.
- T7 terminal events (C9): cancel before start, no initial stages, purge while waiting.
- T8 replay identity: `outputs.updated` then `stage.started`, recorded through `EventRecorder`.
- T9 in `tests/test_bloom_dedup.py`: exactness against an independent popcount after marks,
  repeats, hydrate and reset; the size ratio (best of 5 repeats, 20,000 calls each).
- T10 `tests/test_examples_run.py`: benchmark in `HEADLESS`.
- Probe `audit/evaluations/probe_event_order_under_fanout.py`: 32-way fan-out, 10-worker
  processor, SQLite and `STABILIZE_PROBE_DSN`; one completion event, every entity's started before
  its completed, replay has every `ref_id`, zero delivery defects.
- `audit/evaluations/expect/0.34.5.tests`: each new test with its failure marker and a control.

## Refutation rounds on this plan (mine, by reading the code)

Round 1:

| Attacked | Result |
|---|---|
| Serialise completion with the pause lock | Refuted: `store.pause` (`sqlite/operations.py:54`) takes no lock. Replaced by W1 |
| Record `status.changed` before the guarded write | Refuted for a split event database. Events moved after the write |
| Repair old logs in replay | Refuted: a loop on a host a few ms behind would be read as a late start. Dropped; see D1 |
| Moving the record changes the event's `version` | Holds (`sqlite/transaction.py:176`) |
| SQLite `lock_admission` must be first; held events drain at begin | Holds: the changed sites record nothing before a transaction |
| An append inside the transaction can deadlock a split SQLite setup | Holds: it autocommits on its own connection. Cost measured in Phase 6 |
| The fill counter can drift | `_set_bit` is the only writer; T9 checks an independent popcount |

Round 2:

| Attacked | Result |
|---|---|
| Does the plan stop the class, or only today's seven sites | Refuted: one census test was the only guard. Added W7 |
| An optional `expected_status` closes the workflow-row class | Refuted: ten unguarded callers remain and an eleventh would compile. Made required, all sites listed |
| T2 as first written (`has_pending_message_for_task`) can go red for the right reason | Refuted: `StartTask` and its successor `RunTask` carry the same task id. Rewritten around `poll_one()` on an empty queue |
| Every way a workflow ends records a terminal event | Refuted by reading: four paths skip `CompleteWorkflow` (C9) |
| No event is ever lost silently | Refuted by reading: dropped held events only warn (C10) |
| The retry can read a stale workflow | Holds: `retrieve` has no cache and SQLite reads autocommit |
| PostgreSQL compare-and-set under two open transactions | Holds at READ COMMITTED (no isolation override in `persistence/`): the second UPDATE waits, re-checks, matches no row |
| The loser needs retries enabled | Default `concurrency_max_retries` is 3; at 0 the message is redelivered and finds the workflow complete |
| W7 flags legitimate recording | Limited to a delivery, to events with a source handler, and outside `delivery_exempt()`; T5 has the three controls |
| The suite-wide fixture misattributes a defect from a leftover thread | After the fix the count is zero everywhere, so attribution cannot matter; before it, Phase 4 wants the red |

Settled only by running, in Phases 2 to 4: C1 and C2 on PostgreSQL, C8, C9, C10, C7.

## What this plan does not cover

Sustained load; a multi-host processor fleet; a real process kill between commit and append (T3
injects the failure); Webmail's throughput figures on their hardware; events duplicated by a
retried transaction on a split event database (documented behaviour, unchanged); the red-proof
shards (failing on main before this change); the stale 0.33.2 CI baseline job; the seven tickets
shown in-progress since October (#93, #97 to #101, #103), whose state I have not checked.

## Found while reproducing (2026-11-24, 0.34.5 handlers, both backends)

Red runs of the new tests on the unfixed handlers: SQLite 27 failed / 8 passed, PostgreSQL 25 failed / 6
passed, each failure with its stated assertion (the passes are the controls, the contract tests and two bloom
characterisations). The fan-out probe through a 10-worker processor: `workflow.completed` per round 7, 10, 10
on SQLite and 10, 10, 10 on PostgreSQL.

- C1, C2 (successor visible before the event; event lost when the append is refused; phantom on rollback),
  C8 (start, `ResumeStage`), C9 (four paths): CONFIRMED on both backends.
- C4 is not a race. `status.changed` (NOT_STARTED to RUNNING) is recorded in the claim transaction and is the
  first event of every stage since 0.34.0, so replay names no stage at all (33 of 33 unnamed in every round).
- A started event after its completed event was not observed in six probe rounds.
- C11 (new, CONFIRMED): `RestartStage` reopens a finished workflow and records nothing; replay keeps reading
  it as finished.
- C12 (new, CONFIRMED): `StartStage` commits "stage RUNNING" in one transaction and plans the stage in a
  second. When the second does not commit, the redelivery is ignored as "already RUNNING" and the recovery
  sweep re-issues `StartTask`, so the task runs without the planner's work (upstream outputs in the context,
  before-stages) and with no `stage.started`.

### Added requirements

- E13. When a `StartStage` message is handled again after its claim committed and its plan did not, the engine
  shall plan the stage: upstream outputs in its context, 1 `stage.started`, 1 `StartTask`.
- E14. When the recovery sweep finds a RUNNING stage that was claimed and never planned, it shall re-issue
  `StartStage` for it and shall not issue `StartTask`.
- E15. If a `StartStage` message other than the claiming one arrives for a stage that is claimed and not yet
  planned, then the engine shall ignore it, as in 0.34.5.
- E16. When `RestartStage` reopens a finished workflow, the engine shall record 1 workflow `status.changed`
  from its finished status to RUNNING, and replay shall read the workflow as RUNNING.
- E3 for `StartStage` reads with E13: the redelivery that records the event is the claiming message's own.
- E10 as implemented: a 500,000-item filter against a 1,000-item filter, within 20x (0.34.5: about 460x).

### W8. A claimed stage that was never planned (C12)

Concept: the processed mark of the claiming message is already written in the plan transaction, so "claimed
and not planned" is `RUNNING` plus a claim id whose message is not marked processed.

- The claim transaction writes the claiming message's id into the stage context under an engine key; the plan
  transaction removes it.
- `StartStage` for a RUNNING stage resumes planning through the existing zombie re-plan path (claim with
  expected phase RUNNING, version check) when its own message id is the claim id, or when the claiming
  message is neither marked processed nor still a row of the queue (`Queue.live_delivery_keys`).
- `recovery_plan._plan_running_stage`: a RUNNING stage with a claim id whose message is unprocessed gets
  nothing while a message for the stage is queued, and `StartStage` once none is.
- First version of this rule matched on the id alone and gave recovery's `StartStage` the claim id. Refuted
  before release: a queue names a delivery by its row id, so the re-issued message arrives under a new id.
  The test that passed handed the message to the handler without the queue; the tests now push and poll.
- On 0.34.5 the sweep re-issued `StartTask` even while the claiming `StartStage` was still queued (seen in the
  falsification run against the published package).
- Relies on a workflow's messages living in one queue table, which `queue_table` on the workflow row fixes.

Rejected: recording `stage.started` in the claim transaction (its `task_count` would be read before planning
builds the tasks, and it leaves the unplanned stage as it is). Re-planning any RUNNING stage with no started
task (a second upstream's `StartStage` arriving while the first is planning would plan twice). One transaction
for claim and plan (planning would run under the SQLite write lock and hold a second PostgreSQL connection).

### Also found and fixed

- A waiting workflow that a user canceled had `canceled_by` and `cancellation_reason` overwritten with
  `system` and the purge reason when the queue was purged; a workflow that expired while waiting was stored
  with the purge reason. CONFIRMED on published 0.34.5, both backends.
- A workflow that ended TERMINAL for having no initial stages kept `end_time` empty on the `StartWorkflow`
  path; it is now set there as on the waiting path.

### Observed and left as it is

- `StartWaitingWorkflows` writes the cancellation fields of a purged workflow after its transaction commits;
  a process that stops in between leaves status CANCELED with those fields empty. The `workflow.canceled`
  event now carries them.
- A waiting workflow flagged canceled between the candidate read and the admission lock is recorded with
  `canceled_by` empty in its event; the store row holds the value.
- `HTTPTask` builds a TLS context for every request, plain `http://` included (about 4.5 ms each).
- With a separate event database, a transaction that is retried after an append (deadlock retry, a lost
  status race at `ResumeStage`) leaves the first attempt's event in the log, as `events/txn_scope.py` states.

## Release record (2026-11-24)

Closed 2026-11-24. stabilize 0.34.6 is on PyPI (uploaded 13:30Z), tag v0.34.6 on main 5c0d7e2, merged as pull request 1 on Stabilize-Software-Foundation/Stabilize.

EARS verification, from CI pipeline 6314 on tree dee2d1a (the released tree; main 5c0d7e2 has the same tree):
- E1 to E4, E13 to E16 (events in their transaction, refused append, rollback, half-started stage): tests/test_event_transaction_order.py, test_half_started_stage.py, test_workflow_terminal_events.py pass on SQLite and PostgreSQL; job 28856 shows the same tests red on published 0.34.5 ("all 54 tests go red on 0.34.5 for their stated reason").
- E5, E6 (one completion, compare-and-set): tests/test_workflow_status_guard.py pass on both backends. The fan-out probe on published 0.34.5 in job 28856: workflow.completed per round 1, 3, 1 on SQLite and 10, 10, 10 on PostgreSQL. The same probe on this checkout passed in the audit probes job 28853.
- E7 (terminal events): tests/test_workflow_terminal_events.py.
- E8 (delivery defects): tests/test_event_contract.py, and the suite-wide fixture that fails a test with an unexpected defect. Suite result below.
- E9 (replay identity): probe C on 0.34.5 could not name 33 of 33 stages on both backends; the installed 0.34.6 benchmark prints "All 32 HTTP stages verified via event replay".
- E10 (fill_ratio): TestFillRatio passes; should_reset 0.2 microseconds against 4,006 on 0.34.5 (local measurement, 100,000-item filter).
- E11 (benchmark): tests/test_examples_run.py runs it in CI. From PyPI in a new venv, python -I site-packages/stabilize/examples/benchmark-overhead.py in an empty directory: exit 0, phase 3 995 ms, 235 events, the directory left empty.
- E12 (no schema or payload change): baseline job 28855, existing suite on published 0.34.5 against this checkout tests, 2951 passed; upgrade job 28857 passed.

Suite: Python 3.14 SQLite and PostgreSQL 3066 passed, 2 skipped (job 28852). Python 3.11 3055 passed, 13 skipped (job 28854). Lint and mypy, row-level security, the 5-minute PostgreSQL run and the wheel job passed.

Served artefact: PyPI sha256 of the wheel f3bb11e78eace91f5d7df2673e1bfd25035ba6084185b87ef3887c909fc0ffe7 and of the sdist 4596ab4a3d7215af0b49b31edb74a66ba2f2463c23ab246a390b8e675f3c7759, equal to the files built by CI job 28883 on main. 311 source files in the wheel are byte-equal to main. 365 files of the served wheel equal the installed tree.

Found in CI on the way and fixed in the same pull request: tests/test_mg_tenancy.py expected the gate role name (7720331); the baseline venv lacked coverage (7720331); tests/test_subscription_cursor.py polled before the server-wide commit watermark had passed its event, and failed once in pipeline 6313 with four workers on one server (c87c3ec). CI could not build the new repository until CI allowed its installation account (CI #329).

What was exercised: the paths above, on SQLite and on CI PostgreSQL 18, with xdist workers on one server, a 10-worker processor in the probe, and a 5-minute sustained PostgreSQL run.

What was not tested: sustained load beyond 5 minutes; more than one processor host; a real process kill between the two StartStage transactions (the tests inject a TransientError); Webmail throughput on their hardware and their cProfile case on PostgreSQL; a full local suite (barred on vm-1). The plan had a first pipeline with the red tests alone; I dropped it and relied on the falsification job, which runs the same tests against published 0.34.5.

What I remain unsure about: Webmail reported 24 s for benchmark phase 3 on a 2-core machine and I could not reproduce or explain it. The four red-proof shards had not finished when the release went out, so I do not know whether each new test fails on an engine mutant. The manual red-proofs job fails on 10 of 50 proofs, 9 of them stale on main before this change (#112). A started event after its completed event was allowed by the 0.34.5 fault and never observed.
