# 116: stabilize 0.35.0, processor latency, throughput and outage bounds

Ticket: #116 (in progress). Bundles #113 and #106. Source: Webmail v2, thread
`01M887C2Y0HHC7BDFTW3TDPYTJ`, measured on 0.34.6 with PostgreSQL 18 in tenancy mode.
Plan approved 2026-11-26.

## Baseline (Webmail's measurements on 0.34.6)

| Measure | Value |
|---|---|
| One-stage workflow | 7 messages, about 118 SQL statements by their count |
| Our own count (`tests/test_query_budget.py`) | 87 single mode, 135 tenancy mode, 48 transactions |
| Idle processor, 50 ms poll | 39.3 statements a second |
| 5 workflows/s for 30 s, p95 start-to-task, 250 ms poll, 4 workers | 27.4 s |
| same, 250 ms, 8 workers | 1.71 s |
| same, 100 ms, 4 workers | 1.43 s |
| same, 100 ms, 8 workers | 0.53 s |
| same, 50 ms, 4 workers | 0.26 s |
| 10 workflows/s, 100 ms, 8 workers | p95 2.6 s, falling behind |
| Resume after SIGKILL | 62 to 67 s (the 60 s message lock) |
| TERM during a database outage, default pool options | not finished in 40 s |
| same with `acquire_timeout=5` | 8.0 s |

## EARS spec

- E1 (W1). When a handler finishes while the pool was full, or a transaction that pushed an undelayed
  message commits in the same process, the QueueProcessor shall poll without waiting for
  `poll_frequency_ms`.
- E2 (W1). The QueueProcessor shall hold no processor lock while it polls. If a poll raises, then
  `active_count` shall return to its value before the poll.
- E3 (W1). When stop was requested before a poll returned, the QueueProcessor shall release the polled
  messages with their attempts and store-retry budget as they were before the poll.
- E4 (W1). `Queue.poll_many(n)` shall claim at most n messages, on PostgreSQL in one statement and on
  SQLite in one commit.
- E5 (W2). When a handler's transaction marks the delivered message processed, the same transaction shall
  delete its queue row, fenced on attempts and tenant. If the delete matches no row, the transaction shall
  commit as in 0.34.6.
- E6 (W3). While `message.attempts` is 1, the QueueProcessor shall not query `processed_messages` before
  handling. Otherwise it shall check as in 0.34.6.
- E7 (W4). Where `continuation_hops > 0` (default 16), when a delivery's transaction commits an undelayed
  push on the threaded path, the worker shall handle the first such message itself, up to
  `continuation_hops` messages in a row. `process_one` shall handle exactly one message.
- E8 (W4). If a delivery ends by error, duplicate, latch or stop after claiming a successor, then the
  processor shall release that successor so it is visible at once with attempts 0.
- E9 (W5). While a processor's stop event is set, an engine borrow on that processor's threads shall fail
  within 5 s. With default options, a TERM during a refused-connection outage shall end the process within
  10 s.
- E10 (W5). `PoolOptions` shall carry `lock_timeout` (default 30 s), `statement_timeout` and
  `idle_in_transaction_session_timeout` (default unset). A value the caller set in `connect_kwargs` or the
  DSN shall win. The first borrow shall log a warning when the effective value differs.
- E11 (W6). Where `idle_poll_ms` is set and a notify loopback was received within the last 12 s, an idle
  processor shall poll every `idle_poll_ms` and issue at most 6 statements a second. Otherwise it shall
  poll every `poll_frequency_ms`.
- E12 (W6). Where `PostgresQueue(wake_notify=True)`, a committed push from another process shall reach a
  listening processor so that start-to-task p95 is at most 100 ms.
- E13 (W6). If a timeout poll finds a message ready for longer than `max(2 x poll_frequency_ms, 200 ms)`,
  then the processor shall log the remedy and poll every `poll_frequency_ms` for 5 minutes.
- E14 (W7). Where `PoolOptions(worker_id=...)` is set, when the processor starts and `pg_stat_activity` shows its own
  incarnation and no other incarnation of that worker id, it shall release every message locked by that
  worker id. Otherwise it shall release none and log a warning.
- E15 (W7). If `worker_id` is set and the queue table lacks `locked_by`, then `start()` shall raise
  `EngineSchemaError` naming `stabilize mg-up`. Without `worker_id` no schema change shall be needed.
- E16 (W8). If a NOT_STARTED workflow has a StartWorkflow row in the dead-letter table, then recovery
  shall not queue StartWorkflow and shall report `dead_lettered`.
- E17 (W8). If a task returns RUNNING with `scheduled_time`, then the engine shall not run it again before
  `scheduled_time`.
- E18 (W9, #113). `benchmark-overhead.py` shall print journal mode, measured commit and read cost, and
  commit time on its own line.
- E19 (W10, #106). As ticket #106. The planner shall record, under the engine key `_declared_values`,
  the value a stage declared for a key an ancestor also outputs, and HTTPTask shall take `headers` and
  `body` from that record when it exists, and never from a value that only an ancestor supplied.
- E20. With every new option at its default, a published 0.34.6 process and a 0.35.0 process sharing one
  queue shall run 200 workflows with each task effect present once.
- E21. Statements for a one-stage workflow on the synchronous path shall be at most 73 (single mode) and
  107 (tenancy mode), and each extra stage at most 57 and 79.
- E22. On one CI job, 5 workflows/s for 30 s at 4 workers and a 250 ms poll shall have a p95
  start-to-task of at most 1.0 s on 0.35.0 and at least 5 s on 0.34.6. 500 preloaded workflows at 8
  workers shall complete at 3 times the 0.34.6 rate or more.

Withdrawn from the draft: "at most 60 statements per one-stage workflow". Handler reads are most of the
remainder and stay out of scope.

## Technical problems (implementation order)

1. Recovery and dead letters (`recovery.py:352-368`).
2. Consumer wake-up and lock scope (`queue/processor/processor.py:430-443`).
3. Transactions per message: ack and duplicate check as their own transactions.
4. Chain latency: 7 messages, each waiting for a poll.
5. Stop and outage bounds: an uninterruptible pool wait under the processor lock.
6. Cross-process wake and idle cost.
7. Liveness of a lock holder after a kill.

## Solution domains

- Wake-up: condition variable (threading), LISTEN/NOTIFY with a poll fallback (River, Oban,
  graphile-worker).
- Transaction count: delete-on-commit acknowledgement, batch claim with `FOR UPDATE SKIP LOCKED LIMIT n`.
- Chain latency: local continuation of a durable message (Temporal sticky execution).
- Liveness: static consumer identity (Kafka `group.instance.id`).
- Timeouts: libpq `options`, `psycopg_pool` timeout, through `PoolOptions` and `with_schema`.

## Alternatives

### Wake-up
- In-process hub announced after commit, plus a worker-finished signal, with the poll interval as the
  fallback timeout [CHOSEN].
- LISTEN/NOTIFY on every push, on by default [REJECTED: a 0.34.6 publisher never notifies, a notifying
  commit takes a database-wide lock, and a full notification queue fails every notifying commit].
- A shorter poll interval [REJECTED: moves the ceiling and raises the idle cost in proportion].

### Handing back a claim
- `Queue.release` (lease cleared, attempts minus one, fenced) [CHOSEN].
- `reschedule(message, 0)` [REJECTED: resets `store_retries` and `store_retry_since`, burns an attempt].

### Acknowledgement
- Fenced delete in the transaction that marks the message, one statement with the mark on PostgreSQL
  [CHOSEN].
- Abort the transaction when the delete matches no row [REJECTED: a task that already ran would run again
  on the other worker].
- Keep the separate ack [REJECTED: one extra transaction and fsync per message].

### Duplicate check
- Skip on a first delivery (`attempts == 1`) [CHOSEN].
- Drop the check [REJECTED: handlers that do not mark in a transaction, and 0.34.6 peers, still rely on
  it for redeliveries].

### Chain latency
- Insert the first undelayed successor already leased and handle it on the same thread, 16 hops, threaded
  path only [CHOSEN].
- Merge handlers into one transaction [REJECTED: changes the unit of recovery].
- Continuation on the synchronous path [REJECTED for this release: tests step through `process_one`].

### Idle cost and cross-process wake
- Opt-in `idle_poll_ms` gated on a proven notify loopback, opt-in `wake_notify`, guard for a silent
  publisher [CHOSEN].
- Stretch the idle poll with no wake channel [REJECTED: start latency becomes the idle interval].

### Liveness after a kill
- Opt-in `worker_id`, `locked_by` column required only then, release at start after a `pg_stat_activity`
  check that must first see its own incarnation [CHOSEN].
- A shorter default `lock_duration` [REJECTED: a failover longer than the lock shows every running task to
  a second worker].
- Session advisory lock per processor [REJECTED: lost on failover, meaningless behind a transaction
  pooler].
- Worker heartbeat table [REJECTED for this release: a new table and a required migration for adopters].
- Refuse to start when the identity looks taken [REJECTED: a dead host's lingering backend would block
  the restart].

### Stop and timeouts
- Stop event in a context variable, pool waits in 5 s slices on engine threads only [CHOSEN].
- Lower the default `acquire_timeout` [REJECTED: a 6 s blip would cost a 15 s redelivery].
- Default `statement_timeout` [REJECTED: would cancel host replays that work today].
- `lock_timeout` default 30 s [CHOSEN: matches `lock_admission`].

### Out of scope, with reason
- Rewriting handler reads: the code the kill tests and red-proofs are written against.
- psycopg pipeline mode for the tenant arm: pooler support on the estate is unknown.
- Deferring the tenant arm on read-only transactions: about 7% of round trips, and it touches the fence.

## Changes during implementation (2026-11-26)

- `worker_id` is a field of `PoolOptions`, not of `QueueProcessorConfig`. The identity has to be on the
  pool's sessions, and the pool exists before any processor.
- The session timeouts are read back by one statement on the first borrow of a pool. A wrapper around
  the caller's `configure` was tried and rejected: it replaced the callback object the pool receives.
- After an empty poll at the stretched interval, the next due time is read in a transaction of its own.
- New options are fields of `QueueProcessorConfig`, `PoolOptions` and `PostgresQueue` only.
  `HandlerConfig` is unchanged because its fields enter the config fingerprint stored on workflows.
- In-transaction acknowledgement and the first-delivery skip apply to queue classes that declare
  `counts_deliveries` (the two shipped queues). A third-party queue keeps the 0.34.6 path.
- #106: keeping a stage's declared value in its context when the stage is planned again was tried and
  REJECTED. `tests/test_structured_loops.py` showed that loops rely on an ancestor's output replacing
  the value on re-entry. The declared value is kept in `_declared_values` for the task to read.
- A task registered under a built-in stage type's name (`wait`) is reported by a warning at
  registration. Raising was rejected: hosts that register such a name today would stop starting.
- `scheduled_time` on a RUNNING result was confirmed as a bug on 0.34.6 and is honoured as a lower
  bound: the delay is the later of the backoff and the time asked for.
- Existing tests edited, each because it asserted the contract this ticket changes:
  `test_query_budget.py` (87/67 to 73/57), `test_delivery_marks.py` (a handled message's row leaves with
  its transaction), `test_pool_options_and_lifetime.py` (the default pool sets `lock_timeout`),
  `test_engine_columns.py` (optional columns). `probe_sigkill_before_ack.py` also kills at
  `queue.settle`.

## Verification status (2026-11-26, CI pipelines 6536, 6559 and 6564)

Met, with the measurement:

- E1 to E8, E10, E16, E17, E19: tests green on SQLite and PostgreSQL, Python 3.14 and 3.11; each test is
  red on published 0.34.6 for its stated reason (38 tests), and each of 32 stored mutants makes its test
  fail.
- E9: stop during a refused-connection outage 3.2 s idle and 3.5 s with a task running (0.34.6: 27.0 s
  and 61.1 s).
- E11 to E15: tests green on PostgreSQL. Not exercised behind a transaction pooler or across a real
  standby promotion.
- E18: the packaged example prints the lines.
- E20: published 0.34.6 and the checkout on one queue, 200 of 200 workflows, no effect repeated or
  missing, on SQLite and PostgreSQL.
- E21: single mode 73 and 57 (test budget); tenancy mode 115 and 78, measured against 143 and 98 on
  0.34.6 in the same job. The draft's 107 and 79 were derived from a documented 135 that this workload
  does not reproduce; the reduction is the predicted 28 and 20.
- E22, first half: p95 start-to-task under 5 workflows a second 0.09 to 0.18 s in three runs, against
  31.6 to 35.9 s on 0.34.6.

NOT MET:

- E22, second half: 500 preloaded workflows at 8 workers and a 50 ms poll complete at 1.0 to 1.3 times
  the 0.34.6 rate with psycopg's pure-Python implementation (4.3 to 6.2 against 4.4 to 7.9 a second) and
  1.45 times with the C implementation (10.4 against 15.1), and not 3 times. The target assumed that
  0.34.6 was bound by its poll interval at that setting. It was bound by CPU time per message: a profile
  shows 21 database round trips a message on the synchronous path and the driver at the top of the own
  time list. This release removes waiting and about a fifth of the round trips. It does not cut the
  cost of a message by the factor the target needs.
- The plan's row "one workflow on an idle processor, at most a quarter of 0.34.6" held in two runs
  (0.22 and 0.23) and not in a third on a busy runner (0.41).

Open decision for the director: release with E22's second half recorded as not met, or hold for work on
the cost per message (round trips per message: the handler reads, the tenant arm, the driver).

## Invariants

1. At-least-once delivery, one marking transaction per delivery.
2. Ack, extend, reschedule and release are fenced on attempts and tenant.
3. A crash at any point leaves each message either leased until expiry or visible.
4. With every new option at its default, a 0.34.6 process and a 0.35.0 process can share a queue.
5. No schema change is needed unless `worker_id` is set.
6. `process_one` handles exactly one message.
7. Custom `Queue` subclasses that implement only the 0.34.6 methods keep working.
