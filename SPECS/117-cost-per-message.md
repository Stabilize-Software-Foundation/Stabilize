# 117: cost per message

Ticket #117. Follows E22 of `SPECS/116-processor-throughput-and-bounds.md`, which 0.35.0 did not meet.

## EARS spec

- E1. On PostgreSQL in tenancy mode, 500 preloaded one-stage workflows at 8 workers and a 50 ms poll shall complete at 3 times the published 0.34.6 rate or more, measured in one CI job with the same psycopg implementation on both sides.
- E2. The synchronous path shall issue at most 11 database round trips per message in tenancy mode (0.35.0: about 21, measured by the profile scenario of audit/evaluations/bench_processor.py).
- E3. The change shall keep every invariant of SPECS/116 (at-least-once delivery, one marking transaction per delivery, fenced ack, a mixed fleet with 0.35.0 on one queue).
- E4. If the tenant arm cannot be sent with the first statement of a transaction (a pooler without pipeline support), then the engine shall send it as its own statement, as 0.35.0 does.

## Measured on 0.35.0 (CI pipelines 6536, 6559, 6564, 6570):
- Backlog rate 4.4 to 7.9 workflows a second against 4.3 to 6.7 on 0.34.6 with pure-Python psycopg (1.0 to 1.3 times); 15.1 and 21.4 against 10.4 and 14.6 with the C implementation (1.45 times).
- About 21 round trips per message on the synchronous path, 3.9 tenant arms per message, the driver at the top of the own-time list.

## Technical problems

1. Round trips spent on reads a handler repeats (retrieve_stage is 4 SELECTs per message).
2. One tenant arm statement per transaction, 3.9 per message.
3. Driver cost per round trip (pure-Python psycopg is about twice the C implementation).

## Solution domains

- PostgreSQL extended-protocol pipelining (psycopg pipeline mode) for the arm.
- Single-statement reads with joins or JSON aggregation for the stage and its tasks.
- The existing delivery state in persistence/committed_marks.py to carry what one delivery already read.

## Alternatives

- Arm: pipeline with the first statement [candidate] vs skip the arm on read-only transactions [candidate; saves about 7 percent and touches the fence] vs a session-level setting [REJECTED: wrong tenant on a pooled connection].
- Reads: one statement per stage load [candidate] vs a per-delivery cache [candidate; must not outlive the delivery] vs leaving reads as they are [REJECTED: cannot reach E2].
No alternative is chosen yet. The choice needs a measurement of each on the benchmark first.

Out of scope of #116 by its approved plan; recorded there as NOT MET.
