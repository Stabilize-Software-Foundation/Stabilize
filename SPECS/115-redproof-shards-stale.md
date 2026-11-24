# 115: Red-proof shards on CI: 1187 of 3068 tests proven, the stored proof list is five releases old

Ticket: #115. Opened 2026-11-24 from CI pipeline 6319.

EARS SPEC:
- The four red-proof shard jobs shall exit 0 on main: each collected test is proven red on at least one engine mutant or is listed in audit/redproof/unproven_allowed.txt with a reason.
- When a release adds or changes tests, the change shall update audit/redproof/known_proofs.tsv in the same pull request, from a CI rebaseline run.
- Each shard shall finish within its 130-minute timeout with no test left at 0 mutants tried.

Found 2026-11-24 in CI pipeline 6319 on main 571ea1c, the first run in which the shards got past the cluster guard and ran to the end:
- shard 1 (job 28900): proven 240 of 627, violations 431, replay 368 known proofs of which 75 mutants still apply.
- shard 2 (job 28901): proven 330 of 1095, violations 804, replay 882 known proofs, 100 still apply. 558 tests had 0 mutants tried.
- shard 3 (job 28902): proven 273 of 577, allowlisted 5, violations 337, replay 450 known proofs, 88 still apply.
- shard 4 (job 28903): proven 344 of 769, allowlisted 6, violations 448, replay 558 known proofs, 143 still apply.
Total 1187 of 3068 proven, 2020 violations. Each shard ran 80 to 105 minutes.

audit/redproof/known_proofs.tsv (2262 lines) was last changed in a522583 at 0.34.0. Releases 0.34.1 to 0.34.6 moved source without a rebaseline, so most stored mutants no longer apply and the fallback search runs out of time. The tests added under #111 are not in the list: in shard 2, 36 tests of tests/test_event_transaction_order.py and tests/test_event_contract.py were tried against 1 or 2 mutants and none was proven. Each of the 54 new tests is shown red on published 0.34.5 by the falsification job (28895), which is a different proof from a mutant of the current engine.

Synthesis: solution domain mutation testing; concepts a stored proof per test (known_proofs.tsv), replayed first, with a fallback search (audit/redproof/run.py, replay.py, rebaseline.py). No new component. Alternatives: run rebaseline.py in a CI job with a longer budget and commit its artefact, then fix what stays unproven [CHOSEN]; raise the shard timeout and keep searching from scratch each run [REJECTED: 105 minutes already proves 39 percent]; allowlist the unproven tests [REJECTED: 2020 entries would empty the gate]; rebaseline locally [REJECTED: test runs are barred on vm-1].

Related: #112 (manual red-proofs). Not a gate on the 0.34.6 wheel, which the product jobs cover.
