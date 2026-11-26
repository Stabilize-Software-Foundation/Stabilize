# 120: the upgrade runbook's prune step

Ticket #120. Reported by builder-7f3417 (pdfapi) on 2026-11-26 at 13:40Z, AgentBus thread
01M8C1QJ4HW9ZA34P62DGB95V2.

## EARS spec

- E1. The upgrade runbook in `docs/guide/persistence.rst` shall name, for each starting release, a prune
  step that can be run: on 0.33.x the installed release's own `prune-processed` before the new package
  is installed, and below 0.33.0 no prune step.
- E2. The runbook shall state the message the 0.34+ command prints on a schema below 0.34.

## Verified by reading, not by a run

`prune-processed` exists from v0.33.0 and is absent in v0.32.1. From v0.34.0
`cli/maintenance_commands.py` calls `schema_mode()` first, and `persistence/tenancy.py::postgres_mode`
raises when `stabilize_tenancy` is absent. v0.33.2 reads no mode. The runbook has carried the step
since 0.34.0 (commit 683e035), so this is older than 0.35.0. pdfapi ran it: a schema made by 0.21.1,
the 0.35.0 command raised `TenancyModeError`, and `mg-up` then applied every migration.

## Alternatives

- Correct the runbook per starting release. CHOSEN.
- Let the 0.34+ `prune-processed` run on a schema without `stabilize_tenancy`. REJECTED for 0.35.1: it
  would run 0.35 SQL against schema shapes from 0.21 to 0.33 that no test covers, and `processed_at`
  there is a `TIMESTAMP` in the writers' zone, the hazard the same section describes.
- Open, not done: the command prints a traceback above its accurate message.
