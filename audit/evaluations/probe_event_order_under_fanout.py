"""Audit probe (#111): the event log of a wide fan-out is causal and names every stage.

  A  one completion    one workflow.completed for one workflow, although every
                       leaf stage sends its own CompleteWorkflow
  B  causal order      no stage or task has its started event after its
                       completed event
  C  replay identity   replay names every stage by its ref_id
  D  contract          the processor reports no delivery defect
  E  CONTROL           the workflow SUCCEEDED and every stage has both events, so
                       A to D cannot pass on an empty log

Runs on SQLite, and on PostgreSQL when STABILIZE_PROBE_DSN is set.

Run:  python audit/evaluations/probe_event_order_under_fanout.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))

from _backends import backends  # noqa: E402

from stabilize import (  # noqa: E402
    Orchestrator,
    QueueProcessor,
    StageExecution,
    Task,
    TaskExecution,
    TaskRegistry,
    TaskResult,
    Workflow,
)
from stabilize.events import (  # noqa: E402
    EventReplayer,
    EventType,
    configure_event_sourcing,
    reset_event_bus,
    reset_event_recorder,
)
from stabilize.queue.processor.config import QueueProcessorConfig  # noqa: E402

WIDTH = 32
ROUNDS = 3


class Leaf(Task):
    def execute(self, stage: StageExecution) -> TaskResult:
        return TaskResult.success(outputs={"leaf": stage.ref_id})


def _stage(ref_id: str, needs: set[str] | None = None) -> StageExecution:
    return StageExecution(
        ref_id=ref_id,
        type="leaf",
        name=ref_id,
        tasks=[TaskExecution.create(name=ref_id, implementing_class="leaf", stage_start=True, stage_end=True)],
        requisite_stage_ref_ids=needs or set(),
    )


def _event_store(store: Any) -> Any:
    url = store.connection_string
    if url.startswith("sqlite"):
        from stabilize.events import SqliteEventStore

        return SqliteEventStore(url, create_tables=True)
    from stabilize.events import PostgresEventStore

    return PostgresEventStore(url)


def run_once(store: Any, queue: Any) -> dict[str, Any]:
    reset_event_bus()
    reset_event_recorder()
    event_store = _event_store(store)
    configure_event_sourcing(event_store)
    registry = TaskRegistry()
    registry.register("leaf", Leaf)
    processor = QueueProcessor(
        queue,
        config=QueueProcessorConfig(poll_frequency_ms=5, max_workers=10),
        store=store,
        task_registry=registry,
    )
    stages = [_stage("root")] + [_stage(f"leaf_{i}", {"root"}) for i in range(WIDTH)]
    workflow = Workflow.create(application="probe", name="fanout", stages=stages)
    store.store(workflow)
    Orchestrator(queue).start(workflow)
    processor.start()
    try:
        deadline = time.monotonic() + 120.0
        while time.monotonic() < deadline and not store.retrieve(workflow.id).status.is_complete:
            time.sleep(0.02)
        time.sleep(0.5)
    finally:
        processor.stop()
    events = list(event_store.get_events_for_workflow(workflow.id))
    first: dict[tuple[str, str], int] = {}
    for event in events:
        first.setdefault((event.entity_id, event.event_type.value), event.sequence)
    late = []
    for kind_started, kind_completed in (("stage.started", "stage.completed"), ("task.started", "task.completed")):
        for (entity, kind), sequence in first.items():
            if kind == kind_started and first.get((entity, kind_completed), sequence + 1) < sequence:
                late.append(entity)
    replayed = EventReplayer(event_store).rebuild_workflow_state(workflow.id)
    refs = {s.get("ref_id") for s in replayed.get("stages", {}).values()}
    result = {
        "status": store.retrieve(workflow.id).status.name,
        "completed": sum(1 for e in events if e.event_type == EventType.WORKFLOW_COMPLETED),
        "stage_started": sum(1 for e in events if e.event_type == EventType.STAGE_STARTED),
        "stage_completed": sum(1 for e in events if e.event_type == EventType.STAGE_COMPLETED),
        "late": len(late),
        "unnamed": len({s.ref_id for s in stages} - refs),
        "defects": len(processor.delivery_defects),
    }
    reset_event_bus()
    reset_event_recorder()
    close = getattr(event_store, "close", None)
    if callable(close):
        close()
    return result


def main() -> int:
    failures: list[str] = []

    def report(ok: bool, label: str, detail: str) -> None:
        print(f"[{'PASS' if ok else 'FAIL'}] {label}: {detail}")
        if not ok:
            failures.append(label)

    for name, factory in backends():
        if factory is None:
            print(f"[SKIP] {name}: no PostgreSQL available")
            continue
        runs = []
        for _ in range(ROUNDS):
            with factory() as (store, queue):
                runs.append(run_once(store, queue))
        want = WIDTH + 1
        report(
            all(r["status"] == "SUCCEEDED" and r["stage_started"] == want == r["stage_completed"] for r in runs),
            f"{name} E CONTROL",
            f"status and stage.started/stage.completed per round="
            f"{[(r['status'], r['stage_started'], r['stage_completed']) for r in runs]} (want SUCCEEDED, {want}, {want})",
        )
        report(
            all(r["completed"] == 1 for r in runs),
            f"{name} A one completion",
            f"workflow.completed per round={[r['completed'] for r in runs]} (want 1)",
        )
        report(
            all(r["late"] == 0 for r in runs),
            f"{name} B causal order",
            f"entities started after completed per round={[r['late'] for r in runs]} (want 0)",
        )
        report(
            all(r["unnamed"] == 0 for r in runs),
            f"{name} C replay identity",
            f"stages replay cannot name per round={[r['unnamed'] for r in runs]} (want 0)",
        )
        report(
            all(r["defects"] == 0 for r in runs),
            f"{name} D contract",
            f"delivery defects per round={[r['defects'] for r in runs]} (want 0)",
        )
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — one completion, causal order, every stage named, no delivery defect")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
