"""A published 0.34.6 process and this checkout share one queue, and each task effect is present once (#116).

0.35.0 removes a message's queue row inside the handler's transaction and hands a committed
successor to the same worker already leased. A 0.34.6 process on the same queue acknowledges in a
transaction of its own and polls every message. Both run 200 one-stage workflows from one queue.

  <backend>  200 workflows SUCCEEDED, each task effect present once, both releases did work

A run in which one of the two processes handled nothing is reported as PROBE INVALID.

Needs OLD_PYTHON (an interpreter with the published release installed). Without it the probe
exits SKIP. PostgreSQL also needs STABILIZE_PROBE_DSN.

    OLD_PYTHON=.old/bin/python python audit/evaluations/probe_mixed_fleet.py
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))

WORKFLOWS = 200


def _open(kind: str, url: str, create: bool) -> tuple[Any, Any]:
    if kind == "sqlite":
        from stabilize import SqliteQueue, SqliteWorkflowStore

        store = SqliteWorkflowStore(url, create_tables=create)
        queue = SqliteQueue(url, table_name="queue_messages")
        if create:
            queue._create_table()
        return store, queue
    from stabilize import PostgresQueue, PostgresWorkflowStore

    return PostgresWorkflowStore(url), PostgresQueue(url, table_name="queue_messages")


def child(kind: str, url: str, effects: str) -> int:
    import stabilize
    from stabilize import QueueProcessor, StageExecution, TaskRegistry, TaskResult
    from stabilize.queue.processor.config import QueueProcessorConfig
    from stabilize.tasks.interface import Task

    class Effect(Task):
        def execute(self, stage: StageExecution) -> TaskResult:
            with open(effects, "a") as fh:
                fh.write(f"{stage.execution.id}\n")
                fh.flush()
            time.sleep(0.01)
            return TaskResult.success()

    store, queue = _open(kind, url, create=False)
    registry = TaskRegistry()
    registry.register("effect", Effect)
    processor = QueueProcessor(
        queue, config=QueueProcessorConfig(poll_frequency_ms=20, max_workers=4), store=store, task_registry=registry
    )
    Path(effects + ".version").write_text(stabilize.__version__)
    processor.start()
    while not os.path.exists(effects + ".stop"):
        time.sleep(0.05)
    processor.stop()
    queue.close()
    store.close()
    return 0


def _lines(path: str) -> list[str]:
    return [x for x in Path(path).read_text().splitlines() if x] if os.path.exists(path) else []


def run_case(kind: str, url: str, old_python: str) -> tuple[bool, str]:
    from stabilize import Orchestrator, StageExecution, TaskExecution, Workflow

    store, queue = _open(kind, url, create=True)
    queue.clear()
    orchestrator = Orchestrator(queue, store=store)
    ids: list[str] = []
    for _ in range(WORKFLOWS):
        workflow = Workflow.create(
            application="probe",
            name="mixed",
            stages=[
                StageExecution(
                    ref_id="s",
                    type="probe",
                    name="s",
                    tasks=[TaskExecution.create("effect", "effect", stage_start=True, stage_end=True)],
                )
            ],
        )
        store.store(workflow)
        orchestrator.start(workflow)
        ids.append(str(workflow.id))
    with tempfile.TemporaryDirectory() as directory:
        old_effects, new_effects = f"{directory}/old", f"{directory}/new"
        old = subprocess.Popen([old_python, __file__, "--child", kind, url, old_effects])
        new: subprocess.Popen[bytes] | None = None
        try:
            deadline = time.monotonic() + 120
            while len(_lines(old_effects)) < 10 and time.monotonic() < deadline:
                time.sleep(0.05)
            new = subprocess.Popen([sys.executable, __file__, "--child", kind, url, new_effects])
            deadline = time.monotonic() + 600
            pending = list(ids)
            while pending and time.monotonic() < deadline:
                pending = [i for i in pending if store.retrieve(i).status.name != "SUCCEEDED"]
                if pending:
                    time.sleep(0.2)
        finally:
            Path(old_effects + ".stop").touch()
            Path(new_effects + ".stop").touch()
            for process in (old, new):
                if process is not None:
                    try:
                        process.wait(timeout=90)
                    except subprocess.TimeoutExpired:
                        process.kill()
        by_old, by_new = _lines(old_effects), _lines(new_effects)
        versions = [
            Path(p + ".version").read_text() if os.path.exists(p + ".version") else "?"
            for p in (old_effects, new_effects)
        ]
    queue.close()
    store.close()
    counts: dict[str, int] = {}
    for line in by_old + by_new:
        counts[line] = counts.get(line, 0) + 1
    repeated = [i for i, n in counts.items() if n != 1]
    missing = [i for i in ids if i not in counts]
    detail = (
        f"{WORKFLOWS - len(pending)} of {WORKFLOWS} SUCCEEDED, {len(repeated)} effect(s) repeated, "
        f"{len(missing)} missing; {versions[0]} ran {len(by_old)} task(s), {versions[1]} ran {len(by_new)}"
    )
    if versions[0] == versions[1]:
        return False, f"PROBE INVALID: both processes run {versions[0]}; {detail}"
    if not by_old or not by_new:
        return False, f"PROBE INVALID: one release handled no task; {detail}"
    return not pending and not repeated and not missing, detail


def _backend_urls() -> Any:
    with tempfile.TemporaryDirectory() as d:
        yield "sqlite", f"sqlite:///{d}/probe.db"
    from _backends import postgres_dsn

    with postgres_dsn() as dsn:
        yield "postgres", dsn


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "--child":
        return child(sys.argv[2], sys.argv[3], sys.argv[4])
    from _backends import skip

    old_python = os.environ.get("OLD_PYTHON")
    if not old_python:
        skip("needs OLD_PYTHON, an interpreter with the published release installed")
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    for kind, url in _backend_urls():
        ok, detail = run_case(kind, url, str(old_python))
        print(f"[{'PASS' if ok else 'FAIL'}] {kind} mixed fleet: {detail}")
        if not ok:
            failures.append(kind)
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — two releases on one queue ran every workflow with each effect present once")
    return 0


if __name__ == "__main__":
    sys.exit(main())
