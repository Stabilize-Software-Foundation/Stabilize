"""A threaded processor killed (SIGKILL) between two messages of a chain repeats no work and strands none (#116).

From 0.35.0 a worker that commits one successor handles it itself. For each of the 7 lifecycle
message types, worker 1 (a subprocess running QueueProcessor.start()) kills itself with SIGKILL
right after the handler of that type returns: its transaction is committed and its successor is
leased to the dead process. Worker 2 then runs the queue until the workflow completes. Every task
execution appends a line to a file.

  <type>          SIGKILL after the handler returned: executions 1, SUCCEEDED
  CONTROL         no kill: executions 1, SUCCEEDED

A case where worker 1 was not killed is reported as PROBE INVALID, not as a pass.

    AUDIT_ALLOW_DESTRUCTIVE=1 python audit/evaluations/probe_sigkill_mid_chain.py
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import tempfile
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))

TYPES = ["StartWorkflow", "StartStage", "StartTask", "RunTask", "CompleteTask", "CompleteStage", "CompleteWorkflow"]
LOCK = timedelta(seconds=2)


def _open(kind: str, url: str, create: bool) -> tuple[Any, Any]:
    if kind == "sqlite":
        from stabilize import SqliteQueue, SqliteWorkflowStore

        store = SqliteWorkflowStore(url, create_tables=create)
        queue = SqliteQueue(url, table_name="queue_messages", lock_duration=LOCK)
        if create:
            queue._create_table()
        return store, queue
    from stabilize import PostgresQueue, PostgresWorkflowStore

    return PostgresWorkflowStore(url), PostgresQueue(url, table_name="queue_messages", lock_duration=LOCK)


def _processor(store: Any, queue: Any, marker: str) -> Any:
    from stabilize import QueueProcessor, StageExecution, TaskRegistry, TaskResult
    from stabilize.queue.processor.config import QueueProcessorConfig
    from stabilize.tasks.interface import Task

    class Effect(Task):
        def execute(self, stage: StageExecution) -> TaskResult:
            with open(marker, "a") as fh:
                fh.write(f"{os.getpid()}\n")
                fh.flush()
                os.fsync(fh.fileno())
            return TaskResult.success()

    registry = TaskRegistry()
    registry.register("effect", Effect)
    config = QueueProcessorConfig(poll_frequency_ms=20, max_workers=2, retry_delay=timedelta(milliseconds=50))
    return QueueProcessor(queue, config=config, store=store, task_registry=registry)


def worker(kind: str, url: str, marker: str, wf_id: str, kill_on: str, seconds: float) -> int:
    from stabilize.queue.messages import get_message_type_name

    store, queue = _open(kind, url, create=False)
    proc = _processor(store, queue, marker)
    if kill_on != "-":
        handle = proc._handle_message

        def handle_then_die(message: Any) -> None:
            handle(message)
            if get_message_type_name(message) == kill_on:
                os.kill(os.getpid(), signal.SIGKILL)

        proc._handle_message = handle_then_die
    proc.start()
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if store.retrieve(wf_id).status.is_complete:
            break
        time.sleep(0.05)
    proc.stop()
    queue.close()
    store.close()
    return 0


def run_case(kind: str, url: str, kill_on: str) -> tuple[int, str, int]:
    from stabilize import Orchestrator, StageExecution, TaskExecution, Workflow

    store, queue = _open(kind, url, create=True)
    queue.clear()
    marker = tempfile.NamedTemporaryFile(delete=False, suffix=".exec").name
    wf = Workflow.create(
        application="probe",
        name="midchain",
        stages=[
            StageExecution(
                ref_id="s",
                type="probe",
                name="s",
                tasks=[TaskExecution.create("effect", "effect", stage_start=True, stage_end=True)],
            )
        ],
    )
    store.store(wf)
    Orchestrator(queue, store=store).start(wf)
    first = subprocess.run([sys.executable, __file__, "--worker", kind, url, marker, wf.id, kill_on, "30"], timeout=60)
    time.sleep(LOCK.total_seconds() + 0.5)
    subprocess.run([sys.executable, __file__, "--worker", kind, url, marker, wf.id, "-", "30"], timeout=60)
    status = store.retrieve(wf.id).status.name
    executions = len([x for x in Path(marker).read_text().splitlines() if x])
    os.unlink(marker)
    queue.close()
    store.close()
    return executions, status, first.returncode


def _backend_urls() -> Any:
    with tempfile.TemporaryDirectory() as d:
        yield "sqlite", f"sqlite:///{d}/probe.db"
    from _backends import postgres_dsn

    with postgres_dsn() as dsn:
        yield "postgres", dsn


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "--worker":
        return worker(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], float(sys.argv[7]))
    if os.environ.get("AUDIT_ALLOW_DESTRUCTIVE") != "1":
        print("[SKIP] kills its own worker processes; set AUDIT_ALLOW_DESTRUCTIVE=1")
        return 0
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    for kind, url in _backend_urls():
        executions, status, rc = run_case(kind, url, "-")
        ok = executions == 1 and status == "SUCCEEDED" and rc == 0
        print(f"[{'PASS' if ok else 'FAIL'}] {kind} CONTROL no kill: executions={executions} status={status}")
        if not ok:
            failures.append(f"{kind}/CONTROL")
            continue
        for kill_on in TYPES:
            executions, status, rc = run_case(kind, url, kill_on)
            if rc != -signal.SIGKILL:
                print(f"[FAIL] {kind} {kill_on}: PROBE INVALID, worker 1 exited {rc}, not killed by SIGKILL")
                failures.append(f"{kind}/{kill_on}")
                continue
            ok = executions == 1 and status == "SUCCEEDED"
            print(
                f"[{'PASS' if ok else 'FAIL'}] {kind} {kill_on} SIGKILL after the handler returned: "
                f"executions={executions} (want 1) status={status} (want SUCCEEDED)"
            )
            if not ok:
                failures.append(f"{kind}/{kill_on}")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — a SIGKILL between two messages of a chain repeats no task and strands no workflow")
    return 0


if __name__ == "__main__":
    sys.exit(main())
