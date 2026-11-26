"""A processor asked to stop while PostgreSQL refuses connections ends within 10 s, with default options (#116).

Webmail measured 0.34.6: a TERM during an outage did not finish in 40 s, because a borrow waits
30 s for a connection and the poll held the processor lock while it waited.

  A  idle processor, PostgreSQL stopped, TERM: the process exits within 10 s
  B  a handler whose task is running when PostgreSQL stops, TERM: the process exits within 10 s
  C  CONTROL (release direction): PostgreSQL up, a 0.5 s task is waited for and its workflow succeeds
  D  PostgreSQL started again: a new processor finishes the workflow of case B

Needs STABILIZE_PROBE_DSN, and PROBE_PG_STOP and PROBE_PG_START (shell commands that stop and start
that server). Without them the probe exits SKIP.

    AUDIT_ALLOW_DESTRUCTIVE=1 PROBE_PG_STOP=... PROBE_PG_START=... python audit/evaluations/probe_stop_during_outage.py
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))

LIMIT_SECONDS = 10.0


def _parts(dsn: str) -> tuple[Any, Any]:
    from stabilize import PostgresQueue, PostgresWorkflowStore

    return PostgresWorkflowStore(dsn), PostgresQueue(dsn, table_name="queue_messages")


def _registry(marker: str, seconds: float) -> Any:
    from stabilize import StageExecution, TaskRegistry, TaskResult
    from stabilize.tasks.interface import Task

    class Slow(Task):
        def execute(self, stage: StageExecution) -> TaskResult:
            Path(marker + ".task").write_text(str(os.getpid()))
            time.sleep(seconds)
            return TaskResult.success()

    registry = TaskRegistry()
    registry.register("slow", Slow)
    return registry


def child(dsn: str, marker: str, task_seconds: float) -> int:
    from stabilize import QueueProcessor

    store, queue = _parts(dsn)
    processor = QueueProcessor(queue, store=store, task_registry=_registry(marker, task_seconds))
    asked = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: asked.set())
    processor.start()
    Path(marker + ".ready").write_text(str(os.getpid()))
    asked.wait()
    return int(processor.stop())


def _workflow(store: Any, queue: Any) -> str:
    from stabilize import Orchestrator, StageExecution, TaskExecution, Workflow

    workflow = Workflow.create(
        application="probe",
        name="outage",
        stages=[
            StageExecution(
                ref_id="s",
                type="probe",
                name="s",
                tasks=[TaskExecution.create("slow", "slow", stage_start=True, stage_end=True)],
            )
        ],
    )
    store.store(workflow)
    Orchestrator(queue, store=store).start(workflow)
    return str(workflow.id)


def _wait_for(path: str, seconds: float) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if os.path.exists(path):
            return True
        time.sleep(0.02)
    return False


def _shell(command: str) -> None:
    done = subprocess.run(command, shell=True, capture_output=True, text=True)
    if done.returncode != 0:
        raise SystemExit(f"PROBE INVALID: {command!r} exited {done.returncode}: {done.stdout}{done.stderr}")


def _term_and_time(process: subprocess.Popen[bytes]) -> tuple[float, int | None]:
    begin = time.monotonic()
    process.send_signal(signal.SIGTERM)
    try:
        code = process.wait(timeout=120)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
        return time.monotonic() - begin, None
    return time.monotonic() - begin, code


def _spawn(dsn: str, marker: str, task_seconds: float) -> subprocess.Popen[bytes]:
    return subprocess.Popen([sys.executable, __file__, "--child", dsn, marker, str(task_seconds)])


def _outage_case(dsn: str, stop: str, start: str, with_task: bool) -> tuple[bool, str, str | None]:
    store, queue = _parts(dsn)
    queue.clear()
    workflow_id = _workflow(store, queue) if with_task else None
    queue.close()
    store.close()
    with tempfile.TemporaryDirectory() as directory:
        marker = f"{directory}/m"
        process = _spawn(dsn, marker, 4.0)
        try:
            if not _wait_for(marker + ".ready", 30):
                return False, "PROBE INVALID: the child never started", workflow_id
            if with_task and not _wait_for(marker + ".task", 30):
                return False, "PROBE INVALID: the task never started", workflow_id
            _shell(stop)
            time.sleep(3.0)
            seconds, code = _term_and_time(process)
        finally:
            if process.poll() is None:
                process.kill()
            _shell(start)
    ok = code is not None and seconds <= LIMIT_SECONDS
    ended = "killed by the probe after 120 s" if code is None else f"exit {code}"
    return ok, f"stop during an outage took {seconds:.1f} s ({ended}; limit {LIMIT_SECONDS:.0f} s)", workflow_id


def _control(dsn: str) -> tuple[bool, str]:
    store, queue = _parts(dsn)
    queue.clear()
    workflow_id = _workflow(store, queue)
    with tempfile.TemporaryDirectory() as directory:
        marker = f"{directory}/m"
        process = _spawn(dsn, marker, 0.5)
        try:
            if not _wait_for(marker + ".task", 30):
                return False, "PROBE INVALID: the task never started"
            seconds, code = _term_and_time(process)
        finally:
            if process.poll() is None:
                process.kill()
    deadline = time.monotonic() + 5
    status = store.retrieve(workflow_id).status.name
    while status != "SUCCEEDED" and time.monotonic() < deadline:
        time.sleep(0.1)
        status = store.retrieve(workflow_id).status.name
    queue.close()
    store.close()
    ok = code == 0 and status in ("SUCCEEDED", "RUNNING")
    return ok, f"database up: stop took {seconds:.1f} s, exit {code}, workflow {status}"


def _resumes(dsn: str, workflow_id: str) -> tuple[bool, str]:
    from datetime import timedelta

    from stabilize import PostgresQueue, PostgresWorkflowStore, QueueProcessor

    store = PostgresWorkflowStore(dsn)
    queue = PostgresQueue(dsn, table_name="queue_messages", lock_duration=timedelta(seconds=60))
    with tempfile.TemporaryDirectory() as directory:
        processor = QueueProcessor(queue, store=store, task_registry=_registry(f"{directory}/m", 0.1))
        processor.start()
        deadline = time.monotonic() + 150
        status = store.retrieve(workflow_id).status.name
        while status != "SUCCEEDED" and time.monotonic() < deadline:
            time.sleep(0.5)
            status = store.retrieve(workflow_id).status.name
        processor.stop()
    queue.close()
    store.close()
    return status == "SUCCEEDED", f"after the database returned the workflow is {status}"


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "--child":
        return child(sys.argv[2], sys.argv[3], float(sys.argv[4]))
    from _backends import postgres_dsn, skip

    stop, start = os.environ.get("PROBE_PG_STOP"), os.environ.get("PROBE_PG_START")
    if os.environ.get("AUDIT_ALLOW_DESTRUCTIVE") != "1" or not stop or not start:
        skip("stops the PostgreSQL server: set AUDIT_ALLOW_DESTRUCTIVE=1, PROBE_PG_STOP and PROBE_PG_START")
    os.environ.setdefault("STABILIZE_CIRCUIT_BREAKER_ENABLED", "false")
    failures: list[str] = []
    with postgres_dsn() as dsn:
        ok, detail = _control(dsn)
        print(f"[{'PASS' if ok else 'FAIL'}] C CONTROL {detail}")
        if not ok:
            failures.append("C")
        ok, detail, _ = _outage_case(dsn, str(stop), str(start), with_task=False)
        print(f"[{'PASS' if ok else 'FAIL'}] A idle processor: {detail}")
        if not ok:
            failures.append("A")
        ok, detail, workflow_id = _outage_case(dsn, str(stop), str(start), with_task=True)
        print(f"[{'PASS' if ok else 'FAIL'}] B task running: {detail}")
        if not ok:
            failures.append("B")
        if workflow_id is not None:
            ok, detail = _resumes(dsn, workflow_id)
            print(f"[{'PASS' if ok else 'FAIL'}] D resumes: {detail}")
            if not ok:
                failures.append("D")
    if failures:
        print(f"VERDICT: FAIL — {', '.join(failures)}")
        return 1
    print("VERDICT: PASS — a stop during a database outage is bounded, and the work resumes afterwards")
    return 0


if __name__ == "__main__":
    sys.exit(main())
