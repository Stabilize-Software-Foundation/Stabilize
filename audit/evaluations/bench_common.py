"""Database, workflow and processor helpers shared by the processor benchmark's scenarios (#116)."""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))

TENANT = "bench"
TASK_AT: dict[str, float] = {}
TASK_LOCK = threading.Lock()


def _cli(*args: str) -> None:
    done = subprocess.run([sys.executable, "-m", "stabilize.cli.main", *args], capture_output=True, text=True)
    if done.returncode != 0:
        raise SystemExit(f"stabilize {' '.join(args[:2])} failed: {done.stdout}\n{done.stderr}")


def _tenancy_database() -> str:
    from _backends import dedicated_dsn, skip

    base = os.environ.get("STABILIZE_PROBE_DSN")
    if not base:
        skip("no PostgreSQL available (set STABILIZE_PROBE_DSN)")
    dsn = dedicated_dsn(str(base), f"bench_{uuid.uuid4().hex[:8]}")
    _cli("mg-up", "--db-url", dsn)
    _cli("mg-tenancy", "enable", "--db-url", dsn)
    return dsn


def _drop(dsn: str) -> None:
    from urllib.parse import urlsplit

    import psycopg

    name = urlsplit(dsn).path.lstrip("/")
    with psycopg.connect(str(os.environ["STABILIZE_PROBE_DSN"]), autocommit=True) as conn:
        conn.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')


def _parts(dsn: str) -> tuple[Any, Any]:
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue import PostgresQueue

    return PostgresWorkflowStore(dsn, tenant=TENANT), PostgresQueue(dsn, table_name="queue_messages", tenant=TENANT)


def _registry(effects: str | None = None) -> Any:
    from stabilize import Task, TaskRegistry, TaskResult
    from stabilize.models.stage import StageExecution

    class Stamp(Task):
        def execute(self, stage: StageExecution) -> TaskResult:
            now = time.monotonic()
            workflow_id = str(stage.execution.id)
            with TASK_LOCK:
                TASK_AT.setdefault(workflow_id, now)
            if effects is not None:
                with open(effects, "a") as fh:
                    fh.write(workflow_id + "\n")
            return TaskResult.success()

    registry = TaskRegistry()
    registry.register("bench_stamp", Stamp)
    return registry


def _workflow(stages: int = 1) -> Any:
    from stabilize.models.stage import StageExecution
    from stabilize.models.task import TaskExecution
    from stabilize.models.workflow import Workflow

    return Workflow.create(
        application="bench",
        name="w",
        stages=[
            StageExecution(
                ref_id=f"s{i}",
                type="bench_stamp",
                name=f"S{i}",
                requisite_stage_ref_ids=set() if i == 0 else {f"s{i - 1}"},
                tasks=[
                    TaskExecution.create(name="t", implementing_class="bench_stamp", stage_start=True, stage_end=True)
                ],
            )
            for i in range(stages)
        ],
    )


def _processor(store: Any, queue: Any, workers: int, poll_ms: int, effects: str | None = None) -> Any:
    from stabilize import QueueProcessor
    from stabilize.queue.processor.config import QueueProcessorConfig

    return QueueProcessor(
        queue,
        config=QueueProcessorConfig(poll_frequency_ms=poll_ms, max_workers=workers),
        store=store,
        task_registry=_registry(effects),
    )


def _succeeded(store: Any, workflow_id: str) -> bool:
    return bool(store.retrieve(workflow_id).status.name == "SUCCEEDED")


def _drained(queue: Any) -> bool:
    counts = queue.counts()
    return bool(counts.pending == 0 and counts.processing == 0)


def _await_all(store: Any, queue: Any, ids: list[str], seconds: float) -> float:
    """Wait until the queue holds no message, with one count per sweep, then check every workflow once.

    The first version of this benchmark read the status of every pending workflow in a loop. With 500
    to 2,000 workflows that loop put more load on PostgreSQL and on the interpreter than the engine
    did, and the rates it reported were its own.
    """
    deadline = time.monotonic() + seconds
    empty = 0
    while empty < 2:
        if time.monotonic() >= deadline:
            raise SystemExit(f"the queue did not drain in {seconds} s: {queue.counts()}")
        empty = empty + 1 if _drained(queue) else 0
        time.sleep(0.05)
    end = time.monotonic()
    unfinished = [i for i in ids if not _succeeded(store, i)]
    if unfinished:
        raise SystemExit(f"{len(unfinished)} of {len(ids)} workflows are not SUCCEEDED although the queue is empty")
    return end


def _percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * fraction))]
