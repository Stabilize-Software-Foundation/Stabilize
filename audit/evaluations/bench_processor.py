"""Processor latency, throughput and statement counts on PostgreSQL in tenancy mode (#116).

Runs against whichever stabilize the interpreter has installed and writes one JSON document.
Run it once per release, then compare the two documents:

    OLD/bin/python audit/evaluations/bench_processor.py --out old.json
    NEW/bin/python audit/evaluations/bench_processor.py --out new.json --fleet
    python audit/evaluations/bench_processor.py --compare old.json new.json

The comparison exits 1 unless the old release shows the ceiling and the new one clears the
targets, so a run that cannot see the problem cannot pass. Needs STABILIZE_PROBE_DSN.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import subprocess
import sys
import tempfile
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


def paced(dsn: str, rate: float = 5.0, seconds: float = 30.0, workers: int = 4, poll_ms: int = 250) -> dict[str, Any]:
    from stabilize import Orchestrator

    store, queue = _parts(dsn)
    processor = _processor(store, queue, workers, poll_ms)
    orchestrator = Orchestrator(queue, store=store)
    started: dict[str, float] = {}
    processor.start()
    try:
        begin = time.monotonic()
        count = int(rate * seconds)
        for index in range(count):
            due = begin + index / rate
            pause = due - time.monotonic()
            if pause > 0:
                time.sleep(pause)
            workflow = _workflow()
            store.store(workflow)
            started[str(workflow.id)] = time.monotonic()
            orchestrator.start(workflow)
        _await_all(store, queue, list(started), 600.0)
    finally:
        processor.stop()
        queue.close()
        store.close()
    latencies = [TASK_AT[i] - at for i, at in started.items()]
    return {
        "workflows": len(latencies),
        "p50_s": round(statistics.median(latencies), 4),
        "p95_s": round(_percentile(latencies, 0.95), 4),
        "max_s": round(max(latencies), 4),
    }


def single(dsn: str, repeats: int = 20, workers: int = 8, poll_ms: int = 100) -> dict[str, Any]:
    from stabilize import Orchestrator

    store, queue = _parts(dsn)
    processor = _processor(store, queue, workers, poll_ms)
    orchestrator = Orchestrator(queue, store=store)
    totals: list[float] = []
    to_task: list[float] = []
    processor.start()
    try:
        for _ in range(repeats):
            time.sleep(0.31)
            workflow = _workflow()
            store.store(workflow)
            at = time.monotonic()
            orchestrator.start(workflow)
            while not _succeeded(store, str(workflow.id)):
                time.sleep(0.003)
            totals.append(time.monotonic() - at)
            to_task.append(TASK_AT[str(workflow.id)] - at)
    finally:
        processor.stop()
        queue.close()
        store.close()
    return {
        "median_total_s": round(statistics.median(totals), 4),
        "median_to_task_s": round(statistics.median(to_task), 4),
        "max_total_s": round(max(totals), 4),
    }


def preloaded(dsn: str, count: int = 500, workers: int = 8, poll_ms: int = 50) -> dict[str, Any]:
    from stabilize import Orchestrator

    store, queue = _parts(dsn)
    orchestrator = Orchestrator(queue, store=store)
    ids: list[str] = []
    for _ in range(count):
        workflow = _workflow()
        store.store(workflow)
        orchestrator.start(workflow)
        ids.append(str(workflow.id))
    processor = _processor(store, queue, workers, poll_ms)
    begin = time.monotonic()
    processor.start()
    try:
        end = _await_all(store, queue, ids, 900.0)
    finally:
        processor.stop()
        queue.close()
        store.close()
    return {"workflows": count, "seconds": round(end - begin, 3), "per_second": round(count / (end - begin), 2)}


class _Counters:
    def __init__(self) -> None:
        self.statements = 0
        self.writing_commits = 0
        self.polled = 0
        self._dirty: set[int] = set()
        self._lock = threading.Lock()

    def install(self, queue: Any) -> Any:
        import psycopg

        real_execute = psycopg.Cursor.execute
        real_commit = psycopg.Connection.commit
        real_rollback = psycopg.Connection.rollback
        counters = self

        def execute(cursor: Any, query: Any, *args: Any, **kwargs: Any) -> Any:
            result = real_execute(cursor, query, *args, **kwargs)
            text = query.as_string(None) if hasattr(query, "as_string") else str(query)
            reads = text.lstrip().upper().startswith(("SELECT", "SHOW"))
            with counters._lock:
                counters.statements += 1
                if not reads and (cursor.rowcount or 0) > 0:
                    counters._dirty.add(id(cursor.connection))
            return result

        def commit(conn: Any) -> None:
            real_commit(conn)
            with counters._lock:
                if id(conn) in counters._dirty:
                    counters._dirty.discard(id(conn))
                    counters.writing_commits += 1

        def rollback(conn: Any) -> None:
            real_rollback(conn)
            with counters._lock:
                counters._dirty.discard(id(conn))

        psycopg.Cursor.execute = execute  # type: ignore[method-assign]
        psycopg.Connection.commit = commit  # type: ignore[method-assign]
        psycopg.Connection.rollback = rollback  # type: ignore[method-assign]
        name = "poll_many" if callable(getattr(queue, "poll_many", None)) else "poll_one"
        original = getattr(queue, name)

        def poll(*args: Any, **kwargs: Any) -> Any:
            result = original(*args, **kwargs)
            taken = len(result) if isinstance(result, list) else (1 if result is not None else 0)
            with counters._lock:
                counters.polled += taken
            return result

        setattr(queue, name, poll)

        def restore() -> None:
            psycopg.Cursor.execute = real_execute  # type: ignore[method-assign]
            psycopg.Connection.commit = real_commit  # type: ignore[method-assign]
            psycopg.Connection.rollback = real_rollback  # type: ignore[method-assign]

        return restore


def threaded_counts(dsn: str, workflows: int = 20) -> dict[str, Any]:
    from stabilize import Orchestrator

    store, queue = _parts(dsn)
    processor = _processor(store, queue, 4, 20)
    orchestrator = Orchestrator(queue, store=store)
    warm = _workflow()
    store.store(warm)
    orchestrator.start(warm)
    processor.start()
    counters = _Counters()
    try:
        _await_all(store, queue, [str(warm.id)], 60.0)
        restore = counters.install(queue)
        try:
            for _ in range(workflows):
                workflow = _workflow()
                store.store(workflow)
                orchestrator.start(workflow)
                while str(workflow.id) not in TASK_AT:
                    time.sleep(0.002)
                time.sleep(0.25)
        finally:
            restore()
    finally:
        processor.stop()
        queue.close()
        store.close()
    return {
        "workflows": workflows,
        "writing_commits_per_workflow": round(counters.writing_commits / workflows, 2),
        "polled_per_workflow": round(counters.polled / workflows, 2),
    }


def synchronous_statements(dsn: str) -> dict[str, Any]:
    import psycopg

    from stabilize import Orchestrator, QueueProcessor

    def run(stages: int) -> int:
        store, queue = _parts(dsn)
        processor = QueueProcessor(queue, store=store, task_registry=_registry())
        workflow = _workflow(stages)
        counted: list[int] = []
        real = psycopg.Cursor.execute

        def execute(cursor: Any, query: Any, *args: Any, **kwargs: Any) -> Any:
            counted.append(1)
            return real(cursor, query, *args, **kwargs)

        psycopg.Cursor.execute = execute  # type: ignore[method-assign]
        try:
            store.store(workflow)
            Orchestrator(queue, store=store).start(workflow)
            processor.process_all(timeout=60.0)
        finally:
            psycopg.Cursor.execute = real  # type: ignore[method-assign]
        if not _succeeded(store, str(workflow.id)):
            raise SystemExit(f"the {stages}-stage workflow did not finish on the synchronous path")
        queue.close()
        store.close()
        return len(counted)

    run(1)
    one, two, four = run(1), run(2), run(4)
    return {"one_stage": one, "each_extra_stage": (four - two) / 2}


def _fleet_child(dsn: str, effects: str, seconds: float) -> int:
    store, queue = _parts(dsn)
    processor = _processor(store, queue, 8, 50, effects)
    processor.start()
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline and not os.path.exists(effects + ".stop"):
        time.sleep(0.05)
    processor.stop()
    queue.close()
    store.close()
    return 0


def fleet(dsn: str, processes: int, count: int = 2000) -> dict[str, Any]:
    from stabilize import Orchestrator

    store, queue = _parts(dsn)
    orchestrator = Orchestrator(queue, store=store)
    ids: list[str] = []
    for _ in range(count):
        workflow = _workflow()
        store.store(workflow)
        orchestrator.start(workflow)
        ids.append(str(workflow.id))
    with tempfile.TemporaryDirectory() as directory:
        effects = [f"{directory}/effects-{n}" for n in range(processes)]
        begin = time.monotonic()
        children = [
            subprocess.Popen([sys.executable, __file__, "--fleet-child", dsn, path, "900"]) for path in effects
        ]
        try:
            end = _await_all(store, queue, ids, 900.0)
        finally:
            for path in effects:
                Path(path + ".stop").touch()
            for child in children:
                child.wait(timeout=120)
        seen: dict[str, int] = {}
        for path in effects:
            if os.path.exists(path):
                for line in Path(path).read_text().splitlines():
                    seen[line] = seen.get(line, 0) + 1
    queue.close()
    store.close()
    repeated = sorted(i for i, n in seen.items() if n != 1)
    missing = sorted(set(ids) - set(seen))
    return {
        "processes": processes,
        "workflows": count,
        "per_second": round(count / (end - begin), 2),
        "effects_repeated": len(repeated),
        "effects_missing": len(missing),
    }


def measure(with_fleet: bool) -> dict[str, Any]:
    import stabilize

    result: dict[str, Any] = {"version": stabilize.__version__, "file": stabilize.__file__}
    scenarios: list[tuple[str, Any]] = [
        ("synchronous_statements", synchronous_statements),
        ("threaded_counts", threaded_counts),
        ("single", single),
        ("preloaded", preloaded),
        ("paced", paced),
    ]
    if with_fleet:
        scenarios.append(("fleet_2", lambda dsn: fleet(dsn, 2)))
        scenarios.append(("fleet_4", lambda dsn: fleet(dsn, 4)))
    for name, scenario in scenarios:
        dsn = _tenancy_database()
        try:
            TASK_AT.clear()
            begin = time.monotonic()
            result[name] = scenario(dsn)
            result[name]["scenario_seconds"] = round(time.monotonic() - begin, 1)
            print(f"{name}: {json.dumps(result[name])}", flush=True)
        finally:
            _drop(dsn)
    return result


def compare(old_path: str, new_path: str) -> int:
    old = json.loads(Path(old_path).read_text())
    new = json.loads(Path(new_path).read_text())
    checks: list[tuple[str, bool, str]] = []

    def check(label: str, ok: bool, detail: str) -> None:
        checks.append((label, ok, detail))

    op, np_ = old["paced"]["p95_s"], new["paced"]["p95_s"]
    check("old release shows the ceiling: paced p95 >= 5 s", op >= 5.0, f"{old['version']} p95 {op} s")
    check("paced p95 <= 1.0 s", np_ <= 1.0, f"{new['version']} p95 {np_} s (was {op} s)")
    os_, ns = old["single"]["median_total_s"], new["single"]["median_total_s"]
    check("one idle workflow: median at most a quarter", ns <= os_ / 4, f"{ns} s (was {os_} s)")
    orate, nrate = old["preloaded"]["per_second"], new["preloaded"]["per_second"]
    check("500 preloaded: at least 3 times the rate", nrate >= 3 * orate, f"{nrate}/s (was {orate}/s)")
    oc = old["threaded_counts"]["writing_commits_per_workflow"]
    nc = new["threaded_counts"]["writing_commits_per_workflow"]
    check("writing commits per workflow: at least 13 fewer", nc <= oc - 13, f"{nc} (was {oc})")
    opoll = old["threaded_counts"]["polled_per_workflow"]
    npoll = new["threaded_counts"]["polled_per_workflow"]
    check("old release polls each message: >= 6.5 polled per workflow", opoll >= 6.5, f"{opoll}")
    check("polled messages per workflow <= 1.2", npoll <= 1.2, f"{npoll} (was {opoll})")
    one, extra = new["synchronous_statements"]["one_stage"], new["synchronous_statements"]["each_extra_stage"]
    old_one, old_extra = old["synchronous_statements"]["one_stage"], old["synchronous_statements"]["each_extra_stage"]
    check("tenancy statements, one stage: at least 28 fewer", one <= old_one - 28, f"{one} (was {old_one})")
    check("tenancy statements, each extra stage: at least 20 fewer", extra <= old_extra - 20, f"{extra} (was {old_extra})")
    for name in ("fleet_2", "fleet_4"):
        if name not in new:
            check(f"{name} ran", False, "missing from the new run; pass --fleet")
            continue
        fleet_result = new[name]
        once = fleet_result["effects_repeated"] == 0 and fleet_result["effects_missing"] == 0
        check(
            f"{name}: each effect present once",
            once,
            f"{fleet_result['effects_repeated']} repeated, {fleet_result['effects_missing']} missing, "
            f"{fleet_result['per_second']}/s",
        )
    width = max(len(label) for label, _, _ in checks)
    for label, ok, detail in checks:
        print(f"[{'PASS' if ok else 'FAIL'}] {label.ljust(width)}  {detail}")
    failed = [label for label, ok, _ in checks if not ok]
    print(f"VERDICT: {'PASS' if not failed else 'FAIL'} ({len(checks) - len(failed)} of {len(checks)})")
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out")
    parser.add_argument("--fleet", action="store_true")
    parser.add_argument("--compare", nargs=2)
    parser.add_argument("--fleet-child", nargs=3)
    args = parser.parse_args()
    if args.fleet_child:
        return _fleet_child(args.fleet_child[0], args.fleet_child[1], float(args.fleet_child[2]))
    if args.compare:
        return compare(args.compare[0], args.compare[1])
    result = measure(args.fleet)
    text = json.dumps(result, indent=2)
    if args.out:
        Path(args.out).write_text(text)
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
