"""Processor latency, throughput and statement counts on PostgreSQL in tenancy mode (#116).

Runs against whichever stabilize the interpreter has installed and writes one JSON document.
Run it once per release, then compare the two documents:

    OLD/bin/python audit/evaluations/bench_processor.py --out old.json
    NEW/bin/python audit/evaluations/bench_processor.py --out new.json --fleet
    python audit/evaluations/bench_processor.py --compare old.json new.json

The comparison (bench_compare.py) exits 1 unless the old release shows the ceiling and the new
one clears the counted targets and the paced latency. The backlog rate and the idle-workflow
median are printed with their ratio to the old release. Needs STABILIZE_PROBE_DSN.
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
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))

from bench_common import (
    TASK_AT,
    _await_all,
    _drop,
    _parts,
    _percentile,
    _processor,
    _registry,
    _succeeded,
    _tenancy_database,
    _workflow,
)
from bench_compare import compare



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


def profile(dsn: str, workflows: int = 60, rows: int = 45) -> dict[str, Any]:
    """Where one thread spends its time handling one-stage workflows on the synchronous path."""
    import cProfile
    import io
    import pstats

    import psycopg

    from stabilize import Orchestrator, QueueProcessor

    store, queue = _parts(dsn)
    orchestrator = Orchestrator(queue, store=store)
    for _ in range(workflows):
        workflow = _workflow()
        store.store(workflow)
        orchestrator.start(workflow)
    processor = QueueProcessor(queue, store=store, task_registry=_registry())
    profiler = cProfile.Profile()
    begin = time.monotonic()
    cpu = time.process_time()
    profiler.enable()
    handled = processor.process_all(timeout=600.0)
    profiler.disable()
    wall = time.monotonic() - begin
    cpu = time.process_time() - cpu
    queue.close()
    store.close()
    for order in ("tottime", "cumulative"):
        text = io.StringIO()
        pstats.Stats(profiler, stream=text).strip_dirs().sort_stats(order).print_stats(rows)
        print(f"--- profile by {order} ---")
        print(text.getvalue())
    return {
        "driver": psycopg.pq.__impl__,
        "messages": handled,
        "wall_ms_per_message": round(1000 * wall / max(1, handled), 2),
        "cpu_ms_per_message_under_profile": round(1000 * cpu / max(1, handled), 2),
    }


def unprofiled(dsn: str, workflows: int = 60) -> dict[str, Any]:
    """Wall and process CPU time per message on the synchronous path, with no profiler attached."""
    from stabilize import Orchestrator, QueueProcessor

    store, queue = _parts(dsn)
    orchestrator = Orchestrator(queue, store=store)
    for _ in range(workflows):
        workflow = _workflow()
        store.store(workflow)
        orchestrator.start(workflow)
    processor = QueueProcessor(queue, store=store, task_registry=_registry())
    begin = time.monotonic()
    cpu = time.process_time()
    handled = processor.process_all(timeout=600.0)
    wall = time.monotonic() - begin
    cpu = time.process_time() - cpu
    queue.close()
    store.close()
    return {
        "messages": handled,
        "wall_ms_per_message": round(1000 * wall / max(1, handled), 2),
        "cpu_ms_per_message": round(1000 * cpu / max(1, handled), 2),
    }


def measure(with_fleet: bool, only: set[str] | None = None) -> dict[str, Any]:
    import psycopg

    import stabilize

    result: dict[str, Any] = {
        "version": stabilize.__version__,
        "file": stabilize.__file__,
        "driver": psycopg.pq.__impl__,
        "psycopg": psycopg.__version__,
    }
    print(f"stabilize {result['version']}, psycopg {result['psycopg']} ({result['driver']})", flush=True)
    scenarios: list[tuple[str, Any]] = [
        ("unprofiled", unprofiled),
        ("synchronous_statements", synchronous_statements),
        ("threaded_counts", threaded_counts),
        ("single", single),
        ("preloaded", preloaded),
        ("paced", paced),
    ]
    if with_fleet:
        scenarios.append(("profile", profile))
        scenarios.append(("fleet_2", lambda dsn: fleet(dsn, 2)))
        scenarios.append(("fleet_4", lambda dsn: fleet(dsn, 4)))
    for name, scenario in scenarios:
        if only is not None and name not in only:
            continue
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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out")
    parser.add_argument("--fleet", action="store_true")
    parser.add_argument("--compare", nargs=2)
    parser.add_argument("--fleet-child", nargs=3)
    parser.add_argument("--only", help="comma-separated scenario names to run")
    args = parser.parse_args()
    if args.fleet_child:
        return _fleet_child(args.fleet_child[0], args.fleet_child[1], float(args.fleet_child[2]))
    if args.compare:
        return compare(args.compare[0], args.compare[1])
    result = measure(args.fleet, set(args.only.split(",")) if args.only else None)
    text = json.dumps(result, indent=2)
    if args.out:
        Path(args.out).write_text(text)
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
