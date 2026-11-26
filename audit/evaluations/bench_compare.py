"""Compares two documents written by bench_processor.py and prints one line per check (#116).

Counted results and the paced latency are checks. The idle-workflow median and the backlog rate
are printed as measured, with their ratio to the old release.
"""

from __future__ import annotations

import json
from pathlib import Path


def compare(old_path: str, new_path: str) -> int:
    old = json.loads(Path(old_path).read_text())
    new = json.loads(Path(new_path).read_text())
    checks: list[tuple[str, bool, str]] = []
    reported: list[tuple[str, str]] = []

    def check(label: str, ok: bool, detail: str) -> None:
        checks.append((label, ok, detail))

    def ratio(after: float, before: float) -> str:
        return f"{after / before:.2f}" if before else "n/a"

    op, np_ = old["paced"]["p95_s"], new["paced"]["p95_s"]
    check("old release shows the ceiling: paced p95 >= 5 s", op >= 5.0, f"{old['version']} p95 {op} s")
    check("paced p95 <= 1.0 s", np_ <= 1.0, f"{new['version']} p95 {np_} s (was {op} s)")
    os_, ns = old["single"]["median_total_s"], new["single"]["median_total_s"]
    reported.append(("one idle workflow: median, as a share of the old", f"{ns} s (was {os_} s), {ratio(ns, os_)}"))
    orate, nrate = old["preloaded"]["per_second"], new["preloaded"]["per_second"]
    reported.append(("500 preloaded: rate, in multiples of the old", f"{nrate}/s (was {orate}/s), {ratio(nrate, orate)}"))
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
    width = max(len(label) for label in [c[0] for c in checks] + [r[0] for r in reported])
    for label, ok, detail in checks:
        print(f"[{'PASS' if ok else 'FAIL'}] {label.ljust(width)}  {detail}")
    for label, detail in reported:
        print(f"[INFO] {label.ljust(width)}  {detail}")
    _report_c_driver(Path(old_path).parent)
    failed = [label for label, ok, _ in checks if not ok]
    print(f"VERDICT: {'PASS' if not failed else 'FAIL'} ({len(checks) - len(failed)} of {len(checks)})")
    return 1 if failed else 0


def _report_c_driver(directory: Path) -> None:
    """Print, without judging, the runs made with psycopg's C implementation when both exist."""
    old_c, new_c = directory / "bench-old-c.json", directory / "bench-new-c.json"
    if not (old_c.exists() and new_c.exists()):
        print("C implementation: not measured in this job")
        return
    old = json.loads(old_c.read_text())
    new = json.loads(new_c.read_text())
    print(f"C implementation ({old['version']} on {old['driver']}, {new['version']} on {new['driver']}), reported only:")
    rows = [
        ("CPU ms per message, synchronous", "unprofiled", "cpu_ms_per_message"),
        ("wall ms per message, synchronous", "unprofiled", "wall_ms_per_message"),
        ("one idle workflow, median s", "single", "median_total_s"),
        ("500 preloaded, workflows per second", "preloaded", "per_second"),
        ("paced p95 s", "paced", "p95_s"),
    ]
    for label, scenario, key in rows:
        before = old.get(scenario, {}).get(key)
        after = new.get(scenario, {}).get(key)
        print(f"  {label.ljust(38)} {before} -> {after}")
