"""The overhead benchmark names its storage settings and reports commit time on a line of its own (#113)."""

from __future__ import annotations

import re
from pathlib import Path

import stabilize
from tests.test_examples_run import EXAMPLES, run_example


def _benchmark() -> Path:
    packaged = Path(stabilize.__file__).parent / "examples" / "benchmark-overhead.py"
    return packaged if packaged.exists() else EXAMPLES / "benchmark-overhead.py"


def test_the_benchmark_reports_its_storage_and_the_time_in_commits(tmp_path: Path) -> None:
    result = run_example(_benchmark(), tmp_path)
    assert result.returncode == 0, result.stdout[-2000:] + result.stderr[-2000:]
    out = result.stdout

    assert re.search(r"journal_mode=\w+ synchronous=\w+", out), (
        "the benchmark does not print the SQLite journal mode and synchronous setting it ran with"
    )
    commit = re.search(r"One commit \(median of (\d+)\): ([0-9.]+)ms", out)
    read = re.search(r"One read \(median of (\d+)\): ([0-9.]+)ms", out)
    assert commit is not None and int(commit.group(1)) >= 50, "no measured commit cost over at least 50 commits"
    assert read is not None and int(read.group(1)) >= 50, "no measured read cost over at least 50 reads"
    counted = re.search(r"Commits that wrote in phase 3: (\d+)\s+Time in commits: ~(\d+)ms", out)
    assert counted is not None, "the time spent in commits is not reported on a line of its own"
    assert int(counted.group(1)) > 0, "the benchmark counted no commit in phase 3"
    assert abs(int(counted.group(2)) - int(counted.group(1)) * float(commit.group(2))) <= int(counted.group(1)), (
        "the reported commit time is not the commit count times the measured commit cost"
    )
