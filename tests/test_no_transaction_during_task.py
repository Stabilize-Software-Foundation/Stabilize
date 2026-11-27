"""While a task runs, no session of the engine holds a transaction open (PostgreSQL)."""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any, ClassVar

import pytest

from stabilize import Orchestrator, QueueProcessor, Task, TaskRegistry, TaskResult
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.queue.processor.config import QueueProcessorConfig
from tests.mg_support import scratch_database, stabilize

psycopg = pytest.importorskip("psycopg")

OPEN_FOR = """
    SELECT pid FROM pg_stat_activity
    WHERE datname = current_database() AND pid <> pg_backend_pid()
      AND xact_start IS NOT NULL AND now() - xact_start > interval '1 second'
    ORDER BY pid
"""


class LooksAtSessions(Task):
    url: ClassVar[str] = ""
    open_for_a_second: ClassVar[list[int] | None] = None

    def execute(self, stage: StageExecution) -> TaskResult:
        time.sleep(2.0)
        with psycopg.connect(LooksAtSessions.url, autocommit=True) as conn:
            LooksAtSessions.open_for_a_second = [row[0] for row in conn.execute(OPEN_FOR).fetchall()]
        return TaskResult.success()


@pytest.fixture()
def db(postgres_container: Any) -> Iterator[str]:
    with scratch_database(postgres_container, "idletxn") as url:
        assert stabilize("mg-up", "--db-url", url).returncode == 0
        yield url


def test_no_engine_session_holds_a_transaction_while_a_task_runs(db: str) -> None:
    from stabilize.persistence.postgres import PostgresWorkflowStore
    from stabilize.queue.postgres import PostgresQueue

    LooksAtSessions.url = db
    LooksAtSessions.open_for_a_second = None
    store, queue = PostgresWorkflowStore(db), PostgresQueue(db)
    registry = TaskRegistry()
    registry.register("looks_at_sessions", LooksAtSessions)
    processor = QueueProcessor(
        queue, store=store, task_registry=registry, config=QueueProcessorConfig(poll_frequency_ms=50, max_workers=2)
    )
    stage = StageExecution(
        ref_id="only",
        type="work",
        name="only",
        tasks=[
            TaskExecution.create(name="t", implementing_class="looks_at_sessions", stage_start=True, stage_end=True)
        ],
    )
    workflow = Workflow.create(application="idle", name="idle", stages=[stage])
    held = psycopg.connect(db)
    try:
        held.execute("SELECT 1")
        held_pid = held.info.backend_pid
        Orchestrator(queue, store=store).start(workflow)
        processor.start()
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline and not store.retrieve(workflow.id).status.is_complete:
            time.sleep(0.1)
        assert store.retrieve(workflow.id).status == WorkflowStatus.SUCCEEDED
    finally:
        processor.stop()
        held.close()
        queue.close()
        store.close()

    seen = LooksAtSessions.open_for_a_second
    assert seen is not None, "the task did not run"
    assert held_pid in seen, f"the check cannot see a transaction this test held open itself: {seen}"
    assert seen == [held_pid], (
        f"a session other than the test's own held a transaction open for a second while the task ran: {seen}"
    )
