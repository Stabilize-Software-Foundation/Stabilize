"""A store, queue and event store on one database for either backend, with hooks on appends and reads."""

from __future__ import annotations

import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from stabilize.events import EventType, configure_event_sourcing, reset_event_bus, reset_event_recorder
from stabilize.events.base import EntityType, Event
from stabilize.events.recorder import EventRecorder
from stabilize.models.stage import StageExecution
from stabilize.models.status import WorkflowStatus
from stabilize.models.task import TaskExecution
from stabilize.models.workflow import Workflow
from stabilize.tasks.registry import TaskRegistry


@dataclass
class Stack:
    store: Any
    queue: Any
    event_store: Any
    recorder: EventRecorder
    backend: str


@contextmanager
def open_stack(backend: str, tmp_path: Path, request: pytest.FixtureRequest) -> Iterator[Stack]:
    reset_event_bus()
    reset_event_recorder()
    if backend == "sqlite":
        from stabilize.events.store.sqlite.store import SqliteEventStore
        from stabilize.persistence.sqlite import SqliteWorkflowStore
        from stabilize.queue.sqlite import SqliteQueue

        url = f"sqlite:///{tmp_path / 'stack.db'}"
        store: Any = SqliteWorkflowStore(url, create_tables=True)
        queue: Any = SqliteQueue(url)
        queue._create_table()
        event_store: Any = SqliteEventStore(url, create_tables=True)
    else:
        pytest.importorskip("psycopg")
        from stabilize.events.store.postgres.store import PostgresEventStore
        from stabilize.persistence.postgres import PostgresWorkflowStore
        from stabilize.queue.postgres import PostgresQueue

        url = request.getfixturevalue("postgres_url")
        store = PostgresWorkflowStore(url)
        queue = PostgresQueue(url, table_name="queue_messages")
        queue.clear()
        event_store = PostgresEventStore(url)
    recorder = configure_event_sourcing(event_store)
    try:
        yield Stack(store, queue, event_store, recorder, backend)
    finally:
        try:
            queue.clear()
        finally:
            for resource in (queue, store, event_store):
                close = getattr(resource, "close", None)
                if callable(close):
                    close()
            reset_event_bus()
            reset_event_recorder()


class HookedEventStore:
    def __init__(self, inner: Any, before_append: Callable[[Event], None]) -> None:
        self._inner = inner
        self._before_append = before_append

    def append(self, event: Event, connection: Any | None = None) -> Event:
        self._before_append(event)
        return self._inner.append(event, connection=connection)  # type: ignore[no-any-return]

    def append_batch(self, events: list[Event], connection: Any | None = None) -> list[Event]:
        for event in events:
            self._before_append(event)
        return self._inner.append_batch(events, connection=connection)  # type: ignore[no-any-return]

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def hooked_recorder(stack: Stack, before_append: Callable[[Event], None]) -> EventRecorder:
    return EventRecorder(HookedEventStore(stack.event_store, before_append))


def registry() -> TaskRegistry:
    from tests.conftest import SuccessTask

    reg = TaskRegistry()
    reg.register("success", SuccessTask)
    return reg


def one_stage_workflow(**workflow_fields: Any) -> Workflow:
    stage = StageExecution(
        ref_id="s1",
        type="work",
        name="Work",
        tasks=[TaskExecution.create(name="t", implementing_class="success", stage_start=True, stage_end=True)],
    )
    workflow = Workflow.create(application="order", name="w", stages=[stage])
    for key, value in workflow_fields.items():
        setattr(workflow, key, value)
    return workflow


def events_of(stack: Stack, workflow_id: str, event_type: EventType) -> list[Event]:
    return [e for e in stack.event_store.get_events_for_workflow(workflow_id) if e.event_type == event_type]


def workflow_status_changes(stack: Stack, workflow_id: str) -> list[tuple[str, str]]:
    return [
        (e.data["old_status"], e.data["new_status"])
        for e in events_of(stack, workflow_id, EventType.STATUS_CHANGED)
        if e.entity_type == EntityType.WORKFLOW
    ]


def from_another_thread(call: Callable[[], Any], timeout: float = 20.0) -> Any:
    box: dict[str, Any] = {}

    def run() -> None:
        try:
            box["value"] = call()
        except BaseException as error:
            box["error"] = error

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        raise TimeoutError("the call on the other thread did not return")
    if "error" in box:
        raise box["error"]
    return box["value"]


def read_together(store: Any, parties: int, method: str = "retrieve", timeout: float = 15.0) -> None:
    barrier = threading.Barrier(parties)
    waited: set[int] = set()
    guard = threading.Lock()
    original = getattr(store, method)

    def read(*args: Any, **kwargs: Any) -> Any:
        result = original(*args, **kwargs)
        ident = threading.get_ident()
        with guard:
            first = ident not in waited and len(waited) < parties
            if first:
                waited.add(ident)
        if first:
            barrier.wait(timeout)
        return result

    setattr(store, method, read)


def drain(queue: Any) -> list[Any]:
    messages: list[Any] = []
    while True:
        message = queue.poll_one()
        if message is None:
            return messages
        messages.append(message)
        queue.ack(message)


def run_together(calls: list[Callable[[], None]], timeout: float = 60.0) -> list[BaseException | None]:
    outcomes: list[BaseException | None] = [None] * len(calls)

    def run(index: int) -> None:
        try:
            calls[index]()
        except BaseException as error:
            outcomes[index] = error

    threads = [threading.Thread(target=run, args=(i,), daemon=True) for i in range(len(calls))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout)
    assert not any(t.is_alive() for t in threads), "a handler did not return"
    return outcomes


def set_statuses(workflow: Workflow, workflow_status: WorkflowStatus, stage_status: WorkflowStatus) -> None:
    workflow.status = workflow_status
    for stage in workflow.stages:
        stage.status = stage_status
