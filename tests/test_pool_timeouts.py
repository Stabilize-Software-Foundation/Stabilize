"""Engine connections carry a lock timeout by default, and the caller's own settings win (#116)."""

from __future__ import annotations

import logging
from typing import Any

import pytest

pytest.importorskip("psycopg")

from stabilize.persistence.pool_options import PoolOptions  # noqa: E402
from stabilize.persistence.postgres import PostgresWorkflowStore  # noqa: E402
from stabilize.queue.postgres import PostgresQueue  # noqa: E402


def _show(part: Any, setting: str) -> str:
    with part._pool.connection() as conn:
        row = conn.execute(f"SHOW {setting}").fetchone()
        conn.rollback()
    return str(next(iter(row.values())) if isinstance(row, dict) else row[0])


def test_a_store_connection_carries_a_30_second_lock_timeout(postgres_url: str) -> None:
    store = PostgresWorkflowStore(postgres_url)
    try:
        found = _show(store, "lock_timeout")
        assert found == "30s", f"lock_timeout is {found!r} on an engine connection: a blocked statement waits unbounded"
    finally:
        store.close()


def test_a_queue_connection_carries_a_30_second_lock_timeout(postgres_url: str) -> None:
    queue = PostgresQueue(postgres_url, table_name="queue_messages")
    try:
        found = _show(queue, "lock_timeout")
        assert found == "30s", f"lock_timeout is {found!r} on an engine connection: a blocked statement waits unbounded"
    finally:
        queue.close()


def test_statement_and_idle_timeouts_are_unset_by_default(postgres_url: str) -> None:
    store = PostgresWorkflowStore(postgres_url)
    try:
        assert _show(store, "statement_timeout") == "0"
        assert _show(store, "idle_in_transaction_session_timeout") == "0"
    finally:
        store.close()


def test_the_three_timeouts_are_applied_when_set(postgres_url: str) -> None:
    options = PoolOptions(lock_timeout=12, statement_timeout=45, idle_in_transaction_session_timeout=90)
    store = PostgresWorkflowStore(postgres_url, options=options)
    try:
        assert _show(store, "lock_timeout") == "12s"
        assert _show(store, "statement_timeout") == "45s"
        assert _show(store, "idle_in_transaction_session_timeout") == "90s"
    finally:
        store.close()


def test_lock_timeout_none_leaves_the_server_default(postgres_url: str) -> None:
    store = PostgresWorkflowStore(postgres_url, options=PoolOptions(lock_timeout=None))
    try:
        assert _show(store, "lock_timeout") == "0"
    finally:
        store.close()


def test_a_lock_timeout_in_the_callers_connect_options_wins(postgres_url: str) -> None:
    options = PoolOptions(connect_kwargs={"options": "-c lock_timeout=5s"})
    store = PostgresWorkflowStore(postgres_url, options=options)
    try:
        assert _show(store, "lock_timeout") == "5s"
    finally:
        store.close()


def test_options_in_the_dsn_are_kept_and_the_lock_timeout_is_added(postgres_url: str) -> None:
    from psycopg.conninfo import make_conninfo

    dsn = make_conninfo(postgres_url, options="-c statement_timeout=7s")
    store = PostgresWorkflowStore(dsn)
    try:
        assert _show(store, "statement_timeout") == "7s", "the engine replaced the options the DSN carries"
        assert _show(store, "lock_timeout") == "30s", "a DSN with its own options got no lock timeout"
    finally:
        store.close()


def test_a_schema_and_the_lock_timeout_travel_together(postgres_url: str) -> None:
    store = PostgresWorkflowStore(postgres_url, schema="public")
    try:
        assert _show(store, "search_path") == "public"
        assert _show(store, "lock_timeout") == "30s"
    finally:
        store.close()


def test_the_new_fields_are_part_of_the_pool_key() -> None:
    assert PoolOptions(lock_timeout=5).key() != PoolOptions().key()
    assert PoolOptions(statement_timeout=5).key() != PoolOptions().key()
    assert PoolOptions(idle_in_transaction_session_timeout=5).key() != PoolOptions().key()


@pytest.mark.parametrize("value", [0, -1, True, "30s"])
def test_a_timeout_that_is_not_a_positive_number_is_refused(value: Any) -> None:
    with pytest.raises(ValueError, match="lock_timeout"):
        PoolOptions(lock_timeout=value)


def test_a_connection_that_does_not_hold_the_configured_timeout_is_reported(
    postgres_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    def override(conn: Any) -> None:
        conn.execute("SET lock_timeout = '1s'")
        conn.commit()

    store = PostgresWorkflowStore(postgres_url, options=PoolOptions(configure=override))
    try:
        with caplog.at_level(logging.WARNING):
            store.exists("no-such-workflow")
        assert any("lock_timeout" in r.getMessage() and "1s" in r.getMessage() for r in caplog.records), (
            "a connection holding lock_timeout 1s where 30s is configured was not reported"
        )
    finally:
        store.close()


def test_a_connection_that_holds_the_configured_timeout_is_not_reported(
    postgres_url: str, caplog: pytest.LogCaptureFixture
) -> None:
    store = PostgresWorkflowStore(postgres_url)
    try:
        with caplog.at_level(logging.WARNING):
            store.exists("no-such-workflow")
        assert not any("lock_timeout" in r.getMessage() for r in caplog.records)
    finally:
        store.close()


def test_a_lock_timeout_error_is_redelivered_on_the_store_budget() -> None:
    from stabilize.errors.store_errors import is_store_unavailable

    class LockNotAvailableError(Exception):
        sqlstate = "55P03"

    assert is_store_unavailable(LockNotAvailableError("canceling statement due to lock timeout"))
