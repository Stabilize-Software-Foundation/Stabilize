How the processor takes work
============================

This page describes how a ``QueueProcessor`` finds its next message from 0.35.0 on, what it costs when
idle, how it stops, and how its work resumes after a kill.

When the processor polls
------------------------

The processor polls the queue when a worker is free and one of four things happens:

* A handler finishes while the pool was full.
* A transaction in this process that pushed a message commits, or ``Queue.push()``,
  ``reschedule()`` or ``replay_dlq()`` returns.
* A delayed message pushed in this process becomes due.
* ``poll_frequency_ms`` passes with none of the above.

The first three are announcements inside one process. They reach a processor through any queue object
built on the same database, table and tenant. A push from another process is found by the next timed
poll, or at once when the wake channel below is on.

One poll claims a message for each free worker. On PostgreSQL that is one statement.

The processor holds no lock while it polls. A poll that returns after ``request_stop()`` or ``stop()``
hands its messages back with their attempt restored.

A chain runs on one worker
--------------------------

A one-stage workflow is seven messages, and each handler commits the next one. When a delivery commits
one undelayed successor, the queue row is inserted already leased to this worker, and the same thread
handles it next. The seven messages run back to back with one poll at the start.

``QueueProcessorConfig.continuation_hops`` (16 by default) limits how many successors a worker handles in
a row before the next one goes to the queue for any worker. ``0`` sends every message to a poll, as
before 0.35.0. Of several pushes in one transaction the first is continued and the others are polled.
A delayed push is never continued.

If the delivery fails after its commit, meets a duplicate, or the processor is stopping, the claimed
successor is released and another poll takes it. A process killed between two messages of a chain
leaves one leased message behind, the same as a process killed inside a handler.

``process_one()`` and ``process_all()`` handle one message per poll and do not continue chains.

One transaction per message
---------------------------

The transaction that marks a message processed also deletes its queue row, checked against the attempt
the message was delivered at. A handler therefore commits once per message, and there is no separate
acknowledgement. When another worker has taken the lease in the meantime, the row is left to that
worker, whose delivery finds the mark and is skipped.

A message delivered for the first time is not looked up in ``processed_messages``, because a row that
was polled once cannot have been marked. A redelivery is looked up as before.

An idle processor and the wake channel
--------------------------------------

By default an idle processor polls every ``poll_frequency_ms`` (50 ms), which is about 20 transactions
a second. Two settings together make it cheaper on PostgreSQL:

.. code-block:: python

    queue = PostgresQueue(dsn, wake_notify=True)
    config = QueueProcessorConfig(idle_poll_ms=1000)

``wake_notify=True`` adds one ``NOTIFY`` to each push that leaves a message for a poll. Set it on every
queue object that pushes to the table, in every process.

``idle_poll_ms`` opens one extra connection that listens on the queue's channel. The processor then
waits ``idle_poll_ms`` after a poll that found the queue drained, and a notification wakes it sooner.
After such a poll it also reads when the next delayed message or expiring lease is due, and wakes then.

The longer wait is used only while the channel is proven. Every ``wake_ping_seconds`` (10) the processor
sends a notification through its own pool and expects it on the listener within 2 s. Until the first
one arrives, and whenever one is late or the listener loses its connection, the processor polls every
``poll_frequency_ms``. ``processor.wake_channel_proven`` and ``processor.idle_interval_ms`` report the
current state.

If a timed poll finds a first delivery that was ready for longer than twice ``poll_frequency_ms`` (and
at least 200 ms), some publisher is not notifying. The processor logs that, and polls every
``poll_frequency_ms`` for ``wake_guard_hold_seconds`` (300).

Things to know before turning it on:

* The listener needs a direct PostgreSQL session and psycopg 3.2. Behind a transaction pooler the proof
  does not arrive and the processor keeps polling at ``poll_frequency_ms``.
* A processor with ``idle_poll_ms`` holds one more connection.
* PostgreSQL takes a database-wide lock when a transaction that issued ``NOTIFY`` commits, and a
  notifying commit fails if the notification queue is full. A session that stays in a transaction while
  it listens is what fills it.
* SQLite has no channel between processes, and ``idle_poll_ms`` is ignored there with a warning.

Stopping
--------

``stop()`` waits for running handlers up to ``shutdown_timeout_seconds``. When the database is
unreachable, a handler or poll waiting for a pooled connection gives up within 5 s of the stop, and a
borrow made after the stop waits 1 s at most. With default options a ``stop()`` during an outage returns
in about 6 s. Before 0.35.0 it took 27 to 61 s, because a borrow waits ``acquire_timeout`` (30 s) and the
poll held the processor lock meanwhile.

Borrows on threads that are not a processor's own wait ``acquire_timeout`` as before.

Lock and statement timeouts
---------------------------

``PoolOptions`` carries three session timeouts in seconds:

==========================================  =======  ==============================================
Field                                       Default  Effect when exceeded
==========================================  =======  ==============================================
``lock_timeout``                            30       SQLSTATE 55P03, and the message is redelivered
``statement_timeout``                       unset    SQLSTATE 57014, and the message is retried
``idle_in_transaction_session_timeout``     unset    the server closes the session
==========================================  =======  ==============================================

They are sent as libpq ``options`` when a connection opens. A value for the same setting in
``connect_kwargs["options"]`` or in the connection string's ``options`` is kept, and ``None`` leaves the
server default. If a new connection does not hold the value the engine asked for, the first borrow logs
a warning that names the setting and the value found.

``statement_timeout`` is unset by default because a host may run long reads through the engine's pool,
such as an event replay.

Resuming after a kill
---------------------

A message leased to a process that was killed is delivered again when its ``lock_duration`` (60 s)
runs out. A supervised process can resume sooner with a worker id:

.. code-block:: python

    options = PoolOptions(worker_id="rules-1")
    queue = PostgresQueue(dsn, options=options)

At most one live process may use a given worker id for a queue table and tenant. A supervisor's program
slot or a systemd unit gives that. The queue records the id on each message it leases, and the pool's
sessions carry the id and a per-process incarnation in ``application_name``.

When a processor with a worker id starts, it looks at ``pg_stat_activity``. If it sees its own session
and no session of another incarnation of its id, it releases every lease held under the id.
``processor.last_reclaim`` holds the count and the reason. If another incarnation still has a session,
or the check cannot see its own session, nothing is released, a warning is logged, and the leases run
out as before.

It needs the ``locked_by`` column, which ``stabilize mg-up`` adds to ``queue_messages``. For a queue
table of your own, run ``ALTER TABLE <table> ADD COLUMN locked_by TEXT``. A host that sets no worker id
needs no schema change. Do not set ``application_name`` yourself on a pool with a worker id.

Two processes that run with the same worker id at the same time can release each other's leases at
start, and a released message is delivered again. Tasks must already tolerate a second delivery,
because a lease that runs out has the same effect.

A shorter ``lock_duration`` also resumes work sooner. Its cost is that a database failover longer than
the lock makes every running message visible to another worker.
