Tasks
=====

Tasks are the building blocks of Stabilize workflows.

The Task Interface
------------------

All tasks must implement the ``Task`` interface:

.. code-block:: python

    from stabilize import Task, TaskResult, StageExecution

    class MyTask(Task):
        def execute(self, stage: StageExecution) -> TaskResult:
            # Access inputs
            data = stage.context.get("data")
            
            # Do work...
            result = process(data)
            
            # Return success
            return TaskResult.success(outputs={"result": result})

Built-in Tasks
--------------

Stabilize comes with robust built-in tasks.

ShellTask
~~~~~~~~~

Executes shell commands. Safely handles stdout/stderr capturing and timeouts.

.. code-block:: python

    TaskExecution.create("shell", "shell")
    # Context: {"command": "ls -la", "cwd": "/tmp"}

PythonTask
~~~~~~~~~~

Executes Python code. Can run inline scripts or module functions.

.. code-block:: python

    TaskExecution.create("python", "python")
    # Context: {"script": "RESULT = {'value': 42}"}

HTTPTask
~~~~~~~~

Makes HTTP requests. Supports all methods, JSON, auth, and retries.

.. code-block:: python

    TaskExecution.create("http", "http")
    # Context: {"url": "https://api.com", "method": "POST", "json": {...}}

The task reads its request from the context keys ``url``, ``method``, ``headers``, ``body`` and
``json``. It writes its response to the outputs ``response_status``, ``response_headers``,
``response_body`` and ``response_body_json``.

Until 2.0.0 it also writes the same four values as ``status_code``, ``headers``, ``body`` and
``body_json``, the names used before 0.35.0. Two of those are also request keys. A stage's context is
filled with its ancestors' outputs before it runs (see :doc:`data_flow`), and up to 0.34.6 an HTTP stage
after another HTTP stage sent the upstream response's headers and body as its own request. From 0.35.0
the task sends ``headers`` and ``body`` only when the stage declared them. A value that reached the
context only from an upstream stage is left out of the request (#106). When a stage is planned again
by a restart, a jump or a loop, the context takes the ancestor's output for a shared key, which is how
a loop variable moves on (see :doc:`data_flow`). The planner keeps the stage's declared value under
the engine key ``_declared_values``, and the request reads ``url``, ``headers`` and ``body`` from
there. Up to 0.35.1 ``url`` was read from the context, and a restarted HTTP stage after another HTTP
stage requested the upstream stage's URL (#122). ``SSHTask`` reads ``host`` and ``user`` the same way.
A stage that declares no ``url`` still takes the one an ancestor outputs.

A task may also set ``headers`` or ``body`` when it runs, in the stage's context or in a copy of the
stage that it hands to ``HTTPTask.execute``. A subclass that adds an ``Authorization`` header this way
keeps the key out of the stored stage. Such a value is sent. The planner records a SHA-256 digest of
``headers`` and ``body`` under the engine key ``_planned_digests`` when an ancestor output them, and a
value that no longer matches its digest was written after planning. 0.35.0 dropped these values in a
stage downstream of one that output the same key (#118), and 0.35.1 sends them again. The engine keys
are the engine's own record. A task does not need to read or write them.

When a request ends in a status the stage does not accept, the task fails the stage and writes the
response into the stage's context, where ``status_code``, ``headers``, ``body`` and ``url`` can be read
beside the ``response_*`` names. Three of those keys are the ones the request is built from. Up to
0.35.1 a stage that was then restarted with ``Orchestrator.restart``, or re-entered by a jump, sent the
failed response's headers and body as its next request, to the last URL of any redirect (#119). From
0.36.0 the engine records what the response replaced under the engine key ``_displaced_request`` and
puts ``url``, ``headers`` and ``body`` back when the stage is reset. A key whose value was changed
after the failure is left as it is. An operator can correct a body in the stored stage and restart
the stage with the corrected body. A task class names its own request keys in
``request_context_keys``. The default is empty, and a task with no request keys keeps whatever its
failed result left in the context, except ``error``. A reset removes ``error`` for every task, and for
``HTTPTask`` it also removes ``error_code`` and the response keys of the failed attempt (#124). A stage
that succeeds after a restart then shows no failure in its context.

Failed requests
~~~~~~~~~~~~~~~

A request that fails ends the task with a result. ``HTTPTask.execute`` does not raise for a refused
address, a timeout, a connection error, a reply that is not HTTP or a body that ends early. With
``continue_on_failure`` the result is FAILED_CONTINUE, and without it TERMINAL. The result's context
holds the text under ``error`` and, from 0.36.0, one of these codes under ``error_code``:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - ``error_code``
     - When
   * - ``invalid_request``
     - No ``url``, an unsupported ``method``, an upload file that is missing, or a ``download_to`` path
       that is refused.
   * - ``blocked_address``
     - The SSRF check refused the address, before the request or when the socket connected. The request
       is not tried again, whatever ``retries`` says.
   * - ``timeout``
     - No reply, or no more of the body, within ``timeout`` seconds.
   * - ``connection_failed``
     - The connection could not be made or broke, the reply was not HTTP, or the body ended before its
       ``Content-Length``.
   * - ``response_too_large``
     - The body is longer than ``max_response_size``.
   * - ``http_status``
     - The status is not 2xx, or not in ``expected_status``. The status is under ``status_code``.
   * - ``no_response``
     - The request ended with neither a response nor an error.

The set is ``stabilize.tasks.http.failure.ERROR_CODES``. The texts under ``error`` are as they were in
0.35.0 and are not a stable interface. Match on the code.

Up to 0.35.1 a timeout while the body was read and a reply that was not HTTP left ``execute`` as an
exception, and ``continue_on_failure`` did not apply to them. A body shorter than its
``Content-Length`` was returned as a success (#126).

A request can fail after the server has sent its status line. That happens when no more of the body
arrives within ``timeout``, when the body ends before its ``Content-Length`` and when the body is
longer than ``max_response_size``. With ``continue_on_failure`` the failed result's outputs then hold
the status the server sent under ``status_code`` and ``response_status``, which can be 200. They hold
``url`` and no ``body``. Without ``continue_on_failure`` the result has no outputs. Read the result's
status or ``error_code`` before ``status_code``, because a 200 here is not a usable answer (#128).

Kept connections
~~~~~~~~~~~~~~~~

By default the task opens a connection for each request and closes it, which for HTTPS means a TLS
handshake each time. From 0.36.0 a stage can set ``keep_alive`` to true. The request then goes out on
an idle connection to the same scheme, host and port when the process holds one, and the connection
stays open for the next request.

.. code-block:: python

    context = {"url": "https://mail.example/api/label", "method": "POST", "json": {...}, "keep_alive": True}

The connections are shared by the stages and threads of one process. A connection is kept only when
the response was read to its end and the server did not ask to close it. A response over
``max_response_size``, a body that ends early, a timeout or any other error closes it. Before a kept
connection is used again the task checks that the server has not closed it. If a GET, HEAD, PUT, DELETE
or OPTIONS still fails on a kept connection before any response arrives, the task sends it once more
on a new connection. It does not do that for a POST or a PATCH, because the server may have acted on
the first one. The failure is reported, and ``retries`` applies as for any other.

The SSRF check runs when each connection is made. A kept connection stays with the address that was
checked. Requests with ``allow_private_urls`` and requests without it never share a connection, and
neither do requests with different ``verify_ssl``, ``ca_cert``, ``client_cert`` or ``client_key``.

The process holds at most 8 idle connections for a host and does not use one that has been idle for
more than 30 s. A request that goes through a proxy is sent as it is without ``keep_alive``. Two
requests that follow a redirect use two connections, since a redirect and its target are separate
requests.

Names that are also stage types
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``wait`` is a built-in stage type. A stage of type ``wait`` that lists no tasks is planned by the
built-in ``WaitStageBuilder``, which gives it a ``WaitTask``. A task you register under the name ``wait``
is not called for such a stage, and from 0.35.0 the registry logs a warning when you register one. Give
your own stage types and tasks a prefix, for example ``rules.wait``, and register with ``strict=True`` to
have a clash with an existing task name raise.

DockerTask
~~~~~~~~~~

Runs Docker containers. Supports volumes, env vars, and resource limits.

.. code-block:: python

    TaskExecution.create("docker", "docker")
    # Context: {"action": "run", "image": "alpine", "command": "echo hi"}

Advanced Task Features
----------------------

Skippable Tasks
~~~~~~~~~~~~~~~

Implement ``SkippableTask`` to conditionally skip execution logic.

.. code-block:: python

    class MySkippable(SkippableTask):
        def is_enabled(self, stage: StageExecution) -> bool:
            return stage.context.get("feature_flag") == True

Retryable Tasks
~~~~~~~~~~~~~~~

Implement ``RetryableTask`` for polling or unreliable operations. The engine handles backoff and timeouts.

A retryable task has two limits:

*   ``get_timeout()`` (or ``get_dynamic_timeout(stage)``) is the **total** time the task may
    spend across all its executions, measured from its first start. When it is exceeded the
    engine calls ``on_timeout(stage)``: a returned ``TaskResult`` is processed as the task's
    result, and ``None`` completes the task as ``TERMINAL``.
*   ``get_execution_timeout(stage)`` bounds a **single** ``execute()`` call. It defaults to the
    total limit; override it to bound each poll separately. A call that exceeds it also goes
    through ``on_timeout(stage)``.

The total limit is wall-clock time. Time the engine was not running counts towards it, and
the engine checks it before it calls ``execute()``. A task whose answer may have arrived
while nothing was polling should look once more in ``on_timeout`` and return what it finds.
``SubWorkflowTask`` and ``HighwayTask`` do this.

A ``TransientError`` raised by ``execute()`` is retried with backoff up to ``max_attempts``
(10) **consecutive** times; a ``RUNNING`` result resets the count. Any other exception ends
the task ``TERMINAL``.

.. code-block:: python

    class WaitForDeploy(RetryableTask):
        def get_timeout(self) -> timedelta:
            return timedelta(hours=2)

        def get_execution_timeout(self, stage: StageExecution) -> timedelta:
            return timedelta(seconds=30)

        def get_backoff_period(self, stage: StageExecution, duration: timedelta) -> timedelta:
            return timedelta(seconds=10)

        def execute(self, stage: StageExecution) -> TaskResult:
            return TaskResult.success() if deployed(stage) else TaskResult.running()

Timers
~~~~~~

``TimerTask`` waits until an absolute time. The stage context names either ``until``
(an ISO 8601 time with a UTC offset, or epoch seconds) or ``duration_seconds``, counted
from the task's first execution. The first execution records ``_timer_armed_at`` and
``_timer_due_at`` in the stage context and asks the engine to run it again at the due
time. The execution at or after the due time succeeds. A delivery before the due time,
after a crash or a redelivery, asks again for the due time. A pause does not move the
due time.

A timer has no total limit. One that is handled after its due time fires then, however
late: after an engine outage, or behind a backlog. ``grace`` (five minutes) and
``unarmed_timeout`` (one hour) bound a single ``execute()`` call.

.. code-block:: python

    StageExecution(ref_id="cooling_off", type="timer", name="Cooling off",
                   context={"duration_seconds": 3600})
    StageExecution(ref_id="go_live", type="timer", name="Go live",
                   context={"until": "2026-11-19T09:00:00+00:00"})

``WaitTask`` is built on ``TimerTask``. It reads ``waitTime`` in seconds, keeps
``waitStartTime`` in the context and reports ``waitedSeconds`` in the outputs.

A ``RUNNING`` result from any ``RetryableTask`` may carry ``scheduled_time`` (an epoch
millisecond). The engine stores it on ``stage.scheduled_time`` while the task waits and
clears it when the task completes. It is a record for whoever reads the stage. The delay
before the next execution comes from ``get_backoff_period``.

Task Cleanup
~~~~~~~~~~~~

Implement ``on_cleanup`` to release resources when a stage enters a terminal state:

.. code-block:: python

    class MyTask(Task):
        def execute(self, stage: StageExecution) -> TaskResult:
            # Acquire resources
            temp_file = create_temp_file()
            stage.context["temp_file"] = temp_file

            # Do work...
            return TaskResult.success()

        def on_cleanup(self, stage: StageExecution) -> None:
            """Called automatically on terminal state (success, failure, or cancel)."""
            temp_file = stage.context.get("temp_file")
            if temp_file:
                cleanup_temp_file(temp_file)

The ``on_cleanup`` method is:

*   Called automatically when stage enters terminal state
*   Called even after process crash (on recovery)
*   Timeout-protected (30 seconds default)

For more complex cleanup, use the finalizer registry:

.. code-block:: python

    from stabilize.finalizers import get_finalizer_registry

    class MyTask(Task):
        def execute(self, stage: StageExecution) -> TaskResult:
            # Register cleanup callback
            registry = get_finalizer_registry()
            registry.register(
                stage.id,
                "cleanup_external_resource",
                lambda: cleanup_external_api(stage.context["resource_id"])
            )

            # Do work...
            return TaskResult.success()
