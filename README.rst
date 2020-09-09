===================
bluesky-queueserver
===================

.. image:: https://img.shields.io/travis/bluesky/bluesky-queueserver.svg
        :target: https://travis-ci.org/bluesky/bluesky-queueserver

.. image:: https://img.shields.io/pypi/v/bluesky-queueserver.svg
        :target: https://pypi.python.org/pypi/bluesky-queueserver

.. image:: https://img.shields.io/codecov/c/github/bluesky/bluesky-queueserve
        :target: https://codecov.io/gh/bluesky/bluesky-queueserve


Server for queueing plans

* Free software: 3-clause BSD license
* Documentation: https://bluesky.github.io/bluesky-queueserver.

Features
--------

This is demo version of the QueueServer that supports creation and closing of Run Engine execution environment, adding
and removing items from the queue, checking the queue status and execution of the queue, pausing (immediate and
deferred), resuming, aborting, stopping and halting plans, collection of data to 'temp' Databroker.

Implementation of error handling is very limited. The modules are relatively stable when running the commands
presented below, the shell may still freeze if the sequence of operations is violated (e.g. if the Manager
application is closed using Ctrl-C is pressed before the RE environment is not closed). In this case
some sockets will remain open and prevent the Manager from restarting. To close the sockets (we are interested
sockets on ports 5555 and 8080), find PIDs of the processes::

  $ sudo netstat -ltnp

and then kill the processes::

  $ kill -9 <pid>

The RE Manager and Web Server are running as two separate applications. To run the demo you will need to open
three shells: the first for RE Manager, the second for Web Server and the third to send HTTP requests to
the server.

In the first shell start RE Manager::

  python -m bluesky_queueserver.manager

The Web Server should be started from the second shell as follows::

  python -m aiohttp.web -H 0.0.0.0 -P 8080 bluesky_queueserver.server:init_func

Use the third shell to send REST API requests to the server. Add plans to the queue::

  http POST 0.0.0.0:8080/add_to_queue plan:='{"name":"count", "args":[["det1", "det2"]]}'
  http POST 0.0.0.0:8080/add_to_queue plan:='{"name":"scan", "args":[["det1", "det2"], "motor", -1, 1, 10]}'

The following plan runs for 10 seconds. It is convenient for testing pausing/resuming/stopping the plan::

  http POST 0.0.0.0:8080/add_to_queue plan:='{"name":"count", "args":[["det1", "det2"]], "kwargs":{"num":10, "delay":1}}'

The names of the plans and devices are strings. The strings are converted to references to plans and
devices in the worker process. In this demo the server can recognize only 'det1', 'det2', 'motor' devices
and 'count' and 'scan' plans. If items are added to the running queue and they
are executed as part of the queue. If execution of the queue is finished before an item is added, then
execution has to be started again (execution is stopped once an attempt is made to fetch an element
from an empty queue).

The last item can be removed from the back of the queue::

  http POST 0.0.0.0:8080/pop_from_queue

The number of entries in the queue may be checked as follows::

  http GET 0.0.0.0:8080

The contents of the queue can be retrieved as follows::

  http GET 0.0.0.0:8080/queue_view

Before the queue can be executed, the worker environment must be created and initialized. This operation
creates a new execution environment for Bluesky Run Engine and used to execute plans until it explicitly
closed::

  http POST 0.0.0.0:8080/create_environment

Execute the plans in the queue::

  http POST 0.0.0.0:8080/process_queue

Request to execute an empty queue is a valid operation that does nothing.

Environment may be closed as follows::

  http POST 0.0.0.0:8080/close_environment

While a plan in a queue is executed, operation Run Engine can be paused. In the unlikely event
if the request to pause is received while RunEngine is transitioning between two plans, the request
may be rejected by the RE Worker. In this case it needs to be repeated. If Run Engine is in the paused
state, plan execution can be resumed, aborted, stopped or halted. If the plan is aborted, stopped
or halted, it is not removed from the plan queue (it remains the first in the queue) and execution
of the queue is stopped. Execution of the queue may be started again if needed.

Immediate pausing of the Run Engine (returns to the last checkpoint in the plan)::

  http POST 0.0.0.0:8080/re_pause option="immediate"

Deferred pausing of the Run Engine (plan is executed until the next checkpoint)::

  http POST 0.0.0.0:8080/re_pause option="deferred"

Resuming, aborting, stopping or halting of currently executed plan::

  http POST 0.0.0.0:8080/re_continue option="resume"
  http POST 0.0.0.0:8080/re_continue option="abort"
  http POST 0.0.0.0:8080/re_continue option="stop"
  http POST 0.0.0.0:8080/re_continue option="halt"

There is minimal user protection features implemented that will prevent execution of
the commands that are not supported in current state of the server. Error messages are printed
in the terminal that is running the server along with output of Run Engine.

There is demo of data collection capability. The instance of the QueueServer is keeping a reference
to an instance of 'temp' Databroker, which is passed to the RE Worker at the time of creation and
used to collect documents from Run Engine. Data from all plans executed during QueueServer session
are accumulated in the 'temp' database. The table that contains Run IDs and UIDs of the runs in
the databased can be printed on the screen by sending the command::

  http POST 0.0.0.0:8080/print_db_uids

The table will be printed in the RE Manager terminal::

    ===================================================================
                 The contents of 'temp' database.
    -------------------------------------------------------------------
    Run ID: 1   UID: bd621328-ffcf-409f-a668-0c303c0d287f
    Run ID: 2   UID: e85f2f40-44e9-4097-be50-c27f42c4e201
    Run ID: 3   UID: 1dec536d-3397-43c1-91a3-2af323452bfe
    -------------------------------------------------------------------
      Total of 3 runs were found in 'temp' database.
    ===================================================================

