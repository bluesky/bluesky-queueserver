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

This is demo version of the server that supports creation and closing of Run Engine execution environment, adding
and removing items from the queue, checking the queue status and execution of the queue. There is no error
handling implemented yet, so the server process will probably freeze if an error occurs. In this case the shell
running the server may need to be closed and a new shell opened.

The server can be started from a shell as follows::

  python -m aiohttp.web -H 0.0.0.0 -P 8080 bluesky_queueserver.server:init_func

The server is controlled from a different shell. Add plans to the queue::

  http POST 0.0.0.0:8080/add_to_queue plan:='{"name":"count", "args":[["det1", "det2"]]}'
  http POST 0.0.0.0:8080/add_to_queue plan:='{"name":"scan", "args":[["det1", "det2"], "motor", -1, 1, 10]}'

The names of the plans and devices are strings. The strings are converted to references to plans and
devices in the worker process. In this demo the server can recognize only 'det1', 'det2', 'motor' devices
and 'count' and 'scan' plans.

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

Execution of the plans in the queue is started as follows::

  http POST 0.0.0.0:8080/process_queue

Environment may be closed as follows::

  http POST 0.0.0.0:8080/close_environment

There is minimal user protection: the environment can not be created twice, the queue processing can not be
started before the the environment is created and the environment can not be closed twice or closed before
it is created (those mistakes are very easy to make). Also, processing an empty queue is a valid operation that
does nothing, additional items can be added at any time. If items are added to the running queue and they
are executed as part of the queue. If execution of the queue is finished before an item is added, then
execution has to be started again (execution is stopped once an attempt is made to fetch an element
from an empty queue).
