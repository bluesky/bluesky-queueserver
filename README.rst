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

This is demo version of the QueueServer. The project is in the process of active development, so
APIs may change at any time without notice. QueueServer may not be considered stable, so install
and use it only for evaluation purposes.

QueueServer is supporting the following functions:


- Opening, closing and destroying of RE (Run Engine) Worker environment.

- Loading and publishing the lists of allowed plans and devices.

- Loading beamlines' startup files from the corresponding ``profile_collection`` repositories.

- Adding and removing plans from the queue.

- Starting the queue.

- Pausing (immediate and deferred), resuming and stopping (stop, abort and halt) the running plan.

- Saving data to Databroker (some more work is needed).

- Streaming documents via Kafka.


In some cases the program may crash and leave some sockets open. This may prevent the Manager from
restarting. To close the sockets (we are interested in sockets on ports 5555 and 60610), find
PIDs of the processes::

  $ sudo netstat -ltnp

and then kill the processes::

  $ kill -9 <pid>


Setup redis linux::

  sudo apt install redis

Setup redis mac::

  https://gist.github.com/tomysmile/1b8a321e7c58499ef9f9441b2faa0aa8

Installation of QueueServer from source::

  pip install -e .

This also sets up an entry point for the 'qserver' CLI tool.

The RE Manager and Web Server are running as two separate applications. To run the demo you will need to open
three shells: the first for RE Manager, the second for Web Server and the third to send HTTP requests to
the server.

In the first shell start RE Manager::

  start-re-manager

The Web Server should be started from the second shell as follows::

  uvicorn bluesky_queueserver.server.server:app --host localhost --port 60610

The third shell will be used to send HTTP requests. RE Manager can also be controlled using 'qserver' CLI
tool. If only CLI tool will be used, then there is no need to start the Web Server. In the following manual
demostrates how to control RE Manager using CLI commands and HTTP requests. The CLI tool commands will be
shown alongside with HTTP requests.

To view interactive API docs, visit::

  http://localhost:60610/docs

The 'qserver' CLI tool can be started from a separate shell. Display help options::

  qserver -h

Install httpie::

  https://httpie.org/docs#installation

The most basic request is 'ping' intended to fetch some response from RE Manager::

  qserver -c ping
  http GET http://localhost:60610

Current default address of RE Manager is set to tcp://localhost:5555, but different
address may be passed as a parameter to CLI tool::

  qserver -c ping -a "tcp://localhost:5555"

The 'qserver' CLI tool may run in the monitoring mode (send 'ping' request to RE Manager every second)::

  qserver -c monitor

Currently 'ping' request returns the status of RE Manager, but the returned data may change. The recommended
way to fetch status of RE Manager is to use 'status' request::

  qserver -c status
  http GET http://localhost:60610/status

Before plans could be executed, the RE Worker environment must be opened. Opening RE Worker environment
involves loading beamline profile collection and instantiation of Run Engine and may take a few minutes.
The package comes with simulated profile collection that includes simulated Ophyd devices and built-in
Bluesky plans and loads almost instantly. An open RE Worker environment may be closed or destroyed.
Orderly closing of the environment is a safe operation, which is possible only when RE Worker
(and RE Manager) is in idle state, i.e. no plans are currently running or paused. Destroying
the environment is potentially dangerous, since it involves killing of RE Process that could potentially
be running plans, and supposed to be used for destroying unresponsive environment in case of RE failure.
Note that any operations on the queue (such as adding or removing plans) can be performed before
the environment is opened.

Open the new RE environment::

  qserver -c environment_open
  http POST http://localhost:60610/environment/open

Close RE environment::

  qserver -c environment_close
  http POST http://localhost:60610/environment/close

Destroy RE environment::

  qserver -c environment_destroy
  http POST http://localhost:60610/environment/destroy

Get the lists (JSON) of allowed plans and devices::

  qserver -c plans_allowed
  qserver -c devices_allowed

  http GET http://localhost:60610/plans/allowed
  http GET http://localhost:60610/devices/allowed

Push a new plan to the back of the queue::

  qserver -c queue_plan_add -p '{"name":"count", "args":[["det1", "det2"]]}'
  qserver -c queue_plan_add -p '{"name":"scan", "args":[["det1", "det2"], "motor", -1, 1, 10]}'
  qserver -c queue_plan_add -p '{"name":"count", "args":[["det1", "det2"]], "kwargs":{"num":10, "delay":1}}'

  http POST http://localhost:60610/queue/plan/add plan:='{"name":"count", "args":[["det1", "det2"]]}'
  http POST http://localhost:60610/queue/plan/add plan:='{"name":"scan", "args":[["det1", "det2"], "motor", -1, 1, 10]}'
  http POST http://localhost:60610/queue/plan/add plan:='{"name":"count", "args":[["det1", "det2"]], "kwargs":{"num":10, "delay":1}}'

It takes 10 second to execute the third plan in the group above, so it is may be the most convenient for testing
pausing/resuming/stopping of experimental plans.

The plan to be added at any position of the queue including pushing to the front or back of the queue::

  qserver -c queue_plan_add -p front '{"name":"count", "args":[["det1", "det2"]]}'
  qserver -c queue_plan_add -p back '{"name":"count", "args":[["det1", "det2"]]}'
  qserver -c queue_plan_add -p 2 '{"name":"count", "args":[["det1", "det2"]]}'  # Inserted at pos #2 (0-based)

  http POST http://localhost:60610/queue/plan/add pos:='"front"' plan:='{"name":"count", "args":[["det1", "det2"]]}'
  http POST http://localhost:60610/queue/plan/add pos:='"back"' plan:='{"name":"count", "args":[["det1", "det2"]]}'
  http POST http://localhost:60610/queue/plan/add pos:=2 plan:='{"name":"count", "args":[["det1", "det2"]]}'

The following command will insert the plan in place of the last element and shift the last element to
the back so that the new element is now previous to last::

  qserver -c queue_plan_add -p -1 '{"name":"count", "args":[["det1", "det2"]]}'
  http POST http://localhost:60610/queue/plan/add pos:=-1 plan:='{"name":"count", "args":[["det1", "det2"]]}'

If the queue has 5 elements (0..4), then the following command pushes the new plan to the back of the queue::

  qserver -c queue_plan_add -p 5 '{"name":"count", "args":[["det1", "det2"]]}'
  http POST http://localhost:60610/queue/plan/add pos:=5 plan:='{"name":"count", "args":[["det1", "det2"]]}'

The 'queue_plan_add' request will accept any index value. If the index is out of range, then the plan will
be pushed to the front or the back of the queue. If the queue is currently running, then it is recommended
to access elements using negative indices (counted from the back of the queue).

The names of the plans and devices are strings. The strings are converted to references to Bluesky plans and
Ophyd devices in the worker process. The simulated beamline profile collection includes all simulated
Ophyd devices and built-in Bluesky plans.

Queue can be edited at any time. Changes to the running queue become effective the moment they are
performed. As the currently running plan is finished, the new plan is popped from the top of the queue.

The contents of the queue may be fetched at any time::

  qserver -c queue_get
  http GET http://localhost:60610/queue/get

The last item can be removed (popped) from the back of the queue::

  qserver -c queue_plan_remove
  echo '{}' | http POST http://localhost:60610/queue/plan/remove

The position of the removed element may be specified similarly to `queue_plan_add` request with the difference
that the position index must point to the existing element, otherwise the request fails (returns 'success==False').
The following examples remove the plan from the front of the queue and the element previous to last::

  qserver -c queue_plan_remove -p front
  qserver -c queue_plan_remove -p -2

  http POST http://localhost:60610/queue/plan/remove pos:='"front"'
  http POST http://localhost:60610/queue/plan/remove pos:=-2

Plans can be read from the queue without changing it. `queue_plan_get` requests are formatted identically to
`queue_plan_remove` requests::

  qserver -c queue_plan_get
  qserver -c queue_plan_get -p front
  qserver -c queue_plan_get -p -2

  echo '{}' | http POST http://localhost:60610/queue/plan/get
  http POST http://localhost:60610/queue/plan/get pos:='"front"'
  http POST http://localhost:60610/queue/plan/get pos:=-2

Remove all entries from the plan queue::

  qserver -c queue-clear
  http POST http://localhost:60610/queue/clear

Start execution of the plan queue. The environment MUST be opened before queue could be started::

  qserver -c queue_start
  http POST http://localhost:60610/queue/start

Request to execute an empty queue is a valid operation that does nothing.

The queue can be stopped at any time. Stopping the queue is a safe operation. When the stopping
sequence is initiated, the currently running plan is finished and the next plan is not be started.
The stopping sequence can be cancelled if it was activated by mistake or decision was changed::

  qserver -c queue_stop
  qserver -c queue_stop_cancel

  http POST http://localhost:60610/queue/stop
  http POST http://localhost:60610/queue/stop/cancel

While a plan in a queue is executed, operation Run Engine can be paused. In the unlikely event
if the request to pause is received while RunEngine is transitioning between two plans, the request
may be rejected by the RE Worker. In this case it needs to be repeated. If Run Engine is in the paused
state, plan execution can be resumed, aborted, stopped or halted. If the plan is aborted, stopped
or halted, it is not removed from the plan queue (it remains the first in the queue) and execution
of the queue is stopped. Execution of the queue may be started again if needed.

Running plan can be paused immediately (returns to the last checkpoint in the plan) or at the next
checkpoint (deferred pause)::

  qserver -c re_pause -p immediate
  qserver -c re_pause -p deferred

  http POST http://localhost:60610/re/pause option="immediate"
  http POST http://localhost:60610/re/pause option="deferred"

Resuming, aborting, stopping or halting of currently executed plan::

  qserver -c re_resume
  qserver -c re_stop
  qserver -c re_abort
  qserver -c re_halt

  http POST http://localhost:60610/re/resume
  http POST http://localhost:60610/re/stop
  http POST http://localhost:60610/re/abort
  http POST http://localhost:60610/re_halt

There is minimal user protection features implemented that will prevent execution of
the commands that are not supported in current state of the server. Error messages are printed
in the terminal that is running the server along with output of Run Engine.

Data on executed plans, including stopped plans, is recorded in the history. History can
be downloaded at any time::

  qserver -c history_get
  http GET http://localhost:60610/history/get

History is not intended for long-term storage. It can be cleared at any time::

  qserver -c history_clear
  http POST http://localhost:60610/history/clear

Stop RE Manager (exit RE Manager application). There are two options: safe request that is rejected
when the queue is running or a plan is paused::

  qserver -c manager_stop
  qserver -c manager_stop -p safe_on

  echo '{}' | http POST http://localhost:60610/manager/stop
  http POST http://localhost:60610/manager/stop option="safe_on"

Manager can be also stopped at any time using unsafe stop, which causes current RE Worker to be
destroyed even if a plan is running::

  qserver -c manager_stop -p safe_off
  http POST http://localhost:60610/manager/stop option="safe_off"

The 'test_manager_kill' request is designed specifically for testing ability of RE Watchdog
to restart malfunctioning RE Manager process. This command stops event loop of RE Manager process
and causes RE Watchdog to restart the process (currently after 5 seconds). RE Manager
process is expected to fully recover its state, so that the restart does not affect
running or paused plans or the state of the queue. Another potential use of the request
is to test handling of communication timeouts, since RE Manager does not respond to the request::

  qserver -c test_manager_kill
  http POST http://localhost:60610/test/manager/kill
