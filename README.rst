===================
bluesky-queueserver
===================

.. image:: https://img.shields.io/pypi/v/bluesky-queueserver.svg
        :target: https://pypi.python.org/pypi/bluesky-queueserver

.. image:: https://img.shields.io/codecov/c/github/bluesky/bluesky-queueserver
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

- Loading beamlines' startup scripts or modules.

- Adding and removing plans from the queue; rearranging plans in the queue.

- Starting/stopping execution of the queue.

- Control of the running plans: pausing (immediate and deferred), resuming and stopping
  (stop, abort and halt) the running plan.

- Saving data to Data Broker.

- Streaming documents via Kafka.


In some cases the program may crash and leave some sockets open. This may prevent the Manager from
restarting. To close the sockets (we are interested in sockets on ports 60615 and 60610), find
PIDs of the processes::

  $ netstat -ltnp

and then kill the processes::

  $ kill -9 <pid>


Installation
------------
(see documentation at https://blueskyproject.io/bluesky-queueserver)

.. note::

  The Web (HTTP) server is no longer part of Queue Server project. The code was moved to the
  separate repository https://github.com/bluesky/bluesky-httpserver.

Starting QueueServer
--------------------

Running the demo requires two shells: the first to run Queue Server (RE Manager) and the second shell
to communicate with the manager using `qserver` CLI tool.

In the first shell start RE Manager::

  start-re-manager

RE Manager supports a number of command line options. Use 'start-re-manager -h' to view
the available options.

RE Manager is controlled by sending message over 0MQ. The `qserver` CLI tool allows to interact with
RE Manager and supports most of the API. To display available options use ::

  qserver -h

Interacting with RE Manager using 'qserver' CLI tool
----------------------------------------------------

The most basic request is 'ping' intended to fetch some response from RE Manager::

  qserver ping

Current default address of RE Manager is set to tcp://localhost:60615, but different
address may be passed as a parameter to CLI tool::

  qserver ping -a "tcp://localhost:60615"

The 'qserver' CLI tool may run in the monitoring mode (send 'ping' request to RE Manager every second)::

  qserver monitor

Currently 'ping' request returns the status of RE Manager, but the returned data may change. The recommended
way to fetch status of RE Manager is to use 'status' request::

  qserver status

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

  qserver environment open

Close RE environment::

  qserver environment close

Destroy RE environment::

  qserver environment destroy

Get the lists (JSON) of allowed plans and devices::

  qserver allowed plans
  qserver allowed devices

The list of allowed plans and devices is generated based on the list of existing plans and devices
('existing_plans_and_devices.yaml' by default) and user group permissions ('user_group_permissions.yaml'
by default). The files with permission data are loaded at RE Manager startup. If any of the files
are changed while RE Manager is running (e.g. a new plan was added to the profile collection and
the new 'existing_plans_and_devices.yaml' file was generated) and restarting RE Manager is not
desirable, the data can be reloaded by sending 'permissions_reload' request::

  qserver permissions reload

Before plans could be executed they should be placed in the **plan queue**. The plan queue contains
**items**. The items are **plans** that could be executed by Run Engine or **instructions** that
can modify the state of the queue or RE Manager. Currently only one instruction ('queue_stop' - stops
execution of the queue) is supported.

Push a new plan to the back of the queue::

  qserver queue add plan '{"name":"count", "args":[["det1", "det2"]]}'
  qserver queue add plan '{"name":"scan", "args":[["det1", "det2"], "motor", -1, 1, 10]}'
  qserver queue add plan '{"name":"count", "args":[["det1", "det2"]], "kwargs":{"num":10, "delay":1}}'

It takes 10 second to execute the third plan in the group above, so it is may be the most convenient for testing
pausing/resuming/stopping of experimental plans.

API for queue operations is designed to work identically with items of all types. For example, a 'queue_stop`
instruction can be added to the queue `queue_item_add` API::

  qserver queue add instruction queue-stop

An item can be added at any position of the queue. Push a plan to the front or the back of the queue::

  qserver queue add plan front '{"name":"count", "args":[["det1", "det2"]]}'
  qserver queue add plan back '{"name":"count", "args":[["det1", "det2"]]}'
  qserver queue add plan 2 '{"name":"count", "args":[["det1", "det2"]]}'  # Inserted at pos #2 (0-based)

The following command will insert an item in place of the last item in the queue; the last item remains
the last item in the queue::

  qserver queue add plan -1 '{"name":"count", "args":[["det1", "det2"]]}'

An item can be inserted before or after an existing item with given Item UID.
Insert the plan before an existing item with <uid>::

  qserver queue add plan before_uid '<uid>' '{"name":"count", "args":[["det1", "det2"]]}'

Insert the plan after an existing item with <uid>::

  qserver queue add plan after_uid '<uid>' '{"name":"count", "args":[["det1", "det2"]]}'

If the queue has 5 items (0..4), then the following command pushes the new plan to the back of the queue::

  qserver queue add plan 5 '{"name":"count", "args":[["det1", "det2"]]}'

The 'queue_item_add' request will accept any index value. If the index is out of range, then the item will
be pushed to the front or the back of the queue. If the queue is currently running, then it is recommended
to access elements using negative indices (counted from the back of the queue).

The names of the plans and devices are strings. The strings are converted to references to Bluesky plans and
Ophyd devices in the worker process. The simulated beamline profile collection includes all simulated
Ophyd devices and built-in Bluesky plans.

A batch of plans may be submitted to the queue by sending a single request. Every plan in the batch
is validated and the plans are added to the queue only if all plans pass validation. Otherwise the
batch is rejected. Currently `qserver` does not support API for batch operations. "args":[["det1"]], "item_type": "plan"}, {"name":"count", "args":[["det2"]], "item_type": "plan"}]'

Queue Server API allow to execute a single item (plan or instruction) submitted with the API call. Execution
of an item starts immediately if possible (RE Manager is idle and RE Worker environment exists), otherwise
API call fails and the item is not added to the queue. The following commands start execution of a single plan::

  qserver queue execute plan '{"name":"count", "args":[["det1", "det2"]], "kwargs":{"num":10, "delay":1}}'

Queue can be edited at any time. Changes to the running queue become effective the moment they are
performed. As the currently running plan is finished, the new plan is popped from the top of the queue.

The contents of the queue may be fetched at any time::

  qserver queue get

The last item can be removed (popped) from the back of the queue::

  qserver queue item remove
  qserver queue item remove back

The position of the removed item may be specified similarly to `queue_item_add` request with the difference
that the position index must point to the existing element, otherwise the request fails (returns 'success==False').
The following examples remove the plan from the front of the queue and the element previous to last::

  qserver queue item remove front
  qserver queue item remove -p -2

The items can also be addressed by UID. Remove the item with <uid>::

  qserver queue item remove '<uid>'

Items can be read from the queue without changing it. `queue_item_get` requests are formatted identically to
`queue_item_remove` requests::

  qserver queue item get
  qserver queue item get back
  qserver queue item get front
  qserver queue item get -2
  qserver queue item get '<uid>'

Items can be moved within the queue. Items can be addressed by position or UID. If positional addressing
is used then items are moved from 'source' position to 'destination' position.
If items are addressed by UID, then the item with <uid_source> is inserted before or after
the item with <uid_dest>::

  qserver queue item move 3 5
  qserver queue item move <uid_source> before <uid_dest>
  qserver queue item move <uid_source> after <uid_dest>

Addressing by position and UID can be mixed. The following instruction will move queue item #3
to the position following an item with <uid_dest>::

  qserver queue item move 3 after <uid_dest>

The following instruction moves item with <uid_source> to the front of the queue::

  qserver queue item move <uid_source> "front"

The parameters of queue items may be updated or replaced. When the item is replaced, it is assigned a new
item UID, while if the item is updated, item UID remains the same. The commands implementing those
operations do not distinguish plans and instructions, i.e. an instruction may be updated/replaced
by a plan or a plan by an instruction. The operations may be performed using CLI tool by calling
*'queue update'* and *'queue replace'* with parameter *<existing-uid>* being item UID of the item in the
queue which is being replaced followed by the JSON representation of the dictionary of parameters
of the new item::

  qserver queue update plan <existing-uid> {"name":"count", "args":[["det1", "det2"]]}'
  qserver queue update instruction <existing-uid> {"action":"queue_stop"}
  qserver queue replace plan <existing-uid> {"name":"count", "args":[["det1", "det2"]]}'
  qserver queue replace instruction <existing-uid> {"action":"queue_stop"}

Remove all entries from the plan queue::

  qserver queue clear

The plan queue can operate in LOOP mode, which is disabled by default. To enable or disable the LOOP mode
the following commands::

  qserver queue mode set loop True
  qserver queue mode set loop False

Start execution of the plan queue. The environment MUST be opened before queue could be started::

  qserver queue start

Request to execute an empty queue is a valid operation that does nothing.

As the queue is running, the list of active runs (runs generated by the running plan may be obtained
at any time). The set of active runs consists of two subsets: open runs and closed runs. For
simple single-run plans the list will contain only one item. The list can be loaded using CLI
commands and HTTP API::

  qserver re runs            # Get the list of active runs (runs generated by the currently running plans)
  qserver re runs active     # Get the list of active runs
  qserver re runs open       # Get the list of open runs (subset of active runs)
  qserver re runs closed     # Get the list of closed runs (subset of active runs)

The queue can be stopped at any time. Stopping the queue is a safe operation. When the stopping
sequence is initiated, the currently running plan is finished and the next plan is not be started.
The stopping sequence can be cancelled if it was activated by mistake or decision was changed::

  qserver queue stop
  qserver queue stop cancel

While a plan in a queue is executed, operation Run Engine can be paused. In the unlikely event
if the request to pause is received while RunEngine is transitioning between two plans, the request
may be rejected by the RE Worker. In this case it needs to be repeated. If Run Engine is in the paused
state, plan execution can be resumed, aborted, stopped or halted. If the plan is aborted, stopped
or halted, it is not removed from the plan queue (it remains the first in the queue) and execution
of the queue is stopped. Execution of the queue may be started again if needed.

Running plan can be paused immediately (returns to the last checkpoint in the plan) or at the next
checkpoint (deferred pause)::

  qserver re pause
  qserver re pause deferred
  qserver re pause immediate

Resuming, aborting, stopping or halting of currently executed plan::

  qserver re resume
  qserver re stop
  qserver re abort
  qserver re halt

There is minimal user protection features implemented that will prevent execution of
the commands that are not supported in current state of the server. Error messages are printed
in the terminal that is running the server along with output of Run Engine.

Data on executed plans, including stopped plans, is recorded in the history. History can
be downloaded at any time::

  qserver history get

History is not intended for long-term storage. It can be cleared at any time::

  qserver history clear

Stop RE Manager (exit RE Manager application). There are two options: safe request that is rejected
when the queue is running or a plan is paused::

  qserver manager stop
  qserver manager stop safe_on

Manager can be also stopped at any time using unsafe stop, which causes current RE Worker to be
destroyed even if a plan is running::

  qserver manager stop safe_off

The 'test_manager_kill' request is designed specifically for testing ability of RE Watchdog
to restart malfunctioning RE Manager process. This command stops event loop of RE Manager process
and causes RE Watchdog to restart the process (currently after 5 seconds). RE Manager
process is expected to fully recover its state, so that the restart does not affect
running or paused plans or the state of the queue. Another potential use of the request
is to test handling of communication timeouts, since RE Manager does not respond to the request::

  qserver manager kill test
