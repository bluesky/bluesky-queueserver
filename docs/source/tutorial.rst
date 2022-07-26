===============
Basic Tutorials
===============

The purpose of the tutorials in this section is to explore basic features of the Queue Server.
The tutorials also contain brief explanations of Queue Server features and API
and recommendations on how and when they should be used.

Most tutorials require two terminals:
one terminal is used to start Run Engine (RE) manager and display logging
messages and console output from plans; the other terminal is used to interact
with RE Manager by running CLI commands.

The RE Manager is the central component of the Queue Server, which
maintains the queue of plans, creates RE Worker environment (a process where plans are
executed) and provides flexible low level 0MQ API for client applications.
The following tutorials provide instructions on how to interact with RE Manager
using ``qserver`` CLI tool, which may be considered a basic client application.
`qserver` functionality is limited to sending single 0MQ API requests and
displaying received responses, but it provides direct access
to almost every low level API (except batch queue operations). Use ``qserver -h``
to display brief help and full set of options or read the manual at :ref:`qserver_cli`.

In production systems, users are more likely to interact with RE Manager
using custom GUI applications or IPython API (currently under development),
but ``qserver`` may still be a convenient tool for exploring or demonstrating
Queue Server features or testing and debugging deployed systems.
The instructions on how to interact with RE Manager using ``qserver`` tool are
also applicable to low level 0MQ API, which could be used in Python programs.
Lists of used 0MQ API with links to documentation are included at the end of
each tutorial.

.. _tutorial_starting_queue_server:

Starting the Queue Server
-------------------------

In the first terminal, start RE Manager as a console application::

  $ start-re-manager
  [I 2022-02-17 06:54:18,077 bluesky_queueserver.manager.manager] Starting ZMQ server at 'tcp://*:60615'
  [I 2022-02-17 06:54:18,077 bluesky_queueserver.manager.manager] ZMQ control channels: encryption disabled
  [I 2022-02-17 06:54:18,080 bluesky_queueserver.manager.manager] Starting RE Manager process
  [I 2022-02-17 06:54:18,096 bluesky_queueserver.manager.manager] Loading the lists of allowed plans and devices ...
  [I 2022-02-17 06:54:18,341 bluesky_queueserver.manager.manager] Starting ZeroMQ server ...
  [I 2022-02-17 06:54:18,343 bluesky_queueserver.manager.manager] ZeroMQ server is waiting on tcp://*:60615

RE Manager functionality may be customized using CLI parameters. The default settings
are selected specifically for running simple demonstrations, so RE Manager may be started
without parameters in most of the tutorials. Type ``start-re-manager -h`` to display the
full set of supported parameters. More detailed description may be found in
:ref:`the application manual <start_re_manager_cli>`.

The console output of RE Manager contains logging messages of the server and Bluesky and
text output of the executed plans (such as Live Tables). RE Manager may be configured
to publish console output to 0MQ socket so that it could be streamed to other
applications (see :ref:`remote_monitoring_tutorial`). Production deployments
of the Queue Server are likely to run RE Manager as a service,
but starting it as an console application is very simple and recommended for tutorials,
demonstrations and software development and testing.

The easiest way to test if the Queue Server is running and accessible is to call the ``status`` API::

  $ qserver status
  06:55:49 - MESSAGE:
  {'devices_allowed_uid': '42ebeb34-cc00-41ff-96ec-9cb4210d0b10',
  'devices_existing_uid': 'adc31393-3604-4765-b7c0-be25da34b9ec',
  'items_in_history': 6,
  'items_in_queue': 5,
  'manager_state': 'idle',
  'msg': 'RE Manager',
  'pause_pending': False,
  'plan_history_uid': '658ac3e5-ece3-4947-833f-293f8ec27687',
  'plan_queue_mode': {'loop': False},
  'plan_queue_uid': 'd409d889-1788-4da6-8af8-05456f63401c',
  'plans_allowed_uid': '18a7fcf5-fba7-41e8-9872-4379f0537ec9',
  'plans_existing_uid': 'ebad3d13-2106-4e38-89b3-2bc513f3576a',
  'queue_stop_pending': False,
  're_state': None,
  'run_list_uid': 'e9d8449a-4635-42a0-bf8f-2af87d020e67',
  'running_item_uid': None,
  'task_results_uid': 'b281bed4-2b90-4f31-b8c7-64f2626927f1',
  'worker_background_tasks': 0,
  'worker_environment_exists': False,
  'worker_environment_state': 'closed'}

The server always accepts ``status`` API requests and returns the set of parameters
that reflects current state of RE Manager. For example, ``'manager_state': 'idle'``
indicates current state of RE Manager and ``'worker_environment_exists': False`` indicates
if RE Worker environment is open and the server is ready to execute plans (currently
the environment does not exist). Timeout occurs if the server is not accessible or does
not respond in time. The detailed reference to RE Manager API could be found in
the section :ref:`supported_methods_for_0MQ_API`. For example, documentation for
the ``status`` API can be found :ref:`here<method_status>`.

RE Manager application can be stopped at any time by activating pressing ``Ctrl-C``.

.. note::

  When RE Manager is closed using ``Ctrl-C`` key combination, execution of any plans, tasks,
  queue operations etc. is interrupted without warning or asking for confirmation.
  There is no risk of accidentally stopping RE Manager when it is running as a service.

API used in this tutorial: :ref:`method_status`.

.. _tutorial_opening_closing_re_worker_environment:

Opening and Closing RE Worker Environment
-----------------------------------------

Start RE Manager using instructions given in :ref:`tutorial_starting_queue_server`.

In response to the request to open RE Worker Environment, RE Manager creates
a new RE Worker process (for executing Bluesky plans), configures Run Engine and
loads startup code in the RE Worker namespace. RE Manager may load startup code
represented as a set of startup files (IPython style), Python script or module.
``bluesky-queueserver`` package includes
`a set of startup files <https://github.com/bluesky/bluesky-queueserver/tree/main/bluesky_queueserver/profile_collection_sim>`_
with simulated devices and plans sufficient for simple demos. RE Manager
is loading the built-in startup code unless alternative location is specified
(see :ref:`tutorial_running_custom_startup_code`).

Open the RE Worker environment using ``qserver`` CLI tool::

  $ qserver environment open
  07:06:00 - MESSAGE:
  {'msg': '', 'success': True}

The returned parameters include the flag, which indicates if the request was
accepted by the server (``'success': True``) and the error message (``'msg': ''``),
which is an empty string if the request is accepted. The API request only initiates
the process of opening the environment, which may take significant time.
The returned result ``'success': True`` does not mean that the environment was successfully loaded
or loaded at all. To find if the environment was loaded, check the status of RE Manager::

  $ qserver status
  07:15:15 - MESSAGE:
  { ...
  'manager_state': 'idle',
    ...
  'worker_environment_exists': True,
  'worker_environment_state': 'idle'}

The most likely reason for failure to open an environment is an exception raised in the startup
code. Search the console output of RE Manager for error messages and traceback.

Repeated requests to open the environment are rejected by the server::

  $ qserver environment open
  07:47:59 - MESSAGE:
  {'msg': 'RE Worker environment already exists.', 'success': False}

The environment must be opened before executing any plans. The request to start
the plan queue is rejected if the environment closed. All queue operations,
including adding/removing/moving plans, do not require open environment.
The process of opening an environment may indirectly affect the queue operations,
because it involves generating new lists of existing/allowed plans and devices
based on loaded startup scripts (see :ref:`plan_validation`).

The operation of closing RE Worker environment involves orderly exit from
the message processing loop and closing the worker process.
Closing the environment is safe, since it may be executed only
if no plans or foreground tasks are running. The requests are rejected
if the environment is busy.

Close the RE Worker environment using ``qserver`` CLI tool::

  $ qserver environment close
  07:48:53 - MESSAGE:
  {'msg': '', 'success': True}

The API request only initiates the process of closing the environment. Check RE Manager status
to determine if the environment was closed successfully::

  $ qserver status
  07:15:15 - MESSAGE:
  { ...
  'manager_state': 'idle',
    ...
  'worker_environment_exists': False,
  'worker_environment_state': 'closed'}

Repeated requests to close the environment are rejected::

  $ qserver environment close
  07:49:46 - MESSAGE:
  {'msg': 'RE Worker environment does not exist.', 'success': False}

RE Worker Environment is designed to run user code in the form of Bluesky plans or user defined
functions. If the main thread gets stuck in an infinite loop or inifinite wait (e.g. waits for
non-responding PV without timeout), the environment may become unresponsive and can not be closed.
This may cause substantial inconvenience during remote operation of the beamline. RE Manager
supports an API that allow to recover from this state by destroying an unresponsive environment
(killing the RE Worker process). After the environment is destroyed, a new environment may be opened
and operation resumed. The operation of destroying an environment is unsafe, and accidentally
sending the request during normal operation kills any running plans or tasks.

The process of destroying the RE Worker environment is initiated using the following command::

  $ qserver environment destroy
  07:50:25 - MESSAGE:
  {'msg': '', 'success': True}

It may take a little time for the operation to complete. Check the status to verify that
the environment is in closed state and RE Manager is idle.

API used in this tutorial: :ref:`method_status`, :ref:`method_environment_open`,
:ref:`method_environment_close`, :ref:`method_environment_destroy`.

.. _tutorial_adding_queue_items:

Adding Items to Queue
---------------------

Queue operations, such as adding and removing items, replacing or moving existing items,
may be performed at any time. The environment does not need to be opened to manipulate the queue.
Queue Server performs validation of the submitted plans and rejects plans that do not exist or
the plans that are not allowed to be executed by the user. Plans may accept devices
as parameter values. The devices must be in the list of allowed devices for the user
submitting the plan, otherwise the plan is rejected (if plan parameters are validated) or
fail during plan execution.

Start RE Manager using instructions given in :ref:`tutorial_starting_queue_server`.

Display the lists of allowed plans and devices. Note that the plans ``scan`` and ``count`` are
in the list of allowed plans and ``det1``, ``det2`` and ``motor`` are in the list of allowed devices.
`qserver` tool displays only the set top-level device names, but subdevice names can also
be used as plan parameters::

  $ qserver allowed plans
  09:27:52 - MESSAGE:
  {'msg': '',
  'plans_allowed': {'adaptive_scan': '{...}',
                    'count': '{...}',
                    'count_bundle_test': '{...}',
                    ...
                    'relative_inner_product_scan': '{...}',
                    'scan': '{...}',
                    ...
                    'x2x_scan': '{...}'},
  'plans_allowed_uid': '18a7fcf5-fba7-41e8-9872-4379f0537ec9',
  'success': True}

  $ qserver allowed devices
  09:31:45 - MESSAGE:.;
  {'devices_allowed': {'ab_det': '{...}',
                      ...
                      'det': '{...}',
                      'det1': '{...}',
                      'det2': '{...}',
                      'det3': '{...}',
                      'det4': '{...}',
                      'det5': '{...}',
                      ...
                      'motor': '{...}',
                      'motor1': '{...}',
                      'motor2': '{...}',
                      'motor3': '{...}',
                      ...
                      'sim_bundle_A': '{...}',
                      'sim_bundle_B': '{...}'},
  'devices_allowed_uid': '42ebeb34-cc00-41ff-96ec-9cb4210d0b10',
  'msg': '',
  'success': True}

First let's clear the queue, since it may already contain plans::

  $ qserver queue clear
  10:08:09 - MESSAGE:
  {'msg': '', 'success': True}

Verify that the number of items in the queue is zero::

  $ qserver status
  10:08:25 - MESSAGE:
  { ...
  'items_in_queue': 0,
  ... }

Load the contents of the queue (``item``), which should be empty at this point::

  $ qserver queue get
  10:08:35 - MESSAGE:
  {'items': [],
  'msg': '',
  'plan_queue_uid': '5ae71b0f-c671-4ce3-93bb-b854296dd4f8',
  'running_item': {},
  'success': True}

Now let's add the plan ``count([det1, det2], num=10, delay=2)`` to the queue::

  $ qserver queue add plan '{"name": "count", "args": [["det1", "det2"]], "kwargs": {"num": 10, "delay": 1}}'
  10:04:49 - MESSAGE:
  {'item': {'args': [['det1', 'det2']],
            'item_type': 'plan',
            'item_uid': '0aa7f1be-3923-4d67-ba7b-b19d26ec6291',
            'kwargs': {'delay': 1, 'num': 10},
            'name': 'count',
            'user': 'qserver-cli',
            'user_group': 'admin'},
  'msg': '',
  'qsize': 1,
  'success': True}

The submitted plan was accepted by the server and added to the queue. The parameter ``'qsize': 1``
shows the new size of the plan queue. Verify the queue size and load the updated queue::

  $ qserver status
  10:08:25 - MESSAGE:
  { ...
  'items_in_queue': 1,
  ... }

  $ qserver queue get
  10:16:43 - MESSAGE:
  {'items': [{'args': [['det1', 'det2']],
              'item_type': 'plan',
              'item_uid': 'af4169c0-1d9c-4412-ad0b-5a232e1b13e7',
              'kwargs': {'delay': 1, 'num': 10},
              'name': 'count',
              'user': 'qserver-cli',
              'user_group': 'admin'}],
  'msg': '',
  'plan_queue_uid': 'dfad1d60-abd9-4bd9-895c-10b7c2dc8897',
  'running_item': {},
  'success': True}

The items are added to the back of the queue by default. Let's add another plan
``scan([det1, det2], motor, -1, 1, 10)`` to the queue::

  $ qserver queue add plan '{"name": "scan", "args": [["det1", "det2"], "motor", -1, 1, 10]}'
  10:21:37 - MESSAGE:
  {'item': {'args': [['det1', 'det2'], 'motor', -1, 1, 10],
            'item_type': 'plan',
            'item_uid': '17e45208-b8d7-4545-9bd6-d6aa7263adc9',
            'name': 'scan',
            'user': 'qserver-cli',
            'user_group': 'admin'},
  'msg': '',
  'qsize': 2,
  'success': True}

Note that the queue size is now 2. Load the list of queue items and verify that the ``scan`` plan
is added to the back of the queue::

  $ qserver queue get
  10:24:24 - MESSAGE:
  {'items': [{'args': [['det1', 'det2']],
              'item_type': 'plan',
              'item_uid': 'af4169c0-1d9c-4412-ad0b-5a232e1b13e7',
              'kwargs': {'delay': 1, 'num': 10},
              'name': 'count',
              'user': 'qserver-cli',
              'user_group': 'admin'},
            {'args': [['det1', 'det2'], 'motor', -1, 1, 10],
              'item_type': 'plan',
              'item_uid': '17e45208-b8d7-4545-9bd6-d6aa7263adc9',
              'name': 'scan',
              'user': 'qserver-cli',
              'user_group': 'admin'}],
  'msg': '',
  'plan_queue_uid': '29d6b8fe-7100-4bdc-b348-845cc2728d1b',
  'running_item': {},
  'success': True}

The RE Manager API supports an extensive set of options to define the location of inserted plans.
For example a plan may be inserted to the front of the queue::

  $ qserver queue add plan front '{"name": "scan", "args": [["det1"], "motor", -2, 2, 5]}'
  10:29:09 - MESSAGE:
  {'item': {'args': [['det1'], 'motor', -2, 2, 5],
            'item_type': 'plan',
            'item_uid': '3a6ae812-5d59-4f05-bfad-67e4f8a798e2',
            'name': 'scan',
            'user': 'qserver-cli',
            'user_group': 'admin'},
  'msg': '',
  'qsize': 3,
  'success': True}

Verify that the new plan was inserted to the front of the queue::

  $ qserver queue get
    10:30:00 - MESSAGE:
    {'items': [{'args': [['det1'], 'motor', -2, 2, 5],
                'item_type': 'plan',
                'item_uid': '3a6ae812-5d59-4f05-bfad-67e4f8a798e2',
                'name': 'scan',
                'user': 'qserver-cli',
                'user_group': 'admin'},
              {'args': [['det1', 'det2']],
                'item_type': 'plan',
                'item_uid': 'af4169c0-1d9c-4412-ad0b-5a232e1b13e7',
                'kwargs': {'delay': 1, 'num': 10},
                'name': 'count',
                'user': 'qserver-cli',
                'user_group': 'admin'},
              {'args': [['det1', 'det2'], 'motor', -1, 1, 10],
                'item_type': 'plan',
                'item_uid': '17e45208-b8d7-4545-9bd6-d6aa7263adc9',
                'name': 'scan',
                'user': 'qserver-cli',
                'user_group': 'admin'}],
    'msg': '',
    'plan_queue_uid': 'ba87dce1-c598-4a4a-a801-3e145e9b4365',
    'running_item': {},
    'success': True}

The queue may contain instructions that are executed by RE Manager and
control execution of the queue. The only supported instruction is ``queue_stop``,
which stops execution of the queue (for example to let the operator change
a sample). The queue can be restarted afterwards. The following
command will insert the instruction before the element at position ``-2``
in the queue::

  $ qserver queue add instruction -2 queue-stop
  10:36:31 - MESSAGE:
  {'item': {'item_type': 'instruction',
            'item_uid': 'e2fcb2b6-a968-4e36-a345-47416b3814b0',
            'name': 'queue_stop',
            'user': 'qserver-cli',
            'user_group': 'admin'},
  'msg': '',
  'qsize': 4,
  'success': True}

  $ qserver queue get
  10:36:40 - MESSAGE:
  {'items': [{'args': [['det1'], 'motor', -2, 2, 5],
              'item_type': 'plan',
              'item_uid': '3a6ae812-5d59-4f05-bfad-67e4f8a798e2',
              'name': 'scan',
              'user': 'qserver-cli',
              'user_group': 'admin'},
            {'item_type': 'instruction',
              'item_uid': 'e2fcb2b6-a968-4e36-a345-47416b3814b0',
              'name': 'queue_stop',
              'user': 'qserver-cli',
              'user_group': 'admin'},
            {'args': [['det1', 'det2']],
              'item_type': 'plan',
              'item_uid': 'af4169c0-1d9c-4412-ad0b-5a232e1b13e7',
              'kwargs': {'delay': 1, 'num': 10},
              'name': 'count',
              'user': 'qserver-cli',
              'user_group': 'admin'},
            {'args': [['det1', 'det2'], 'motor', -1, 1, 10],
              'item_type': 'plan',
              'item_uid': '17e45208-b8d7-4545-9bd6-d6aa7263adc9',
              'name': 'scan',
              'user': 'qserver-cli',
              'user_group': 'admin'}],
  'msg': '',
  'plan_queue_uid': 'bc66304a-2cd3-430a-acae-1b2152b60dba',
  'running_item': {},
  'success': True}

Note, that using negative indices to address queue items (counting items
from the back of the queue) is more reliable, since queue operations could
be performed while the queue is running and items may be removed from
the front of the queue at any moment. Alternatively, items may be addressed
using ``item_uid``, which is never changed by the queue operations.

API used in this tutorial: :ref:`method_status`, :ref:`method_queue_item_add`,
:ref:`method_queue_get`, :ref:`method_queue_clear`, :ref:`method_plans_allowed`,
:ref:`method_devices_allowed`.

.. _tutorial_starting_stopping_queue:

Starting and Stopping the Queue
-------------------------------

Start RE Manager using instructions given in :ref:`tutorial_starting_queue_server`.

Clear the queue and add a few plans to the queue as described in :ref:`tutorial_adding_queue_items`.
For this tutorial, it is recommended to use plans that take relatively long time
to execute. For example the following plan runs for about 20 seconds::

  $ qserver queue add plan '{"name": "count", "args": [["det1", "det2"]], "kwargs": {"num": 10, "delay": 2}}'

In the following example we assume that the queue contains three ``count`` plans with the queue
execution time about 60 seconds::

  $ qserver queue get
  13:07:40 - MESSAGE:
  {'items': [{'args': [['det1', 'det2']],
              'item_type': 'plan',
              'item_uid': 'fffa482a-f655-4999-9e90-1d6550f67b72',
              'kwargs': {'delay': 2, 'num': 10},
              'name': 'count',
              'user': 'qserver-cli',
              'user_group': 'admin'},
            {'args': [['det1', 'det2']],
              'item_type': 'plan',
              'item_uid': '7426b43b-102f-42f1-a43e-2c3f2b9009a7',
              'kwargs': {'delay': 2, 'num': 10},
              'name': 'count',
              'user': 'qserver-cli',
              'user_group': 'admin'},
            {'args': [['det1', 'det2']],
              'item_type': 'plan',
              'item_uid': '859760ef-51ad-4861-832c-b113b008fa3e',
              'kwargs': {'delay': 2, 'num': 10},
              'name': 'count',
              'user': 'qserver-cli',
              'user_group': 'admin'}],
  'msg': '',
  'plan_queue_uid': 'a20c74fe-0888-4e61-9a37-4fbc9697fe3d',
  'running_item': {},
  'success': True}

Open the environment as described in :ref:`tutorial_opening_closing_re_worker_environment`.

Every plan that is executed by RE Manager is added to the plan history. The history
is not designed to for long-term storage and must be periodically cleared::

  $ qserver history clear
  11:51:11 - MESSAGE:
  {'msg': '', 'success': True}

The number of items in the history is reported as part RE Manager status::

  $ qserver status
  12:01:14 - MESSAGE:
  { ...
  'items_in_history': 0,
  'items_in_queue': 3,
  ... }

Start the queue and observe the logging messages and Live Table displayed in
the terminal running RE Manager (``'success': True`` indicates that the request
was accepted by the server and the queue is about to get started)::

  $ qserver queue start
  12:05:15 - MESSAGE:
  {'msg': '', 'success': True}

While the first plan is still running, check the contents of the queue:
``running_item`` is a dictionary of parameters of the currently running plan
and ``items`` is a list of the plans remaining in the queue::

  $ qserver queue get
  Arguments: ['queue', 'get']
  13:07:54 - MESSAGE:
  {'items': [{'args': [['det1', 'det2']],
              'item_type': 'plan',
              'item_uid': '7426b43b-102f-42f1-a43e-2c3f2b9009a7',
              'kwargs': {'delay': 2, 'num': 10},
              'name': 'count',
              'user': 'qserver-cli',
              'user_group': 'admin'},
            {'args': [['det1', 'det2']],
              'item_type': 'plan',
              'item_uid': '859760ef-51ad-4861-832c-b113b008fa3e',
              'kwargs': {'delay': 2, 'num': 10},
              'name': 'count',
              'user': 'qserver-cli',
              'user_group': 'admin'}],
  'msg': '',
  'plan_queue_uid': '4948d6ba-586c-4a70-a1f9-f933124c1e58',
  'running_item': {'args': [['det1', 'det2']],
                    'item_type': 'plan',
                    'item_uid': 'fffa482a-f655-4999-9e90-1d6550f67b72',
                    'kwargs': {'delay': 2, 'num': 10},
                    'name': 'count',
                    'user': 'qserver-cli',
                    'user_group': 'admin'},
  'success': True}

Once all plans are completed, verify RE Manager status to make sure that
the queue is empty and the correct number of plans were added to history::

  $ qserver status
  12:16:31 - MESSAGE:
  { ...
  'items_in_history': 3,
  'items_in_queue': 0,
  ... }

All functions for manipulating the queue are accessible while the queue is running.
Add a few plans to the queue, start the queue and try adding plans to the queue while
it is running. Check the contents of the queue (``qserver queue get``) to observe
changes.

RE Manager supports an API that allows to stop execution of the queue after
completion of the current plan. This API is intended to be used in cases when
the currently running plan should be normally completed, but some intervention
by the operator (e.g. adjustment of the sample) is needed before the next plan
is started. The API call does not influence execution of currently running plan.

Add more plans to the queue and start the queue. While the first plan is running
use the following command to stop the queue::

  $ qserver queue stop
  2:19:01 - MESSAGE:
  {'msg': '', 'success': True}

While the plan is still running, check that the current state is reflected in
the RE Manager status (``queue_stop_pending``)::

  $ qserver status
  12:19:05 - MESSAGE:
  { ...
  'manager_state': 'executing_queue',
  ...
  'queue_stop_pending': True,
  ... }

Observe that the queue stops after the current plan is completed. Note, that the
sequence of commands (``qserver queue start``, ``qserver queue stop`` and ``qserver status``)
must be issued while the plan is running. Increase the values of ``num`` or ``delay``
plan parameters to make the plan run longer if needed.

Since plans may take long time (potentially hours) to execute and an operator may send the API request
to stop the queue by mistake or change the decision while the plan is running, RE Manager
allows to cancel the pending request to stop the queue. Execute the following commands in rapid
sequence while the plan is still running to observe change in ``queue_stop_pending`` status
parameter::

  $ qserver queue start

  $ qserver queue stop

  $ qserver status
  12:41:38 - MESSAGE:
  { ...
  'manager_state': 'executing_queue',
  ...
  'queue_stop_pending': True,
  ... }

  $ qserver queue stop cancel

  $ qserver status
  12:41:46 - MESSAGE:
  { ...
  'manager_state': 'executing_queue',
  ...
  'queue_stop_pending': False,
  ... }

Execution of the plans will continue until the queue is empty.

API used in this tutorial: :ref:`method_status`, :ref:`method_queue_start`, :ref:`method_queue_stop`,
:ref:`method_queue_stop_cancel`, :ref:`method_history_clear`.

.. _tutorial_iteracting_with_run_engine:

Interacting with Run Engine
---------------------------

RE Manager hides most of the low level details related to execution of plans,
but some functionality relevant to Run Engine monitoring and control is
accessible via 0MQ API:

- Status parameters: ``re_state`` indicating current state of the Run Engine and
  ``pause_pending`` which indicates if deferred pause is pending at Run Engine.

- 0MQ API for pausing, resuming, stopping, aborting or halting the running plan.
  See `Bluesky documentation <https://blueskyproject.io/bluesky/state-machine.html#interruptions>`_
  for more detailed information on how Run Engine is handling plan interruptions.

Run Engine is not instantiated if the RE Worker environment is closed and
``re_state`` is always ``None`` and ``pause_pending`` is ``False``::

  $ qserver status
  14:59:09 - MESSAGE:
  { ...
  'pause_pending': False,
  ...
  're_state': None,
  ... }

If environment is open (see :ref:`tutorial_opening_closing_re_worker_environment`),
then ``re_state`` is a string that represents actual state of the Run Engine::

  $ qserver status
  16:19:30 - MESSAGE:
  { ...
  'pause_pending': False,
  ...
  're_state': 'idle',
  ... }

The operations that interrupt execution of currently running plan are handled by
the Run Engine. RE Manager provides API for initiating plan interruptions, including
pausing the plan, and then resuming, stopping, aborting or halting the paused plan.
Note, that the API for stopping the queue and stopping the paused plan are not related,
except that the queue is automatically stopped if the plan is stopped, aborted, halted
or fails to complete in any other way.

It is assumed that the RE Worker environment is open. Add a plan to the queue.
The following plan runs for one minute and should work well for the demonstration::

  $ qserver queue add plan '{"name": "count", "args": [["det1", "det2"]], "kwargs": {"num": 6, "delay": 10}}'

``count`` plan contains a checkpoint before each measurement. The API allow to initiate
deferred and immediate pause. In case of deferred pause (equivalent to single Ctrl-C in IPython)
the plan is executed until the next checkpoint, i.e. the current measurement is completed
and the next measurement is started once the plan is resumed. In case of immediate pause
(double Ctrl-C in IPython) the plan is rolled back to the previous checkpoint and the current
measurement is repeated once the plan is resumed. The plan performs 6 measurments with the
period of 10 seconds between measurements, so it is easy to observer how operations of pausing
and resuming the plans works::

  # Start the queue
  $ qserver queue start

  # Request the deferred pause
  $ qserver re pause
  16:59:59 - MESSAGE:
  {'msg': '', 'success': True}

  # Check status while the plan is still running, but deferred pause is pending
  $ qserver status
  Arguments: ['status']
  { ...
  'manager_state': 'executing_queue',
  ...
  'pause_pending': True,
  ...
  're_state': 'running',
  ...}

  # Check status again once the plan is paused (takes a few seconds to reach the next checkpoint)
  $ qserver status
  17:00:25 - MESSAGE:
  { ...
  'manager_state': 'paused',
  ...
  'pause_pending': False,
  ...
  're_state': 'paused',
  ...}

  # Resume the plan
  $ qserver re resume
  17:07:08 - MESSAGE:
  {'msg': '', 'success': True}

The output of RE Manager contains the following Live Table. Note, that the measurement #1
was fully completed and not repeated after the plan was resumed::

  Transient Scan ID: 1     Time: 2022-02-17 16:59:53
  Persistent Unique Scan ID: 'fc9f444e-9a52-4df6-9486-a877f9022528'
  New stream: 'primary'
  +-----------+------------+------------+------------+
  |   seq_num |       time |       det2 |       det1 |
  +-----------+------------+------------+------------+
  |         1 | 16:59:53.1 |      1.765 |      5.000 |
  [I 2022-02-17 16:59:59,198 bluesky_queueserver.manager.manager] Pausing the queue (currently running plan) ...
  [I 2022-02-17 16:59:59,198 bluesky_queueserver.manager.worker] Pausing Run Engine ...
  Deferred pause acknowledged. Continuing to checkpoint.
  Pausing...
  [I 2022-02-17 17:07:08,353 bluesky_queueserver.manager.manager] Resuming paused plan ...
  [I 2022-02-17 17:07:08,353 bluesky_queueserver.manager.worker] Run Engine: resume
  [I 2022-02-17 17:07:08,353 bluesky_queueserver.manager.worker] Continue plan execution with the option 'resume'
  |         2 | 17:07:08.3 |      1.765 |      5.000 |
  |         3 | 17:07:08.3 |      1.765 |      5.000 |
  |         4 | 17:07:18.3 |      1.765 |      5.000 |
  |         5 | 17:07:28.3 |      1.765 |      5.000 |
  |         6 | 17:07:38.3 |      1.765 |      5.000 |
  Run was closed: 'fc9f444e-9a52-4df6-9486-a877f9022528'
  +-----------+------------+------------+------------+
  generator count ['fc9f444e'] (scan num: 1)

The following sequence of commands starts the queue and request immediate pause.
The sequence may be tested with the same plan::

  $ qserver start
  $ qserver re pause immediate
  $ qserver re resume

In the Live Table, measurement #2 was cancelled when the plan was paused
and repeated after the plan was resumed::

  Transient Scan ID: 2     Time: 2022-02-17 17:15:31
  Persistent Unique Scan ID: '76e20bbc-e38c-40ab-a66f-f16745f9baf2'
  New stream: 'primary'
  +-----------+------------+------------+------------+
  |   seq_num |       time |       det2 |       det1 |
  +-----------+------------+------------+------------+
  |         1 | 17:15:31.7 |      1.765 |      5.000 |
  |         2 | 17:15:41.7 |      1.765 |      5.000 |
  [I 2022-02-17 17:15:42,340 bluesky_queueserver.manager.manager] Pausing the queue (currently running plan) ...
  [I 2022-02-17 17:15:42,341 bluesky_queueserver.manager.worker] Pausing Run Engine ...
  Pausing...
  [I 2022-02-17 17:15:52,403 bluesky_queueserver.manager.manager] Resuming paused plan ...
  [I 2022-02-17 17:15:52,403 bluesky_queueserver.manager.worker] Run Engine: resume
  [I 2022-02-17 17:15:52,403 bluesky_queueserver.manager.worker] Continue plan execution with the option 'resume'
  |         2 | 17:15:52.4 |      1.765 |      5.000 |
  |         3 | 17:16:02.4 |      1.765 |      5.000 |
  |         4 | 17:16:12.4 |      1.765 |      5.000 |
  |         5 | 17:16:22.4 |      1.765 |      5.000 |
  |         6 | 17:16:32.5 |      1.765 |      5.000 |
  Run was closed: '76e20bbc-e38c-40ab-a66f-f16745f9baf2'
  +-----------+------------+------------+------------+
  generator count ['76e20bbc'] (scan num: 2)

Once the plan is paused, it can be resumed (as alread demonstrated), stopped, aborted or halted. The
technical difference between the three methods of terminating a plan relatively small, except that
stopped plans is considered successful, aborted and halted plans are considered failed; a new plan
can be started immediately after a plan is stopped or aborted, but the environment needs to be
restarted (closed and opened again) after a plan is halted.

The respective ``qserver``
commands are ::

  $ qserver re stop
  $ qserver re abort
  $ qserver re halt

API used in this tutorial: :ref:`method_status`, :ref:`method_re_pause`, :ref:`method_re_resume_stop_abort_halt`.

.. _locking_re_manager_tutorial:

Locking RE Manager
------------------

RE Manager can be temporarily locked by a user using a 'secret' key. The user is expected to
remember (or keep) the key and unlock the manager when safe. The user may choose to
lock the worker environment and/or the queue which prevents other users
to change the state of environment (start the queue, run plans, upload scripts etc.) or
the queue (add, edit or reorder plans in the queue etc.) unless they are provided with the key.
For more detailed description see :ref:`locking_re_manager`.

Start RE Manager using instructions given in :ref:`tutorial_starting_queue_server`.

Check the status of RE Manager::

  $ qserver status
  Arguments: ['status']
  08:40:21 - MESSAGE:
  { ...
  'lock': {'environment': False, 'queue': False},
  'lock_info_uid': '5a992925-3c86-420f-b338-576eeb8778d3',
  ... }

The ``lock`` parameter indicates if the environment and the queue are locked, ``lock_info_uid``
is updated each time the lock status is changed and intended for use by monitoring client
applications.

Load the lock status::

  $ qserver lock info
  Arguments: ['lock', 'info']
  12:02:41 - MESSAGE:
  {'lock_info': {'emergency_lock_key_is_set': False,
                'environment': False,
                'note': None,
                'queue': False,
                'time': None,
                'time_str': '',
                'user': None},
  'lock_info_uid': '5a992925-3c86-420f-b338-576eeb8778d3',
  'msg': '',
  'success': True}

When the manager is locked, the status includes the name of the user (``user``) who applied
the lock, time (``time``, ``time_str``) when the lock was applied and optional note (``note``)
for other users of the system, explaining the reason why the lock was applied.
The parameter ``emergency_lock_key_is_set`` (``False``) indicates that the emergency key is
not set and the manager can be unlocked only only with the key used to lock it.

Lock the environment with a note::

  $ qserver --lock-key userlockkey lock environment "The environment is locked. Do not unlock environment!"
  Arguments: ['lock', 'environment', 'The environment is locked. Do not unlock environment!']
  12:03:40 - MESSAGE:
  {'lock_info': {'emergency_lock_key_is_set': False,
                'environment': True,
                'note': 'The environment is locked. Do not unlock environment!',
                'queue': False,
                'time': 1658765020.0658383,
                'time_str': '07/25/2022 12:03:40',
                'user': 'qserver-cli'},
  'lock_info_uid': '05c2127b-5569-411a-8212-debf7149390b',
  'msg': '',
  'success': True}

The lock key can be aribtrarily selected by the user who locks the manager (in this example the key is
``userlockkey``). The parameters ``user``, ``time``, ``time_str`` and ``note`` are properly set
now and the parameter ``environment`` is ``True``.

``qserver lock info`` may be used to validate the lock key. The call always succeeds if called
without the lock key. If the manager is locked, then the included key is validated and
the call succeeds only if the key is valid. Try validating an invalid key::

  $ qserver --lock-key someinvalidkey lock info
  Arguments: ['lock', 'info']
  12:04:14 - MESSAGE:
  {'lock_info': {'emergency_lock_key_is_set': False,
                'environment': True,
                'note': 'The environment is locked. Do not unlock environment!',
                'queue': False,
                'time': 1658765020.0658383,
                'time_str': '07/25/2022 12:03:40',
                'user': 'qserver-cli'},
  'lock_info_uid': '05c2127b-5569-411a-8212-debf7149390b',
  'msg': 'Error: Invalid lock key: \n'
          'RE Manager is locked by qserver-cli at 07/25/2022 12:03:40\n'
          'Environment is locked: True\n'
          'Queue is locked:       False\n'
          'Emergency lock key:    not set\n'
          'Note: The environment is locked. Do not unlock environment!',
  'success': False}

The call fails (``'success': False``) and the error message indicates that the lock key is invalid.
Try validating the valid key::

  $ qserver --lock-key userlockkey lock info
  Arguments: ['lock', 'info']
  12:04:41 - MESSAGE:
  {'lock_info': {'emergency_lock_key_is_set': False,
                'environment': True,
                'note': 'The environment is locked. Do not unlock environment!',
                'queue': False,
                'time': 1658765020.0658383,
                'time_str': '07/25/2022 12:03:40',
                'user': 'qserver-cli'},
  'lock_info_uid': '05c2127b-5569-411a-8212-debf7149390b',
  'msg': '',
  'success': True}

Since the environment is locked, all operations that change the state of environment, such as
opening and closing the environment, starting the queue etc., can be executed only if a valid
lock key is included in the call. Try opening the environment without the lock key::

  $ qserver environment open
  Arguments: ['environment', 'open']
  12:05:14 - MESSAGE:
  {'msg': 'Error: Invalid lock key: \n'
          'RE Manager is locked by qserver-cli at 07/25/2022 12:03:40\n'
          'Environment is locked: True\n'
          'Queue is locked:       False\n'
          'Emergency lock key:    not set\n'
          'Note: The environment is locked. Do not unlock environment!',
  'success': False}

Now try opening the environment with the lock key::

  $ qserver --lock-key userlockkey environment open
  Arguments: ['environment', 'open']
  12:05:44 - MESSAGE:
  {'msg': '', 'success': True}

The operation succeeded. Now close the environment with the lock key::

  $ qserver --lock-key userlockkey environment close
  Arguments: ['environment', 'close']
  12:06:09 - MESSAGE:
  {'msg': '', 'success': True}

``qserver lock`` also allows to lock the queue (blocks access to queue operations)
or both the environment and the queue. Try to lock the queue (optionally add the note)::

  $ qserver --lock-key userlockkey lock queue
  Arguments: ['lock', 'queue']
  12:06:34 - MESSAGE:
  {'lock_info': {'emergency_lock_key_is_set': False,
                'environment': False,
                'note': None,
                'queue': True,
                'time': 1658765194.4385393,
                'time_str': '07/25/2022 12:06:34',
                'user': 'qserver-cli'},
  'lock_info_uid': '6af981eb-0690-4110-839f-8e315649ef40',
  'msg': '',
  'success': True}

and add plans to the queue with and without the ``--lock-key`` parameter, then
lock the environment and the queue::

  $ qserver --lock-key userlockkey lock all
  Arguments: ['lock', 'all']
  12:06:55 - MESSAGE:
  {'lock_info': {'emergency_lock_key_is_set': False,
                'environment': True,
                'note': None,
                'queue': True,
                'time': 1658765215.8313878,
                'time_str': '07/25/2022 12:06:55',
                'user': 'qserver-cli'},
  'lock_info_uid': 'bd84f374-8b05-46d8-bbd9-e61a0c599b15',
  'msg': '',
  'success': True}

The lock may be applied repeatedly to the locked manager to change the lock options as long as
the valid lock key is passed. The lock key can not be changed without unlocking the manager.

To unlock the manager run ``qserver unlock`` with the valid lock key::

  $ qserver --lock-key userlockkey unlock
  Arguments: ['unlock']
  12:07:24 - MESSAGE:
  {'lock_info': {'emergency_lock_key_is_set': False,
                'environment': False,
                'note': None,
                'queue': False,
                'time': None,
                'time_str': '',
                'user': None},
  'lock_info_uid': '6d3e834d-eccd-44be-87b1-db3b8557bfcb',
  'msg': '',
  'success': True}

The lock status is stored in Redis and persists between sessions, i.e. restarting RE Manager
does not clear the lock. If the key is lost, then the manager can be unlocked using
an optional emergency lock key::

  # Start RE Manager with the emergency lock key
  QSERVER_EMERGENCY_LOCK_KEY_FOR_SERVER=emlockkey start-re-manager

  # Lock the environment
  $ qserver --lock-key key-to-forget lock environment

  # Assume that the key is lost. Unlock the manager with the emergency key.
  $ qserver --lock-key emlockkey unlock

  # Check lock status. The manager should be unlocked.
  $ qserver lock info

If the emergency key is not set, then the lock can be
cleared by running :ref:`qserver_clear_lock_cli` CLI tool and then restarting RE Manager
service or application. The tool requires access to Redis server used by RE Manager.
The following steps illustrate the procedure::

  # Start RE Manager.

  # Lock the environment
  $ qserver --lock-key key-to-forget lock environment

  # Check lock status
  $ qserver lock info

  # Assume that the key is lost. Clear the lock in Redis. Pass '--redis-addr' if needed.
  qserver-clear-lock

  # Stop and restart RE Manager application.

  # Check lock status. The manager should be unlocked.
  $ qserver lock info

API used in this tutorial: :ref:`method_lock`, :ref:`method_lock_info`, :ref:`method_unlock`,
:ref:`method_status`, :ref:`method_environment_open`, :ref:`method_environment_close`.


.. _tutorial_running_custom_startup_code:

Running RE Manager with Custom Startup Code
-------------------------------------------

All the tutorials in this section are using a set of built-in startup scripts that provide simulated
devices and simple plans, which are sufficient to explore functionality of the Queue Server.
Any practical application would require starting the server with custom startup scripts
with Ophyd devices that represent real hardware and Bluesky plans that perform useful measurements.
This tutorial provides instructions for configuring the server to load custom IPython-style set
of startup scripts.

Instead of creating new scripts, we will copy the existing startup files in custom directory and
configure the server to load scripts from this directory. Those scripts could be then modified
or replaced custom scripts.

**Step 1.** Create a directory for the startup files in a convenient location, e.g. ``~/qs_startup``.
The directory should be readable and writable for the user running RE Manager.

**Step 2.** Copy startup scripts (only .py files) and ``user_group_permissions.yaml`` from
`the repository <https://github.com/bluesky/bluesky-queueserver/tree/main/bluesky_queueserver/profile_collection_sim>`_
to ``~/qs_startup``. The file ``existing_plans_and_devices.yaml`` will be generated by RE Manager
as part of the tutorial, so do not copy it. The directory should contain the following files::

  $ ls
  00-ophyd.py  15-plans.py  99-custom.py  user_group_permissions.yaml

**Step 3.** Start RE Manager by specifying the path to startup directory::

  $ start-re-manager --startup-dir ~/qs_startup
  [W 2022-02-17 18:43:10,262 bluesky_queueserver.manager.start_manager] The file with the list of allowed plans and devices ('/home/dgavrilov/qs_startup/existing_plans_and_devices.yaml') does not exist. The manager will be started with empty list. The list will be populated after RE worker environment is opened the first time.
  [I 2022-02-17 18:43:10,263 bluesky_queueserver.manager.manager] Starting ZMQ server at 'tcp://*:60615'
  [I 2022-02-17 18:43:10,263 bluesky_queueserver.manager.manager] ZMQ control channels: encryption disabled
  [I 2022-02-17 18:43:10,266 bluesky_queueserver.manager.manager] Starting RE Manager process
  [I 2022-02-17 18:43:10,284 bluesky_queueserver.manager.manager] Loading the lists of allowed plans and devices ...
  [W 2022-02-17 18:43:10,284 bluesky_queueserver.manager.profile_ops] List of plans and devices is not loaded. File 'existing_plans_and_devices.yaml' does not exist.
  [I 2022-02-17 18:43:10,285 bluesky_queueserver.manager.manager] Starting ZeroMQ server ...
  [I 2022-02-17 18:43:10,285 bluesky_queueserver.manager.manager] ZeroMQ server is waiting on tcp://*:60615

**Step 4.** Open RE Worker environment::

  $ qserver environment open

**Step 5.** Verify that ``existing_plans_and_devices.yaml`` file was generated::

  $ ls
  00-ophyd.py  15-plans.py  99-custom.py  existing_plans_and_devices.yaml  user_group_permissions.yaml

RE Manager is ready and plans may be submitted to the queue and executed. If plans or devices are
added or modified, the currently open environment must be closed and opened again to reload
the startup files and generate the new list of existing plans and devices.

In some configurations, it is convenient to place the startup files in the ``startup`` directory
for one of IPython profiles, so that they could be loaded into IPython. At NSLS-II it is
traditional to use the IPython profile named ``collection`` to run Bluesky software and
standard location for startup files is ``~/.ipython/profile_collection/startup``.
RE Manager may be configured to find the startup files by explicitly specifying the directory::

  $ start-re-manager --startup-dir ~/.ipython/profile_collection/startup

or by specifying the name of the IPython profile::

  $ start-re-manager --startup-profile collection

In addition to IPython-style sets of startup files, RE Manager may be configured to load
the code from a Python script (by specifying path to script file) or from an installed
Python module. The configuration instructions may be found in the section
:ref:`location_of_startup_code`. Note, that the code for loading IPython-style startup
files performs patching to provide some compatibility with features of IPython.
Patching was implemented mostly to simplify transition from IPython workflow used on beamlines.
Startup scripts are assumed to be written for execution in pure Python environment and
are not patched. Ideally all blocks of code that use IPython features should be disabled
when executed in by the Queue Server (see :ref:`detecting_if_code_executed_by_re_worker`).

.. _tutorial_manual_gen_list_of_plans_devices:

Manually Generating Lists of Existing Plans and Devices
-------------------------------------------------------

RE Manager generates or updates the list of existing plans and devices automatically when
RE Worker environment is opened, but in some cases it is convenient to generate
the list manually. For example, the developers wishing to update ``existing_plans_and_devices.yaml``
in `the 'profile_collection_sim' directory <https://github.com/bluesky/bluesky-queueserver/tree/main/bluesky_queueserver/profile_collection_sim>`_
when the respective startup files are modified have the only option to do it manually (RE Manager
is designed not to automatically modify files in built-in ``profile_collection_sim`` directory).

**Step 1.** Create the directory with startup files and copy startup Python files as
described in :ref:`tutorial_running_custom_startup_code`. We will assume that
the files are in the directory ``~/qt_startup``. The directory should contain
the following files::

  $ ls
  00-ophyd.py  15-plans.py  99-custom.py

**Step 2.** Use ``qserver-list-plans-devices`` CLI tool to generate ``existing_plans_and_devices.yaml``::

  $ qserver-list-plans-devices --startup-dir ~/qs_startup --file-dir ~/qs_startup

**Step 3.** Check if the file ``existing_plans_and_devices.yaml`` is created in the directory::

  $ ls
  00-ophyd.py  15-plans.py  99-custom.py  existing_plans_and_devices.yaml

Alternatively, ``qserver-list-plans-devices`` may be started from the ``~/qs_startup`` directory::

  $ cd ~/qs_startup
  $ qserver-list-plans-devices --startup-dir .


.. _remote_monitoring_tutorial:

Remote Monitoring of RE Manager Console Output
----------------------------------------------

Queue Server provides a simple ``qserver-console-monitor`` CLI tool for remote
monitoring of console output of RE Manager. The tool subscribes to messages
published by RE Manager over 0MQ and displays text contents of the messages. The
output of ``qserver-console-monitor`` is expected to be identical to the output
of RE Manager. There is also an option to disable printing of console output
RE Manager and use the external monitoring application for visualizing of
RE Manager output.

In Terminal 1 start ``qserver-console-monitor``::

  $ qserver-console-monitor

In Terminal 2 start RE Manager with console output publishing available::

  $ start-re-manager --zmq-publish-console ON

Use Terminal 3 to run some commands using ``qserver`` tool. Terminals 1 and 2
must display identical output. Multiple instances of ``qserver-console-monitor``
may be running simultaneously and display the same console output.
Experiment with closing (Ctrl-C) and restarting ``qserver-console-monitor``.
Notice that all published console output is lost while the monitor is closed.

In Terminal 2, close RE Manager (Ctrl C) and restart it with the option that
disables printing of the console output::

  $ start-re-manager --zmq-publish-console ON --console-output OFF

Notice that no output is printed in Terminal2. External monitor (running in
Terminal 1) is needed to visualize the output from RE Manager.

In practice, the client applications are expected to implement the
functionality for subscribing to published RE Manager output and displaying
it to users. The use of ``qserver-console-monitor`` tool should be limited to
evaluation, testing and debugging of the systems using RE Manager.
