======================
Using the Queue Server
======================

Starting and Stopping Run Engine Manager
----------------------------------------

The core component of the Queue Server is the Run Engine manager, which could be started as an application
or a service. Running RE Manager as an application is easy and recommended for evaluation, testing and demos.
Production systems more likely to run RE Manager as a service.

.. _running_re_manager_as_application:

Running RE Manager as an Application
************************************

Starting RE Manager as an application is demonstrated in tutorials :ref:`tutorial_starting_queue_server` and
:ref:`tutorial_starting_queue_server` and includes activating the Conda environment with installed Queue Server
and running :ref:`start_re_manager_cli` with appropriate set of parameters. Activating some options may also
require environment variables to be set before ``start-re-manager`` is started.

RE Manager is started with the default set of options by typing ::

  $ start-re-manager

in the command prompt. The default options are sufficient for most demos, which are based on the simulated
startup code distributed with the package. If a demo involves remote monitoring of console output,
then activate publishing of console output to 0MQ socket by using ``--zmq-publish-console``::

  $ start-re-manager --zmq-publish-console ON

The manager could be configured to load custom startup code by setting the path to the directory with
code files::

  $ start-re-manager --zmq-publish-console ON --startup-dir <path-to-directory-with-files>

RE Manager automaticaly creates instances of Bluesky Run Engine (``RE``) and Data Broker (``db``).
Production scripts typically create custom instances ``RE`` and ``db``. In this case, RE Manager
must be called with the option ``--keep-re`` to prevent RE Manager from overriding ``RE`` and ``db``::

  $ start-re-manager --zmq-publish-console ON --startup-dir <path-to-directory-with-files> --keep-re

This is the minimum configuration of RE Manager sufficient for practical use of Queue Server for experimental
control. Configuring RE Manager for a production system may require additiona settings. See :ref:`start_re_manager_cli`
for detailed description of parameters.

Run Engine manager running as an application may be closed by pressing Ctrl-C in the terminal.

Running RE Manager as a Service
*******************************

The following example demonstrates how to start RE Manager as a user service, which does not
require root access. The manager is started in the most basic configuration. Change the configuration
by setting by setting environment variables and additional parameters of ``start-re-manager`` as needed.
Setting up the service requires two files: service configuration file and the script that starts
RE Manager. Replace ``<user-name>`` in file paths and the script files with the correct user name.
It is also assumed that the Queue Server is installed in *bs-qserver* environment using *miniconda3*.
Modify the scripts and paths to reflect the system configuration.

Service configuration file::

  # File: /home/<user-name>/.config/systemd/user/queue-server.service

  [Unit]
  Description=Bluesky Queue Server

  [Service]
  ExecStart=/usr/bin/bash /home/<user-name>/queue-server.sh

  [Install]
  WantedBy=default.target
  Alias=queue-server.service

The script for starting RE Manager::

  # File: /home/<user-name>/queue-server.sh

  source "/home/dgavrilov/miniconda3/etc/profile.d/conda.sh"
  conda activate bs-qserver
  start-re-manager --zmq-publish-console ON --console-output OFF

Starting the service::

  `$ systemctl --user start queue-server

Checking the status of the service::

  `$ systemctl --user status queue-server

Stopping the service::

  `$ systemctl --user stop queue-server

The Run Manager is configured not to print any console output, but instead to publish console output to
0MQ socket. See the section :ref:`remote_monitoring_of_console_output` and the tutorial
:ref:`tutorial_remote_monitoring` for the instructions on how to view the console output remotely.
The illustrated procedure can be modified to satisfy practical operational needs.


Closing RE Manager using API
****************************

RE Manager can be stopped programmatically by sending :ref:`method_manager_stop` API request. The API parameter
allows to select whether the operation is performed in *safe* mode (API request is rejected if RE Manager is
not *idle*) or to disable safe mode (RE Manager is closed even if it is performing an operation, e.g. a plan
is running). The API is mostly intended for automated system testing and should not be exposed to general users
through client applications.

Monitoring of Status of RE Manager
----------------------------------

Status of RE Manager may be loaded at any time using :ref:`method_status` API. The API returns
a dictionary with status parameters::

  {'devices_allowed_uid': '0639bc7a-15c1-4bc2-bfeb-41f58a08a8b9',
  'devices_existing_uid': '2fe9df70-5f0f-4c17-bb7b-cda8a0aa80b0',
  'items_in_history': 0,
  'items_in_queue': 0,
  'lock': {'environment': False, 'queue': False},
  'lock_info_uid': '2b438226-f715-4ed3-a057-400a42717bb0',
  'manager_state': 'idle',
  'msg': 'RE Manager v0.0.16.post28.dev0+ge2491ae',
  'pause_pending': False,
  'plan_history_uid': '5894c896-b2ea-42c1-9dd0-f4faaa52cb39',
  'plan_queue_mode': {'loop': False},
  'plan_queue_uid': '5589b7ac-01b9-4f51-a7f2-8e883c352053',
  'plans_allowed_uid': '835a998a-e01e-439f-bb3a-b65817904f7a',
  'plans_existing_uid': '609ec025-2552-4e06-aa54-5e9ae7b7ed2c',
  'queue_stop_pending': False,
  're_state': 'idle',
  'run_list_uid': '0b088775-1cc3-46de-9563-b83e04c0e243',
  'running_item_uid': None,
  'task_results_uid': '846b6dd3-d9c2-4a12-bd03-b82580b8f742',
  'worker_background_tasks': 0,
  'worker_environment_exists': True,
  'worker_environment_state': 'idle'}

The parameter *msg* contains the version information on currently running RE Manager. The states
of RE Manager and Run Engine are returned using parameters *manager_state* and *re_state*.
The latter represents true state of Run Engine that is is propagated from RE Worker environment
(there could be some short delay before the state is updated). The boolean parameter
*worker_environment_exists* is ``True`` if the environment is opened and ``False`` otherwise.

Parameters with names ending with *'_uid'* (such as *plan_queue_uid*) contain UIDs of the respective
objects that are changed each time the objects are updated. For example, *plan_queue_uid* is changed
each time the queue is updated either in response to API request from a client or due to internal
processes. Tracking changes in UIDs and downloading the respective objects only when the UIDs
change is more efficient than continuously polling the objects themselves.

See documentation on :ref:`method_status` API for detailed description of the status parameters.

Opening and Closing RE Worker Environment
-----------------------------------------

The RE Worker environment must be opened before starting the queue, executing plans, functions or uploading script.
The operation of opening the environment consists of creating a separate process (Worker process) and loading
startup code. Once startup code is loaded, RE Manager updates the lists of existing and allowed devices and plans
based on the contents of the Worker namespace. The process of opening the environment is initiated by sending
:ref:`method_environment_open` API request and if the request is accepted, then waiting for the process to complete.

The contents of the environment may be changed remotely by uploading and executing scripts using
:ref:`method_script_upload` API, which allows to add, remove or modify objects in the worker namespace.
The changes introduced by uploaded scripts are lost once the environment is closed.

Similarly to opening the environment, the operation of closing or destroying the environment is initiated by sending
:ref:`method_environment_close` or :ref:`method_environment_destroy` API requests and waiting for operation to
complete. The :ref:`method_environment_close` API is intended for use during normal operation. The environment
can be closed only if RE Manager is idle, i.e. no plans or tasks are currently executed. The operation of destroying
the environment allows to recover RE Manager in case the environment is stuck (e.g. executing an infinite loop)
by killing the worker process. The operation is unsafe and should be used only as a last resort.

See the tutorial :ref:`tutorial_opening_closing_re_worker_environment`.

Managing the Plan Queue
-----------------------

RE Manager supports operations on the queue allowing clients to add, move, remove and replace queue items.
All queue operations may be executed at any time. The contents of the queue may be loaded using
:ref:`method_queue_get` API, which returns the list queue items (*items*) and the currently running item
(*running_item*) if the queue is running. The running item is not considered part of the queue and can
not be used in most of the queue operations.

The queue supports two types of items: plans (Bluesky plans executed in the worker environment)
and instructions. The instructions are used to control the queue. Currently only one instruction
(``'queue_stop'``) instruction is supported.

The operations of adding (:ref:`method_queue_item_add`), moving (:ref:`method_queue_item_move`) and removing
(:ref:`method_queue_item_remove`) items have batch equivalents :ref:`method_queue_item_add_batch`,
:ref:`method_queue_item_move_batch` and :ref:`method_queue_item_remove_batch`. The batch operations accept lists
of items instead of single items and guaranteed to perform atomic operations on the queue.

Queue operations allow multiple modes of addressing queue items. Items may be addressed using item position
(parameter ``pos``), which could be positive or negative index of the item or a string literal (``'front'``
or ``'back'``). While using ``pos='front'`` or ``pos='back'`` to insert or move items to the front or back of
the queue is guaranteed to produce the expected result, using indexes is reliable only if the queue is not
running (negative indexes should work reliably if the queue is running) and no other clients are in
the process of modifying the queue. Another mode of addressing is using item UID to uniquiely identify
the queue items. Queue operations allow to select items by UID and insert items before or after items with
a given UID (parameters ``uid``, ``before_uid`` and ``after_uid``). Batch operations accept lists of
item UIDs (parameter ``uids``) to select and possibly reorder lists of existing items.

The queue may be cleared at any time using :ref:`method_queue_clear` API. If the queue is running, clearing
the queue does not affect currently running item or the state of the queue: if no new items are added
by the time the currently running plan is completed, then the queue is automatically stopped.

See the full list of API in :ref:`supported_methods_for_0MQ_API` and tutorial :ref:`tutorial_adding_queue_items`.

Managing the Plan History
-------------------------

Plan history contains a list of completed plans along with the results of execution (start and stop time,
completion status, error message and traceback in case of failure). The plan history may be loaded using
:ref:`method_history_get` API and cleared using :ref:`method_history_clear` API. Plan history is not designed
to grow indefinitely and should be periodically cleared in order to avoid performance issues.

Controlling Execution of the Queue and the Plans
------------------------------------------------

The plan queue can be started using :ref:`method_queue_start` if RE Worker environment is open, otherwise
the API request fails. The queue stops automatically once it runs out of plans. Users may request RE Manager
to stop the queue by sending :ref:`method_queue_stop` API request. Once RE Manager receives the request,
it waits until the currently executed plan is completed and then stops the queue. The pending request
to stop the queue is reflected in RE Manager status (*queue_stop_pending*) and may be cancelled at any time
while the queue is still running by sending :ref:`method_queue_stop_cancel` request.

The alternative way to stop the queue is to add ``'queue_stop'`` instruction to the desired position in
the queue. RE Manager pops the instruction from the queue and stops the execution. The queue execution may
be resumed at any time starting from the following item.

Execution of the currently running plan can be interrupted using :ref:`method_re_pause` API request.
The API allows to request deferred (the plan runs until the next checkpoint) or immediate pause.
See `Interruptions <https://blueskyproject.io/bluesky/state-machine.html>`_ sections of Bluesky documentation
for more details. The paused plan may be :ref:`resumed, stopped, aborted or halted <method_re_resume_stop_abort_halt>`.
Note, that stopped plan is considered successfully completed, while aborted and halted plans are considered
failed.

Interrupting the current plan allows to stop the queue immediately: the plan may be paused by sending
:ref:`method_re_pause` API request (this will pause the execution of the plan, which may be sufficient to resolve
some technical difficulties) and then stop using :ref:`'re_stop' <method_re_resume_stop_abort_halt>`
or abort using :ref:`'re_abort' <method_re_resume_stop_abort_halt>` API (the latter API pushes the plan to
the top of the queue).

The queue can operate with enabled/disabled *LOOP* mode (see :ref:`method_queue_mode_set`). If the *LOOP* mode
is disabled (normal mode), the items are popped from the front of the queue and executed by in the Worker
(plans) or the manager (instructions). The successfully completed plans (including stopped plans) are
permanently removed from the queue and added to plan history upon completion. If a plan fails, is aborted
and or halted, it is pushed to the front of the queue and  added to the history along with execution results
(error message and traceback) and the queue execution is automatically stopped. The operation is slightly
different if the *LOOP* mode is enabled: successfully executed (or stopped) plans and instructions are
added to the back of the queue, allowing client to infinitely repeate a sequence of plans. The stopped plans
are treated as successful in both modes, except that stopping a plan also stops execution of the queue.

See the tutorials :ref:`tutorial_starting_stopping_queue` and :ref:`tutorial_iteracting_with_run_engine`.

.. _immediate_execution_of_plans:

Immediate Execution of Plans
----------------------------

RE Manager allows to execute plans without placing them in the queue. Plans can be submitted for
immediate execution using :ref:`method_queue_item_execute` API. The requests are accepted only if
RE Manager is in the idle state, otherwise the request is rejected and the plan is discarded.
Once the request is validated and accepted, the plan is passed to RE Worker for immediate
execution. Similarly to items from the queue, the plan is assigned item UID and can be tracked
using the same API. Upon completion, the plan is added to history along with the results of
execution. The plan is never added to the queue, even if it fails or the queue is in the loop
mode. If the queue contains other plans, its contents remain unchanged. Submitting a plan
for immediate execution does not start execution of the existing queue.

See the tutorial :ref:`tutorial_immediate_execution_of_plans`.

.. _executing_functions:

Executing Functions
-------------------

RE Manager allows to start execution functions in RE Worker environment. The requests to start
execution of functions could be submitted by clients using :ref:`method_function_execute` API,
which accepts function name and parameters in the format used for queue item. Clients may
access only functions that exist in RE Worker namespace (e.g. defined in startup script or
an uploaded script) and allowed by user group permissions (see :ref:`configuring_user_group_permissions`).
The functions may access all objects in the namespace and used to change states of the objects
or read the states of the objects and return the results to the client. While it is possible
to corrupt the environment by running arbitrary code, permissions may be used to allow
users access only to one or several carefully designed functions or block access to any functions
(default), and the system may remain safe.

Once the API request is accepted by RE Manager, the task is assigned UID (``task_uid``), which
is returned as one of the API response parameters. The task UID allows to track execution
of the task using :ref:`method_task_status` and :ref:`method_task_result` API. Once function
execution is completed, the task contains the return value of the function (in case of success)
and error message and traceback (in case of failure).

Functions may be started as foreground and background tasks. See :ref:`running_tasks` for details
on running and monitoring tasks.

See the tutorial :ref:`tutorial_executing_functions`.

.. _uploading_scripts:

Uploading Scripts
-----------------

RE Manager provides users with ability to upload and execute Python scripts in the worker namespace.
The :ref:`method_script_upload` API accepts the script represented as string, which is uploaded
to RE Manager over 0MQ, passed to the worker environment and executed. The script is
executed as a task and ``task_uid`` returned by the API may be used to monitor the task status
and download results, indicating if the script was completed successfully and containing
the error message and the traceback in case of failure.

The script may contain arbitrary Python code, which is executed in the worker environment. The code
has full access to the worker namespace and may modify, replace or create new objects, including
functions, devices and plans. For example, an uploaded script may contain code for a new plan, which
becomes available in the worker namespace or modified code for an existing plan, which replaces
the plan in the namespace. By default, the lists of existing and allowed plans and devices are updated
after execution of each script. The new plans and devices become immediately available to users
who have appropriate permissions (see :ref:`configuring_user_group_permissions`).

The variables ``RE`` and ``db`` are reserved for instances of Bluesky Run Engine and Data Broker.
By default, the existing ``RE`` or ``db`` objects are not replaced in the worker namespace
even if the script contains the respective code (scripts are free to use those objects).
This restriction is implemented to prevent accidental changes to the namespace, which may cause
RE Manager to fail. In order to allow the script to replace ``RE`` and ``db``, call the API
with ``update_re=True``. If the uploaded script does not contain new or modified plans or
devices, then there is no need to update the respective lists and the operation may be performed
more efficiently if the ``update_lists=False``.

Scripts may be executed as foreground and background tasks. See :ref:`running_tasks` for details
on running and monitoring tasks.

.. note::

  The scripts may contain arbitrary code. Users and developers should carefully consider
  what code is executed in the worker namespace and how it affects the state of the worker
  environment. For example, a script that executes a plan can be successfully started and
  completed as a foreground task, bypassing all mechanisms for queue management, but it is
  not advised to do so.

See the tutorial :ref:`tutorial_uploading_scripts`.

.. _running_tasks:

Running Tasks
-------------

Tasks are used for remote monitoring of execution of functions and scripts in RE Worker
namespace. Each task is assigned a UID (*task_uid*), which is returned by the API starting
the task and can be used to monitor status of the task and load the results after completion.
For example, :ref:`method_function_execute` API call starting execution of
a function ``function_sleep`` (defined in the demo startup code) returns ::

  {'item': {'args': [30],
            'item_uid': '21ecccbe-df52-4478-a42b-3a94b4f54fcd',
            'kwargs': {},
            'name': 'function_sleep',
            'user': 'qserver-cli',
            'user_group': 'primary'},
  'msg': '',
  'success': True,
  'task_uid': '21ecccbe-df52-4478-a42b-3a94b4f54fcd'}

Calling :ref:`method_task_status` and :ref:`method_task_result` with task UID
``21ecccbe-df52-4478-a42b-3a94b4f54fcd`` returns information on current status of the task
and the result of task execution after the task is completed::

  # Status returned while the task is still running
  {'msg': '',
  'status': 'running',
  'success': True,
  'task_uid': '21ecccbe-df52-4478-a42b-3a94b4f54fcd'}

  # Result returned while the task is still running
  {'msg': '',
  'result': {'run_in_background': False,
              'task_uid': '21ecccbe-df52-4478-a42b-3a94b4f54fcd',
              'time_start': 1659709083.4135385},
  'status': 'running',
  'success': True,
  'task_uid': '21ecccbe-df52-4478-a42b-3a94b4f54fcd'}

  # Status returned after the task is completed
  {'msg': '',
  'status': 'completed',
  'success': True,
  'task_uid': '21ecccbe-df52-4478-a42b-3a94b4f54fcd'}

  # Result returned after the task is completed
  {'msg': '',
  'result': {'msg': '',
              'return_value': {'success': True, 'time': 30},
              'success': True,
              'task_uid': '21ecccbe-df52-4478-a42b-3a94b4f54fcd',
              'time_start': 1659709083.4135385,
              'time_stop': 1659709113.4742212,
              'traceback': ''},
  'status': 'completed',
  'success': True,
  'task_uid': '21ecccbe-df52-4478-a42b-3a94b4f54fcd'}

The ``return_value`` is the return value of the function (always ``None`` for a script),
``msg`` and ``traceback`` are the strings representing the error message and full
traceback in case the task fails.

Functions and scripts may be executed as foreground and background tasks. Foreground
tasks are executed in the main thread of RE Worker process. Foreground tasks could be
started only if RE Manager is *idle*, i.e. no other foreground tasks or plans are
running. As foreground task is started, RE Manager state is changed to ``'executing_task'``,
which blocks other foreground tasks or plans from being started. Background tasks are
executed in separate background threads, could be started at any time and do not
block execution of foreground tasks or plans. Any reasonable number of background
tasks could be running at any time. The number of background tasks is returned
as a parameter of RE Manager :ref:`method_status`::

  { ...
  'task_results_uid': '846b6dd3-d9c2-4a12-bd03-b82580b8f742',
    ...
  'worker_background_tasks': 0,
    ... }

The parameter *'task_results_uid'* is updated each time a new task is started or task
execution is completed. An application waiting for completion of one or more tasks
can wait for the UID to change and then check the status of each task. Considering
that applications are likely to monitor the manager status for other purposes,
monitoring *'task_results_uid'* may be more efficient than continuously polling status
of each task.

.. note::

  Task results are stored at the server for a limited time and then deleted. Currently the expiration time
  is 2 minutes after completion of the task, but could be parametrized in the future.

.. note::

  Background tasks are executed in background threads. It is responsibility of software or workflow developer
  to ensure thread safety. Foreground tasks could be executed in the main thread one at a time and do not
  introduce risks associated with thread safety.

See the tutorials :ref:`tutorial_executing_functions` and :ref:`tutorial_uploading_scripts`.


.. _locking_re_manager:

Locking RE Manager
------------------

Users and client applications can temporarily lock RE Manager. When the manager is locked, users
can access certain groups of API only by pass a *lock key* with API requests. The *lock key* is
an arbitrary string selected by the user who locks RE Manager and stays valid until the manager
is unlocked. The key could be shared with other users who need to control the locked manager.
The lock status is stored in Redis. Restarting the manager does not reset the lock. If the manager
is locked, it needs to be unlocked using valid lock key. Optionally, the emergency key may be set
using the environment variable ``QSERVER_EMERGENCY_LOCK_KEY_FOR_SERVER``. The emergency key allows
to unlock the manager in case the lock key is lost. It can not be used to control the locked RE Manager.

The :ref:`method_lock` API allows to lock the API that control RE Worker environment and/or the queue.
The lock does not affect *read-only* API, therefore monitoring client applications will continue
working when the manager is locked. The full list of API affected by locking the environment and
the queue can be found in the documentation for :ref:`method_lock` API.

The lock is not designed to be used for access control. The typical use case scenarios:

- A beamline scientist or on-site user locks the environment before entering the hutch to change samples.
  This prevents remote users, autonomous agents etc. to open/close the environment, start the queue and
  execute plans and tasks. If necessary, the scientist who locked the environment may still perform
  those operations using the secret lock key without unlocking the manager. Since the queue is not locked,
  the remote users and autonomous agents are still free to edit the queue or add plans to the queue.

- A beamline scientist is performing maintenance or calibration and locks both the environment and
  the queue to have exclusive control of the manager.

API for controlling and monitoring lock status of the manager:

- :ref:`method_lock` - lock the environment and/or the queue using a lock key. The API also accepts
  the name of the user who locks the manager (required) and a text note to other users (optional).
  This information is returned as part of the lock info and included in all relevant error messages.

- :ref:`method_unlock` - unlock the manager using the valid lock key (it must be the same key as
  for locking the manager) or the emergency lock key (if set). If the key is lost and the emergency
  key is not set or unknown, the lock can be cleared using :ref:`qserver_clear_lock_cli` CLI tool
  and restarting RE Manager application or service.

- :ref:`method_lock_info` - load the manager lock status. The lock status is assigned a UID, which
  is updated each time the status is changed. The UID is included in the manager status (:ref:`method_status` API),
  which simplifies monitoring of the lock status. The manager status also contains *'lock'* parameter,
  which indicates if the environment and/or the queue are currently locked.

The operations of locking and unlocking RE Manager using CLI tool could be found in the tutorial
:ref:`tutorial_locking_re_manager`.

.. note::

  The :ref:`method_lock` API controls access to other API, not internal operation of the server.
  For example, if the server is executing the queue, the queue will continue running after
  the manager is locked until it runs out of plans or stopped.
