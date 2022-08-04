======================
Using the Queue Server
======================

Starting and Stopping Run Engine Manager
----------------------------------------

The core component of the Queue Server is the Run Engine manager, which could be started as an application
or a service. Running RE Manager as an application is easy and recommended for evaluation, testing and demos.
Production systems more likely to run RE Manager as a service.

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


Closing RE Manager using API
****************************

RE Manager can be stopped programmatically by sending :ref:`method_manager_stop` API request. The API parameter
allows to select whether the operation is performed in *safe* mode (API request is rejected if RE Manager is
not *idle*) or to disable safe mode (RE Manager is closed even if it is performing an operation, e.g. a plan
is running). The API is mostly intended for automated system testing and should not be exposed to general users
through client applications.

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

The queue can operate with enabled/disabled *LOOP* mode (see :ref:`method_queue_mode_set`). If the *LOOP* mode
is disabled (normal mode), the items are popped from the front of the queue and executed by in the Worker. 
The successfully completed plans (including stopped plans) are permanently removed from the queue and added 
to plan history upon completion. If a plan fails, is aborted and or halted, it is pushed to the front 
of the queue and  added to the history along with execution results (error message and traceback) and the 
queue execution is automatically stopped. The operation is slightly different if the *LOOP* mode is enabled:
successfully executed (or stopped) plans are added to the back of the queue, allowing client to infinitely 
repeate a sequence of plans. The stopped plans are treated as successful in both modes, except that
stopping a plan also stops execution of the queue.


Interacting with the Worker Environment
---------------------------------------


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
