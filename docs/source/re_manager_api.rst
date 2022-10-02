.. _run_engine_manager_api:

======================
Run Engine Manager API
======================


.. currentmodule:: bluesky_queueserver

Asyncio-Based API for Controlling RE Manager
============================================

Asyncio-based API is designed for use in applications that employ `asyncio` event loop.
For example, HTTP server module is using asyncio-based API for communication with RE Manager.
Communication is performed by instantiating `ZMQCommSendAsync` class and awaiting
`ZMQCommSendAsync.send_message()` function, which accepts the method name and parameters
as arguments and returns the response message.

.. autosummary::
   :nosignatures:
   :toctree: generated

    ZMQCommSendAsync
    ZMQCommSendAsync.send_message
    ZMQCommSendAsync.close


Thread-Based API for Controlling RE Manager
===========================================

Thread-based API is designed for use in applications that are not based on `asyncio` event loop,
such as PyQT applications or simple python scripts. Communication with RE Manager is performed
by instantiating `ZMQCommSendThreads` class and calling `ZMQCommSendThreads.send_message()`
function, which accepts the method name and parameters. The `send_message()` function may be
called in blocking and non-blocking mode. In blocking mode, the function returns the response
message. In non-blocking mode the function exits once ZMQ request is send to RE Manager and
passes the response message to user-defined callback function once the response is received.
The reference to the callback function is passed to `send_message()` function as one of the
arguments.

.. autosummary::
   :nosignatures:
   :toctree: generated

    ZMQCommSendThreads
    ZMQCommSendThreads.send_message
    ZMQCommSendThreads.close


.. _supported_methods_for_0MQ_API:

Supported Methods For ZMQ Communication API
===========================================

Brief Reference
---------------

The following reference describes the methods used for controlling RE Manager. For each
method, the documentation contains the description of outgoing and returned parameters.
Method name and the dictionary of method parameters are passed as arguments to
`send_message()` API function.

The result of the request processing is returned
as a dictionary of the returned parameters by `send_message()` function or passed as
an argument of the callback. In case of communication error, `send_message` function may
raise the exception (default) or return the error message. If reference to a callback function
is passed to `send_message` (in thread-based API), the communication error message will be
passed as `msg_err` parameter of the callback function

Get status information from RE Manager:

- :ref:`method_ping`
- :ref:`method_status`

Existing and allowed plans and devices:

- :ref:`method_plans_allowed`
- :ref:`method_devices_allowed`
- :ref:`method_plans_existing`
- :ref:`method_devices_existing`
- :ref:`method_permissions_reload`
- :ref:`method_permissions_get`
- :ref:`method_permissions_set`

History of executed plans:

- :ref:`method_history_get`
- :ref:`method_history_clear`

Open and close RE Worker environment:

- :ref:`method_environment_open`
- :ref:`method_environment_close`
- :ref:`method_environment_destroy`


Operations with the plan queue:

- :ref:`method_queue_mode_set`
- :ref:`method_queue_get`
- :ref:`method_queue_item_add`
- :ref:`method_queue_item_add_batch`
- :ref:`method_queue_item_update`
- :ref:`method_queue_item_get`
- :ref:`method_queue_item_remove`
- :ref:`method_queue_item_remove_batch`
- :ref:`method_queue_item_move`
- :ref:`method_queue_item_move_batch`
- :ref:`method_queue_item_execute`
- :ref:`method_queue_clear`

Start and stop execution of the plan queue:

- :ref:`method_queue_start`
- :ref:`method_queue_stop`
- :ref:`method_queue_stop_cancel`

Send commands to Bluesky Run Engine:

- :ref:`method_re_pause`
- :ref:`method_re_resume_stop_abort_halt`

Monitor the list of active runs:

- :ref:`method_re_runs`

Run tasks in RE Worker namespace:

- :ref:`method_script_upload`
- :ref:`method_function_execute`
- :ref:`method_task_status`
- :ref:`method_task_result`

Lock/unlock RE Manager:

- :ref:`method_lock`
- :ref:`method_lock_info`
- :ref:`method_unlock`

Stopping RE Manager (mostly used in testing):

- :ref:`method_manager_stop`
- :ref:`method_manager_kill`


Detailed Reference
------------------

.. _method_ping:

**'ping'**
^^^^^^^^^^

============  =========================================================================================
Method        **'ping'**
------------  -----------------------------------------------------------------------------------------
Description   Causes RE Manager to send some predefined response. Currently 'ping' request is
              equivalent to 'status' request. The functionality may be changed in the future.
              Use 'status' method to request status information from RE Manager.

              *The request always succeeds*.
------------  -----------------------------------------------------------------------------------------
Parameters    ---
------------  -----------------------------------------------------------------------------------------
Returns       See 'status' method for information on returned data.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_status:

**'status'**
^^^^^^^^^^^^

============  =========================================================================================
Method        **'status'**
------------  -----------------------------------------------------------------------------------------
Description   Returns status of RE Manager.

              *The request always succeeds*.
------------  -----------------------------------------------------------------------------------------
Parameters    ---
------------  -----------------------------------------------------------------------------------------
Returns       **msg**: *str*
                 application name and version, e.g. 'RE Manager v0.0.10'

              **items_in_queue**: *int*
                 the number of items in the plan queue

              **items_in_history**: *int*
                 the number of items in the plan history

              **running_item_uid**: *str* or *None*
                 item UID of the currently running plan or *None* if no plan is currently running.
                 Monitor **plan_queue_uid** to detect updates of the queue and running item data.

              **plan_queue_uid**: *str*
                 plan queue UID, which is updated each time the contents of the queue is changed.
                 Monitor this parameter to determine when the queue or running item data was
                 updated at the server (see **queue_get** API).

              **plan_history_uid**: *str*
                 plan history UID, which is updated each time the contents of the history is changed.
                 Monitor this parameter to determine when the history data should be downloaded.

              **task_results_uid**: *str*
                 UID of the dictionary of task results. UID is updated each time results of a new
                 completed tasks are added to the dictionary. Check the status of the pending tasks
                 (see *task_result* API) once UID is changed.

              **plans_allowed_uid**: *str*
                 UID for the list of allowed plans. UID is updated each time the contents of
                 the list is changed. Monitor the UID to detect changes in the list of allowed
                 plans and download the list from the server only when it is updated.

              **devices_allowed_uid**: *str*
                 UID for the list of allowed devices. Similar to **plans_allowed_uid**.

              **plans_existing_uid**: *str*
                 UID for the list of existing plans in RE Worker namespace. Similar to
                 **plans_allowed_uid**.

              **devices_existing_uid**: *str*
                 UID for the list of existing devices in RE Worker namespace. Similar to
                 **plans_allowed_uid**.

              **'run_list_uid'** - UID of the list of the active runs. Monitor this UID and
                  load the updated list of active runs once the UID is changed.

              **manager_state**: *str*
                  state of RE Manager. Supported states:

                  - **'initializing'** - RE Manager is initializing (the RE Manager is starting
                    or restarting)

                  - **'idle'** - RE Manager is idle and ready to execute requests. Many requests will fail
                    if RE Manager is not in the idle state.

                  - **'paused'** - a plan was paused and Run Engine is in the paused state.
                    The plan needs to be resumed, stopped, halted or aborted.

                  - **'creating_environment'** - RE Worker environment is in the process of being created.

                  - **'starting_queue'** - preparing to execute the queue.

                  - **'executing_queue'** - queue is being executed.

                  - **'executing_task'** - foreground task (function or script) is being executed.

                  - **'closing_environment'** - RE Worker environment is in the process of being
                    closed (safe).

                  - **'destroying_environment'** - RE Worker environment is in the process of being
                    destroyed (emergency).

              **re_state**: *str* or *None*
                  current state of Bluesky Run Engine (see Blue Sky documentation) or *None* if
                  RE Worker environment does not exist (or closed).

              **worker_environment_state**: *str*
                  current state of the worker environment. Supported states: *'initializing'*,
                  *'idle'*, *'executing_plan'*, *'executing_task'*, *'closing'* and *'closed'*.
                  Running background tasks does not influence the state (*'executing_task'* is
                  not set). The environment state is different from Run Engine state (*re_state*),
                  e.g. the environment is considered *idle* when the current plan is paused.

              **worker_background_tasks**: *int*
                  the number of background tasks, which are currently running in the worker environment.
                  Excessive or growing number of background task may indicated serious issue with
                  the environment. The parameter represents the number of background tasks executed
                  in separate threads, such as tasks started by API requests *script_upload* and
                  *function_execute* with *run_in_background=True*. Foreground tasks that block
                  execution of plans and other tasks are not counted.

              **plan_queue_mode**: *dict*
                  the dictionary of parameters that determine queue execution mode. The key/value pairs
                  in the dictionary represent parameter names and values. Supported parameters:
                  *'loop'* (*boolean*) - indicates if the LOOP mode is enabled.

              **queue_stop_pending**: *boolean*
                  indicates if the request to stop the queue after completion of the current plan is
                  pending.

              **pause_pending**: *boolean*
                  indicates if the request to pause the currently running plan was send to Run Engine
                  (see **re_pause** method), but the plan is not stopped yet. It may take considerable
                  time for deferred pause to be processed.

              **worker_environment_exists**: *boolean*
                  indicates if RE Worker environment was created and plans could be executed.

              **lock_info_uid**: *str*
                  UID of **lock_info** (see **lock** and **lock_info** API). Reload *lock_info* using
                  **lock_info** API when the UID is changed.

              **lock**: *dict*
                  The dictionary contains information on current status of the lock:

                  - **environment** (*boolean*) - indicates if the RE Worker environment is locked.
                    See the **lock** API for details.

                  - **queue** (*boolean*) - indicates if the queue is locked. See the **lock** API
                    for details.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_plans_allowed:

**'plans_allowed'**
^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'plans_allowed'**
------------  -----------------------------------------------------------------------------------------
Description   Returns a dictionary that contains information on the allowed plans for a given user
              group. Monitor *'plans_allowed_uid'* status field and download the list from the
              server only when the UID is changed.
------------  -----------------------------------------------------------------------------------------
Parameters    **user_group**: *str*
                  the name of the user group (e.g. 'primary').
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **plans_allowed**: *dict*
                  the dictionary that contains information on the allowed plans.
                  Dictionary keys are plan names.

              **plans_allowed_uid**: *str* or *None*
                  UID of the list of allowed plans, *None* if the request fails.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_devices_allowed:

**'devices_allowed'**
^^^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'devices_allowed'**
------------  -----------------------------------------------------------------------------------------
Description   Returns a dictionary that contains information on the allowed devices for a given user
              group. Monitor *'devices_allowed_uid'* status field and download the list from the
              server only when the UID is changed.
------------  -----------------------------------------------------------------------------------------
Parameters    **user_group**: *str*
                  the name of the user group (e.g. 'primary').
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **devices_allowed**: *dict*
                  the dictionary that contains information on the allowed devices.
                  Dictionary keys are device names.

              **devices_allowed_uid**: *str* or *None*
                  UID of the list of allowed devices, *None* if the request fails.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_plans_existing:

**'plans_existing'**
^^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'plans_existing'**
------------  -----------------------------------------------------------------------------------------
Description   Returns a dictionary that contains information on existing plans in the RE Worker
              namespace. Monitor *'plans_existing_uid'* status field and download the list from the
              server only when the UID is changed.
------------  -----------------------------------------------------------------------------------------
Parameters    ---
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **plans_existing**: *dict*
                  the dictionary that contains information on the existing plans.
                  Dictionary keys are plan names.

              **plans_existing_uid**: *str* or *None*
                  UID of the list of existing plans, *None* if the request fails.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_devices_existing:

**'devices_existing'**
^^^^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'devices_existing'**
------------  -----------------------------------------------------------------------------------------
Description   Returns a dictionary that contains information on the existing devices in RE Worker
              namespace. Monitor *'devices_existing_uid'* status field and download the list from the
              server only when the UID is changed.
------------  -----------------------------------------------------------------------------------------
Parameters    ---
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **devices_existing**: *dict*
                  the dictionary that contains information on the existing devices.
                  Dictionary keys are device names.

              **devices_existing_uid**: *str* or *None*
                  UID of the list of existing devices, *None* if the request fails
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_permissions_reload:

**'permissions_reload'**
^^^^^^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'permissions_reload'**
------------  -----------------------------------------------------------------------------------------
Description   Reload user group permissions from the default location or the location set using
              command line parameters and generate lists of allowed plans and devices based on
              the lists of existing plans and devices. By default, the method will use current lists
              of existing plans and devices stored in memory. Optionally the method can reload the
              lists from the disk file (see *restore_plans_devices* parameter). The method always
              updates UIDs of the lists of allowed plans and devices even if the contents remain
              the same.
------------  -----------------------------------------------------------------------------------------
Parameters    **restore_plans_devices**: *boolean* (optional)
                  reload the lists of existing plans and devices from disk if *True*, otherwise
                  use current lists stored in memory. Default: *False*.

              **restore_permissions**: *boolean* (optional)
                  reload user group permissions from disk if *True*, otherwise use current permissions.
                  Default: *True*.

              **lock_key**: *str* (optional)
                  Lock key. The API fails if **the queue** is locked and no valid key is submitted
                  with the request. See documentation on :ref:`method_lock` API for more details.
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_permissions_get:

**'permissions_get'**
^^^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'permissions_get'**
------------  -----------------------------------------------------------------------------------------
Description   Download the dictionary of user group permissions currently used by RE Manager.
------------  -----------------------------------------------------------------------------------------
Parameters    ---
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **user_group_permissions**: *dict*
                  dictionary containing user group permissions.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_permissions_set:

**'permissions_set'**
^^^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'permissions_set'**
------------  -----------------------------------------------------------------------------------------
Description   Uploads the dictionary of user group permissions. If the uploaded dictionary contains
              a valid set of permissions different from currently used one, the new permissions
              are set as current and the updated lists of allowed plans and devices are generated.
              The method does nothing if the uploaded permissions are identical to currently used
              permissions. The API request fails if the uploaded dictionary does not pass validation.
------------  -----------------------------------------------------------------------------------------
Parameters    **user_group_permissions**: *dict*
                  dictionary, which contains user group permissions.

              **lock_key**: *str* (optional)
                  Lock key. The API fails if **the queue** is locked and no valid key is submitted
                  with the request. See documentation on :ref:`method_lock` API for more details.
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_history_get:

**'history_get'**
^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'history_get'**
------------  -----------------------------------------------------------------------------------------
Description   Returns the list of items in the plan history.

              *The request always succeeds*.
------------  -----------------------------------------------------------------------------------------
Parameters    ---
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **items**: *list*
                  list of items in the plan history, each item is represented by a dictionary of
                  item parameters. Currently the plan history may contain only plans.

                  The dictionary contains a copy of the original item parameters and dictionary with results of
                  execution (**'result'** key), which contains the following keys:

                  - **exit_status** - exit status of the plan. The values are **'completed'** (the plan execution
                    is successfully completed), **'failed'** (the plan execution failed; a plan can fail due to
                    multiple reasons, including internal error of RE Manager; see the error message to determine
                    the reason of failure), **'stopped'** (the plan was paused, then stopped; the plan is considered
                    successfully executed), **'abort'** and **'halt'** (the plan was paused, then aborted or halted;
                    the plan is considered failed), **'unknown'** (the exit status information is lost, e.g. due
                    to restart of RE Manager process, but plan information still needs to be placed in the history;
                    this is very unlikely to happen in practice);

                  - **run_uids** - list of UIDs of runs executed by the plan;

                  - **time_start** and **time_stop** - time of start and completion of the plan (not runs),
                    floating point number returned by *time.time()*.

                  - **msg** - error message if the plan failed, empty string otherwise.

                  - **traceback** - full traceback if the plan failed, empty string otherwise.

              **plan_history_uid**: *str*
                  current plan history UID.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_history_clear:

**'history_clear'**
^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'history_clear'**
------------  -----------------------------------------------------------------------------------------
Description   Clear the contents of the plan history.

              *The request always succeeds*.
------------  -----------------------------------------------------------------------------------------
Parameters    **lock_key**: *str* (optional)
                  Lock key. The API fails if **the queue** is locked and no valid key is submitted
                  with the request. See documentation on :ref:`method_lock` API for more details.
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_environment_open:

**'environment_open'**
^^^^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'environment_open'**
------------  -----------------------------------------------------------------------------------------
Description   Initiate the creation of a new RE Worker environment. The request will
              succeed only if RE Manager is in 'idle' state and the environment does not exist.
              RE Worker environment must be created before plans could be executed. To restart
              the environment, close the existing environment first by sending 'environment_close'
              request. To destroy 'frozen' environment, use 'environment_destroy' method.
------------  -----------------------------------------------------------------------------------------
Parameters    **lock_key**: *str* (optional)
                  Lock key. The API fails if **the environment** is locked and no valid key is submitted
                  with the request. See documentation on :ref:`method_lock` API for more details.
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.
------------  -----------------------------------------------------------------------------------------
Execution     The request only initiates the sequence of creating a new environment. It may take
              a long time to start the environment. Monitor 'manager_state'
              and 'worker_environment_exists' status fields ('status' method) to detect when
              the operation is completed: 'manager_state' should have the value 'opening_environment'
              while operation is in progress and change to 'idle' when the operation completes and
              'worker_environment_exists' is set to True if environment was created successfully.
============  =========================================================================================


.. _method_environment_close:

**'environment_close'**
^^^^^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'environment_close'**
------------  -----------------------------------------------------------------------------------------
Description   Initiate the operation of closing the existing RE Worker environment. Fails if there
              is no existing environment or if RE Manager is not in 'idle' state. Use
              'environment_destroy' method to close a non-responsive RE Worker environment.
------------  -----------------------------------------------------------------------------------------
Parameters    **lock_key**: *str* (optional)
                  Lock key. The API fails if **the environment** is locked and no valid key is submitted
                  with the request. See documentation on :ref:`method_lock` API for more details.
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.
------------  -----------------------------------------------------------------------------------------
Execution     The request initiates the sequence of closing the environment.
              Monitor 'manager_state' and 'worker_environment_exists' status fields
              (see 'status' method) to detection when the operation coompletes:
              'manager_state' is expected to have the value 'closing_environment'
              while operation is in progress and switch to 'idle' when the operation completes
              and 'worker_environment_exists' is set False if environment was closed successfully.
============  =========================================================================================


.. _method_environment_destroy:

**'environment_destroy'**
^^^^^^^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'environment_destroy'**
------------  -----------------------------------------------------------------------------------------
Description   Initiate the operation of destroying of the existing (unresponsive) RE Worker
              environment. The operation fails if there is no existing environment.
              The request is accepted by the server if status fields **worker_environment_exists** is
              *True* or **manager_state** is *'creating_environment'*, otherwise the request is
              rejected.
------------  -----------------------------------------------------------------------------------------
Parameters    **lock_key**: *str* (optional)
                  Lock key. The API fails if **the environment** is locked and no valid key is submitted
                  with the request. See documentation on :ref:`method_lock` API for more details.
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.
------------  -----------------------------------------------------------------------------------------
Execution     The request initiates the sequence of destroying the environment.
              Monitor 'manager_state' and 'worker_environment_exists' status fields
              (see 'status' method): 'manager_state' is expected to have the value
              'destroying_environment' while operation is in progress and switch to 'idle' when
              the operation completes and 'worker_environment_exists' is set False if environment
              was destroyed successfully.
============  =========================================================================================


.. _method_queue_mode_set:

**'queue_mode_set'**
^^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'queue_mode_set'**
------------  -----------------------------------------------------------------------------------------
Description   Sets parameters that define the mode of plan queue execution. The key/value pairs of the *'mode'*
              dictionary represent names and values of the parameters that need to be changed.
              If *mode={}* then the mode will not be changed. To reset the mode parameters to the
              built-in default values, set *mode="default"*. The request fails if the *'mode'* parameter
              is not a dictionary or the *'default'* string, the dictionary contains unsupported keys
              (mode parameters) or key values are of unsupported type. Supported mode parameters
              (dictionary keys): *'loop'* (*True/False*) - enables/disables loop mode.
------------  -----------------------------------------------------------------------------------------
Parameters    **mode**: *dict* or *str*
                  the dictionary of queue mode parameters or *'default'* string. Supported keys of
                  the dictionary: *'loop'* (*boolean*).

              **lock_key**: *str* (optional)
                  Lock key. The API fails if **the queue** is locked and no valid key is submitted
                  with the request. See documentation on :ref:`method_lock` API for more details.
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_queue_get:

**'queue_get'**
^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'queue_get'**
------------  -----------------------------------------------------------------------------------------
Description   Returns the items in the plan queue. The returned list of items may contain plans or
              instructions. Each item is represented as a dictionary. Plans and instructions can be
              distinguished by checking the value with the key 'item_type': 'plan' indicates that
              the item is a plan, while 'instruction' indicates that it is an instruction.

              *The request always succeeds*.
------------  -----------------------------------------------------------------------------------------
Parameters    ---
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **items**: *list*
                  list of queue items

              **running_item**: *dict*
                  parameters of the item representing currently running plan, empty dictionary ({}) is
                  returned if no plan is currently running.

              **plan_queue_uid**: *str*
                  current plan queue UID.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_queue_item_add:

**'queue_item_add'**
^^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'queue_item_add'**
------------  -----------------------------------------------------------------------------------------
Description   Add item to the queue. The item may be a plan or an instruction. By default the
              item is added to the back of the queue. Alternatively the item can be placed at
              the desired position in the queue or before or after one of the existing items.
------------  -----------------------------------------------------------------------------------------
Parameters    **item**: *dict*
                  the dictionary of plan or instruction parameters. Plans are distinguished from
                  instructions based the value of the required parameter 'item_type'. Currently
                  supported item types are 'plan' and 'instruction'.

              **user_group**: *str*
                  the name of the user group (e.g. 'primary').

              **user**: *str*
                  the name of the user (e.g. 'Default User'). The name is included in the item metadata
                  and may be used to identify the user who added the item to the queue. It is not
                  passed to the Run Engine or included in run metadata.

              **pos**: *int*, *'front'* or *'back'* (optional)
                  position of the item in the queue. RE Manager will attempt to insert the item
                  at the specified position. The position may be positive or negative (counted
                  from the back of the queue) integer. If 'pos' value is a string 'front' or 'back',
                  then the item is inserted at the front or the back of the queue.

              **before_uid**, **after_uid**: *str* (optional)
                  insert the item before or after the item with the given item UID.

              **lock_key**: *str* (optional)
                  Lock key. The API fails if **the queue** is locked and no valid key is submitted
                  with the request. See documentation on :ref:`method_lock` API for more details.

              *Parameters 'pos', 'before_uid' and 'after_uid' are mutually exclusive.*
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **qsize**: *int* or *None*
                  the number of items in the plan queue after the plan was added if
                  the operation was successful, *None* otherwise

              **item**: *dict* or *None* (optional)
                  the inserted item. The item contains the assigned item UID. In case of error
                  the item may be returned without modification (with assigned UID). *None* will be
                  returned if request does not contain item parameters.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_queue_item_add_batch:

**'queue_item_add_batch'**
^^^^^^^^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'queue_item_add_batch'**
------------  -----------------------------------------------------------------------------------------
Description   Add a batch of items to the queue. The behavior of the function is identical to
              **queue_item_add**, except that it adds the batch of items instead of a single item.
              The batch may consist of any number of supported items (see **queue_item_add** method).
              Each item in the queue must successfully pass validation before any items are added
              to the queue. If an item fails validation, the whole batch is rejected. In case the
              batch is rejected, the function returns the detailed report for each item: *success*
              status indicating if the item passed validation and error message in case
              validation failed.
------------  -----------------------------------------------------------------------------------------
Parameters    **items**: *list*
                  the list containing a batch of items. Each element is a dictionary containing
                  valid set of item parameters (see instructions for *queue_item_add* API).
                  An empty item list will also be accepted.

              **user_group**: *str*
                  the name of the user group (e.g. 'primary').

              **user**: *str*
                  the name of the user (e.g. 'Default User'). The name is included in the item metadata
                  and may be used to identify the user who added the item to the queue. It is not
                  passed to the Run Engine or included in run metadata.

              **pos**: *int*, *'front'* or *'back'* (optional)
                  position of the first item of the batch in the queue. RE Manager will attempt to
                  insert the batch of items so that the first item in the batch is at the specified
                  position. The position may be positive or negative (counted from the back of the queue)
                  integer. If 'pos' value is a string 'front' or 'back', then the items are inserted at
                  the front or the back of the queue.

              **before_uid**, **after_uid**: *str* (optional)
                  insert the batch of items before or after the item with the given item UID.

              **lock_key**: *str* (optional)
                  Lock key. The API fails if **the queue** is locked and no valid key is submitted
                  with the request. See documentation on :ref:`method_lock` API for more details.

              *Parameters 'pos', 'before_uid' and 'after_uid' are mutually exclusive.*
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully. The request fails if any item
                  fails validation. The queue is not expected to be modified if the request fails.
                  If the parameter is *True*, then validation of all items succeeded: there is no
                  need to verify *success* status for each item returned in *item_list* parameter.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **qsize**: *int* or *None*
                  the number of items in the plan queue after processing the request. The correct
                  queue size may be returned even if the operation fails. In rare failing cases
                  the parameter may return *None*.

              **items**: *list*
                  the list of processed items. Each item contains inserted item (in case of success),
                  passed item or None (in case of error). See notes for return *item* parameter
                  for *queue_add_item* API.

              **results**: *list*
                  the list of reports for each processed item. The size of the list is equal to the
                  size of the list returned as *items* parameter. Each element of the list is
                  a dictionary containing the following keys:

                - **success** - boolean value indicating if the validation of the item was successful.

                - **msg** - error message in case validation of the item failed.

------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_queue_item_update:

**'queue_item_update'**
^^^^^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'queue_item_update'**
------------  -----------------------------------------------------------------------------------------
Description   Update the existing item in the queue. The method is intended for editing queue items,
              but may be used for replacing the existing items with completely different ones.
              The updated item may be a plan or an instruction. The item parameter 'item_uid' must
              be set to a UID of an existing queue item that is expected to be replaced. The method
              fails if the item UID is not found. By default, the UID of the updated item is not changed
              and 'user' and 'user_group' parameters are set to the values provided in the request.
              The 'user_group' is also used for validation of submitted item. If it is preferable
              to replace the item UID with a new random UID (e.g. if the item is replaced with
              completely different item), the method should be called with the optional parameter
              'replace=True'.
------------  -----------------------------------------------------------------------------------------
Parameters    **item**: *dict*
                  the dictionary of plan or instruction parameters. Plans are distinguished from
                  instructions based the value of the required parameter 'item_type'. Currently
                  supported item types are 'plan' and 'instruction'.

              **user_group**: *str*
                  the name of the user group (e.g. 'primary').

              **user**: *str*
                  the name of the user (e.g. 'Default User'). The name is included in the item metadata
                  and may be used to identify the user who added the item to the queue. It is not
                  passed to the Run Engine or included in run metadata.

              **replace**: *boolean* (optional)
                  replace the updated item UID with the new random UID (True) or keep the original
                  UID (False). Default value is (False).

              **lock_key**: *str* (optional)
                  Lock key. The API fails if **the queue** is locked and no valid key is submitted
                  with the request. See documentation on :ref:`method_lock` API for more details.
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **qsize**: *int* or *None*
                  the number of items in the plan queue after the plan was added if
                  the operation was successful, *None* otherwise

              **item**: *dict* or *None* (optional)
                  the inserted item. The item contains the assigned item UID. In case of error
                  the item may be returned without modification (with assigned UID). *None* will be
                  returned if request does not contain item parameters.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_queue_item_get:

**'queue_item_get'**
^^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'queue_item_get'**
------------  -----------------------------------------------------------------------------------------
Description   Read item from the queue. By default the item from the back of the queue is returned.
              Alternatively the item at the given position or the item with the given UID
              may be requested.
------------  -----------------------------------------------------------------------------------------
Parameters    **pos**: *int*, *'front'* or *'back'* (optional)
                  position of the item in the queue. RE Manager will attempt to return the item
                  at the specified position. The position may be positive or negative (counted
                  from the back of the queue) integer. If 'pos' value is a string 'front' or 'back',
                  then the item at the front or the back of the queue is returned.

              **uid**: *str* (optional)
                  uid of the requested item.

              *Parameters 'pos' and 'uid' are mutually exclusive.*
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **item**: *dict*
                  the dictionary of item parameters, ({}) if operation failed.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_queue_item_remove:

**'queue_item_remove'**
^^^^^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'queue_item_remove'**
------------  -----------------------------------------------------------------------------------------
Description   Remove item from the queue. By default the last item in the queue is removed.
              Alternatively the position or UID of the item can be specified.
------------  -----------------------------------------------------------------------------------------
Parameters    **pos**: *int*, *'front'* or *'back'* (optional)
                  position of the item in the queue. RE Manager will attempt to remove the item
                  at the specified position. The position may be positive or negative (counted
                  from the back of the queue) integer. If 'pos' value is a string 'front' or 'back',
                  then the item is removed from the front or the back of the queue.

              **uid**: *str* (optional)
                  uid of the requested item.

              **lock_key**: *str* (optional)
                  Lock key. The API fails if **the queue** is locked and no valid key is submitted
                  with the request. See documentation on :ref:`method_lock` API for more details.

              *Parameters 'pos' and 'uid' are mutually exclusive.*
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **item**: *dict*
                  the dictionary of parameters of the removed item, ({}) if operation failed.

              **qsize**: *int* or *None*
                  the number of items in the plan queue after the plan was added if
                  the operation was successful, *None* otherwise
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_queue_item_remove_batch:

**'queue_item_remove_batch'**
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'queue_item_remove_batch'**
------------  -----------------------------------------------------------------------------------------
Description   Remove a batch of items from the queue. The batch of items is defined as a list
              of item UIDs and passed as the parameter *'uids'*. The list of UIDs may be empty.
              By default, the function does not validate the batch and deletes all batch items
              found in the queue. Batch validation may be enabled by setting *'ignore_missing=False'*.
              In this case the method succeeds only if the batch does not contain repeated items
              and all items are found in the queue. If validation fails then no items are removed
              from the queue.
------------  -----------------------------------------------------------------------------------------
Parameters    **uids**: *list(str)* (*required*)
                  list of UIDs of the items in the batch. The list may not contain repeated UIDs.
                  All UIDs must be present in the queue. The list may be empty.

              **ignore_missing**: *boolean* (*optional, default: True*)
                  if the value is *'False'*, then the method fails if the batch contains repeating
                  items or some of the batch items are not found in the queue. If *'True'* (default),
                  then the method attempts to remove all items in the batch and ignores missing items.
                  The method returns the list of items that were removed from the queue.

              **lock_key**: *str* (optional)
                  Lock key. The API fails if **the queue** is locked and no valid key is submitted
                  with the request. See documentation on :ref:`method_lock` API for more details.
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **items**: *list(dict)*
                  the list of items that were removed during the operation. The items in the list are
                  arranged in the order in which they were removed. Returns empty list if the operation
                  fails.

              **qsize**: *int* or *None*
                  the size of the queue or *None* if operation fails.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_queue_item_move:

**'queue_item_move'**
^^^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'queue_item_move'**
------------  -----------------------------------------------------------------------------------------
Description   Move item to a different position in the queue.
------------  -----------------------------------------------------------------------------------------
Parameters    **pos**: *int*, *'front'* or *'back'*
                  current position of the item in the queue. Integer number can be negative.

              **uid**: *str* (optional)
                  uid of the item to move.

              **pos_dest**: *int*, *'front'*, *'back'*
                  new position of the item. Integer number can be negative.

              **before_uid**, **after_uid**: *str*
                  UID of an existing item in the queue. The selected item will be moved
                  before or after this item.

              **lock_key**: *str* (optional)
                  Lock key. The API fails if **the queue** is locked and no valid key is submitted
                  with the request. See documentation on :ref:`method_lock` API for more details.

              *Parameters 'pos' and 'uid' are mutually exclusive, but at least one of them must
              be specified.*

              *Parameters 'pos_dest', 'before_uid' and 'after_uid' are mutually exclusive,
              but at least one of them must be specified.*
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **item**: *dict*
                  the dictionary of parameters of the moved item, ({}) if operation failed.

              **qsize**: *int* or *None*
                  the size of the queue.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_queue_item_move_batch:

**'queue_item_move_batch'**
^^^^^^^^^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'queue_item_move_batch'**
------------  -----------------------------------------------------------------------------------------
Description   Move a batch of item to a different position in the queue.
              The method accepts a list of UIDs of the items included in the batch. The UIDs in the
              list must be unique (not repeated) and items with listed UIDs must exist in the queue.
              If the list is empty, then operation succeeds and the queue remains unchanged.
              The destination must be specified using one of the mutually exclusive parameters
              *'pos_dest'*, *'before_uid'* or *'after_uid'*. The reference item with the UID of
              passed with the parameters *'before_uid'* or *'after_uid'* must not be in the batch.
              The parameter *'reorder'* controls the order of the items in the moved batch and
              indicates whether items in the batch should be reordered with respect to the order
              of UIDs in the list *'uids'*. The batch may include any set of non-repeated items
              from the queue arranged in arbitrary order. By default (*reorder=False*) the batch
              is inserted in the specified position as a contiguous sequence of items ordered
              according to the UIDs in the list *'uids'*. If *reorder=True*, then the inserted
              items are ordered according to their original positions in the queue. It is assumed
              that the method will be mostly used with the default ordering option and user will
              be responsible for creating properly ordered lists of items. The other option is
              implemented for the cases when the user may want to submit randomly ordered lists of
              UIDs, but preserve the original order of the moved batch.
------------  -----------------------------------------------------------------------------------------
Parameters    **uids**: *list(str)* (*required*)
                  list of UIDs of the items in the batch. The list may not contain repeated UIDs.
                  All UIDs must be present in the queue. The list may be empty.

              **pos_dest**: *'front'* or *'back'*
                  new position of the item. Only string values *"front"* and *"back"* are accepted.

              **before_uid**, **after_uid**: *str*
                  UID of an existing item in the queue. The selected item will be moved
                  before or after this item. The item with the specified UID may not be included
                  in the batch.

              **reorder**: *boolean* (*optional, default: False*)
                  Arranged moved items in the order of UIDs in the *'uids'* list
                  (*False*) or according to the original item positions in the queue (*True*).

              **lock_key**: *str* (optional)
                  Lock key. The API fails if **the queue** is locked and no valid key is submitted
                  with the request. See documentation on :ref:`method_lock` API for more details.

              *Parameters 'pos_dest', 'before_uid' and 'after_uid' are mutually exclusive,
              but at least one of them must be specified.*
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **items**: *list(dict)*
                  the list of items that were moved during the operation. The items in the list are
                  arranged in the order in which they are inserted in the queue. Returns empty list
                  if the operation fails.

              **qsize**: *int* or *None*
                  the size of the queue or *None* if operation fails.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_queue_item_execute:

**'queue_item_execute'**
^^^^^^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'queue_item_execute'**
------------  -----------------------------------------------------------------------------------------
Description   Immediately start execution of the submitted item. The item may be a plan or an
              instruction. The request fails if item execution can not be started immediately
              (RE Manager is not in *IDLE* state, RE Worker environment does not exist, etc.).
              If the request succeeds, the item is executed once. The item is not added to
              the queue if it can not be immediately started and it is not pushed back into
              the queue in case its execution fails/stops. If the queue is in the *LOOP* mode,
              the executed item is not added to the back of the queue after completion.
              The API request does not alter the sequence of enqueued plans.

              The API is primarily intended for implementing of interactive workflows, in which
              users are controlling the experiment using client GUI application and user actions
              (such as mouse click on a plot) are converted into the requests to execute plans
              in RE Worker environment. Interactive workflows may be used for calibration of
              the instrument, while the queue may be used to run sequences of scheduled experiments.

              The item is not added to the queue or change the existing queue. The API modifies
              **plan_queue_uid** status parameter, which is used for monitoring updates of
              the queue and running items. If the item is a plan, the results of execution
              are added to plan history as usual. The respective history item could be accessed
              to check if the plan was executed successfully.

              The API **does not start execution of the queue**. Once execution of the submitted
              item is finished, RE Manager is switched to the IDLE state.
------------  -----------------------------------------------------------------------------------------
Parameters    **item**: *dict*
                  the dictionary of plan or instruction parameters. Plans are distinguished from
                  instructions based the value of the required parameter 'item_type'. Currently
                  supported item types are 'plan' and 'instruction'.

              **user_group**: *str*
                  the name of the user group (e.g. 'primary').

              **user**: *str*
                  the name of the user (e.g. 'Default User'). The name is included in the item metadata
                  and may be used to identify the user who added the item to the queue. It is not
                  passed to the Run Engine or included in run metadata.

              **lock_key**: *str* (optional)
                  Lock key. The API fails if **the environment** is locked and no valid key is submitted
                  with the request. See documentation on :ref:`method_lock` API for more details.
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **qsize**: *int* or *None*
                  the number of items in the plan queue, *None* if request fails otherwise.

              **item**: *dict* or *None* (optional)
                  the inserted item. The item contains the assigned item UID. In case of error
                  the item may be returned without modification (with assigned UID). *None* is
                  returned if request does not contain item parameters.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_queue_clear:

**'queue_clear'**
^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'queue_clear'**
------------  -----------------------------------------------------------------------------------------
Description   Remove all items from the plan queue. The currently running plan does not belong to
              the queue and is not affected by this operation. If the plan fails or its execution
              is stopped, it will be pushed to the beginning of the queue.
------------  -----------------------------------------------------------------------------------------
Parameters    **lock_key**: *str* (optional)
                  Lock key. The API fails if **the queue** is locked and no valid key is submitted
                  with the request. See documentation on :ref:`method_lock` API for more details.
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_queue_start:

**'queue_start'**
^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'queue_start'**
------------  -----------------------------------------------------------------------------------------
Description   Start execution of the queue. The operation succeeds only if RE Manager is in
              'idle' state and RE Worker environment exists. This operation only initiates
              the process of starting the execution of the queue.
------------  -----------------------------------------------------------------------------------------
Parameters    **lock_key**: *str* (optional)
                  Lock key. The API fails if **the environment** is locked and no valid key is submitted
                  with the request. See documentation on :ref:`method_lock` API for more details.
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.
------------  -----------------------------------------------------------------------------------------
Execution     The request initiates the operation of starting the queue. Verify that the queue
              is running by checking the status fields: 'manager_state' is expected to have
              the value 'executing_queue' and 'running_item_uid' should return UID of
              the running plan. It is possible that the queue execution is successfully started,
              but immediately stopped (queue is empty, queue contains a very short plan,
              the first plan in the queue fails to start, the first item in the queue is 'queue_stop'
              instruction etc.). RE Manager is expected to handle those cases in orderly way, but
              the client should be capable of detecting and handling those events as well.
============  =========================================================================================


.. _method_queue_stop:

**'queue_stop'**
^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'queue_stop'**
------------  -----------------------------------------------------------------------------------------
Description   Request RE Manager to stop execution of the queue after completion of the currently
              running plan. The request succeeds only if the queue is currently running
              ('manager_state' status field has value 'executing_queue'). The 'queue_stop_pending'
              status field can be used at any time to verify if the request is pending.
------------  -----------------------------------------------------------------------------------------
Parameters    **lock_key**: *str* (optional)
                  Lock key. The API fails if **the environment** is locked and no valid key is submitted
                  with the request. See documentation on :ref:`method_lock` API for more details.
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_queue_stop_cancel:

**'queue_stop_cancel'**
^^^^^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'queue_stop_cancel'**
------------  -----------------------------------------------------------------------------------------
Description   Cancel the pending request to stop execution of the queue after the currently
              running plan.

              *The request always succeeds*.
------------  -----------------------------------------------------------------------------------------
Parameters    **lock_key**: *str* (optional)
                  Lock key. The API fails if **the environment** is locked and no valid key is submitted
                  with the request. See documentation on :ref:`method_lock` API for more details.
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_re_pause:

**'re_pause'**
^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'re_pause'**
------------  -----------------------------------------------------------------------------------------
Description   Request Run Engine to pause currently running plan. The request will fail if RE Worker
              environment does not exist or no plan is currently running. The request only initates
              the sequence of pausing the plan.

              If *deferred* pause is requested past the last checkpoint of the plan, the plan is run
              to completion and the queue is stopped. The stopped queue can not be resumed using
              **re_resume** method, instead **queue_start** method should be used to restart the queue.
              Check **manager_state** status flag to determine if the queue is stopped (*'idle'* state)
              or Run Engine is paused (*'paused'* state).

              The **pause_pending** status flag is set if pause request is successfully passed to Run
              Engine. It may take significant time for deferred pause to be processed. The flag
              is cleared once the pending pause request is processed (the plan is paused or plan
              is completed and the queue is stopped).
------------  -----------------------------------------------------------------------------------------
Parameters    **option**: *'immediate'* or *'deferred'* (optional)
                  pause the plan immediately (roll back to the previous checkpoint) or continue
                  to the next checkpoint. Default: *'deferred'*.

              **lock_key**: *str* (optional)
                  Lock key. The API fails if **the environment** is locked and no valid key is submitted
                  with the request. See documentation on :ref:`method_lock` API for more details.
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.
------------  -----------------------------------------------------------------------------------------
Execution     The request only initiates the operation of pausing the plan. Wait until the plan is
              paused by polling 'manager_state' status field (expected value is 'paused').
============  =========================================================================================


.. _method_re_resume_stop_abort_halt:

**'re_resume', 're_stop', 're_abort', 're_halt'**
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'re_resume'**, **'re_stop'**, **'re_abort'**, **'re_halt'**
------------  -----------------------------------------------------------------------------------------
Description   Request Run Engine to resume, stop, abort or halt a paused plan. Fails if RE Worker
              environment does not exist or 'manager_state' status field value is not 'paused'.
------------  -----------------------------------------------------------------------------------------
Parameters    ---
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **lock_key**: *str* (optional)
                  Lock key. The API fails if **the environment** is locked and no valid key is submitted
                  with the request. See documentation on :ref:`method_lock` API for more details.
------------  -----------------------------------------------------------------------------------------
Execution     The request only initiates the operation. Wait until the plan is paused by monitoring
              'manager_state' status field (expected value is 'executing_queue' if execution is
              resumed or 'idle' if execution was stopped).
============  =========================================================================================


.. _method_re_runs:


**'re_runs'**
^^^^^^^^^^^^^

============  =========================================================================================
Method        **'re_runs'**
------------  -----------------------------------------------------------------------------------------
Description   Request the list of active runs generated by the currently executed plans.
              The full list of active runs includes the runs that are currently open ('open' runs) and
              the runs that were already closed ('closed' runs). Simple single-run plans will have
              at most one run in the list. Monitor 'run_list_uid' RE Manager status field and
              retrieve the updated list once UID is changed. The UID of the retrieved list is
              included in the returned parameters
------------  -----------------------------------------------------------------------------------------
Parameters    **option**: *'active'*, *'open'* or *'closed'* (optional)
                  select between full list of 'active' (default) runs, the list of 'open' or 'closed'
                  runs.
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **run_list**: *list(dict)*
                  the requested list of runs, list items are dictionaries with keys 'uid' (str),
                  'is_open' (boolean) and 'exit_status' (str or None). See Bluesky documentation
                  for 'exit_status' values.

              **run_list_uid**: str
                  UID of the returned run list, identical to the RE Manager status field with
                  the same name.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_script_upload:

**'script_upload'**
^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'script_upload'**
------------  -----------------------------------------------------------------------------------------
Description   Upload and execute script in RE Worker namespace. The script may add, change or replace
              objects defined in the namespace, including plans and devices. Dynamic modification
              of the worker namespace may be used to implement more flexible workflows. The API call
              updates the lists of existing and allowed plans and devices if necessary. Changes in
              the lists will be indicated by changed list UIDs. Use *'task_result'* API to check
              if the script was loaded correctly. Note, that if the task fails, the script is
              still executed to the point where the exception is raised and the respective changes
              to the environment are applied.
------------  -----------------------------------------------------------------------------------------
Parameters    **script**: *str*
                  The string that contains the Python script. The rules for the script are the same
                  as for Bluesky startup scripts. The script can use objects already existing in
                  the RE Worker namespace.

              **update_lists**: *boolean* (optional, default *True*)
                  Update lists of existing and available plans and devices after execution of the script.
                  It is required to update the lists if the script adds or modifies plans and/or devices
                  in RE Worker namespace, but it is more efficient to disable the update for other scripts,
                  e.g. the scripts that print or modify variables in the namespace during iteractive debug
                  session.

              **update_re**: *boolean* (optional, default *False*)
                  The uploaded scripts may replace Run Engine (*'RE'*) and Data Broker (*'db'*)
                  instances in the namespace. In most cases this operation should not be allowed,
                  therefore it is disabled by default (*update_re* is *False*), i.e. if the script
                  creates new *RE* and *db* objects, those objects are discarded. Set this parameter
                  *True* to allow the server to replace *RE* and *db* objects. This parameter has
                  no effect if the script is not creating new instances of *RE* and/or *db*.

              **run_in_background**: *boolean* (optional, default *False*)
                  Set this parameter *True* to upload and execute the script in the background
                  (while a plan or another foreground task is running). Generally, it is not
                  recommended to update RE Worker namespace in the background. Background tasks
                  are executed in separate threads and only thread-safe scripts should be uploaded
                  in the background. **Developers of data acquisition workflows and/or user
                  specific code are responsible for thread safety.**

              **lock_key**: *str* (optional)
                  Lock key. The API fails if **the environment** is locked and no valid key is submitted
                  with the request. See documentation on :ref:`method_lock` API for more details.
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **task_uid**: *str* or *None*
                  Task UID can be used to check status of the task and download results once the task
                  is completed (see *task_result* API).
------------  -----------------------------------------------------------------------------------------
Execution     The method initiates the operation. Monitor *task_results_uid* status field and call
              *task_result* API to check for success.
============  =========================================================================================


.. _method_function_execute:


**'function_execute'**
^^^^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'function_execute'**
------------  -----------------------------------------------------------------------------------------
Description   Start execution of a function in RE Worker namespace. The function must be defined in the
              namespace (in startup code or a script uploaded using *script_upload* method. The function
              may be executed as a foreground task (only if RE Manager and RE Worker environment are idle)
              or as a background task. Background tasks are executed in separate threads and may
              consume processing or memory resources and interfere with running plans or other tasks.
              RE Manager does not guarantee thread safety of the user code running in the background.
              Developers of startup code are fully responsible for preventing threading issues.

              The method allows to pass parameters (*args* and *kwargs*) to the function. Once the task
              is completed, the results of the function execution, including the return value, can be
              loaded using *task_result* method. If the task fails, the return value is *None* and
              the error message and traceback are included in the result. The data types of
              parameters and return values must be JSON serializable. The task fails if the return
              value can not be serialized.

              The method only **initiates** execution of the function. If the request is successful
              (*success=True*), the server starts the task, which attempts to execute the function
              with given name and parameters. The function may still fail start (e.g. if the user is
              permitted to execute function with the given name, but the function is not defined
              in the namespace). Use *'task_result'* method with the returned *task_uid* to
              check the status of the tasks and load the result upon completion.
------------  -----------------------------------------------------------------------------------------
Parameters    **item**: *dict*
                  the dictionary that contains function name and parameters. The structure of
                  dictionary is identical to *item* representing a plan or an instruction,
                  except that *item_type* is *'function'*.

              **user_group**: *str*
                  the name of the user group (e.g. 'primary').

              **user**: *str*
                  the name of the user (e.g. 'Default User'). The name is included in the item metadata
                  and may be used to identify the user who submitted the item.

              **run_in_background**: *boolean* (optional, default *False*)
                  Set this parameter *True* to start a background task. Background tasks can be
                  started and executed while a plan or another foreground task is running.
                  If workflow requires executing background tasks, user code should be analyzed
                  for thread safety to ensure there are no potential threading issues.
                  **Developers of data acquisition workflows and/or user specific code are
                  responsible for thread safety.**

              **lock_key**: *str* (optional)
                  Lock key. The API fails if **the environment** is locked and no valid key is submitted
                  with the request. See documentation on :ref:`method_lock` API for more details.
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **item**: *dict* or *None* (optional)
                  the item with information on the function. The item contains the assigned item UID.
                  In case of error the item may be returned without modification. *None* will be
                  returned if request does not contain item parameters. In current implementation
                  the assigned *item_uid* is equal to *task_uid*, but it may change in the future.

              **task_uid**: *str* or *None*
                  task UID can be used to check status of the task and download results once the task
                  is completed (see *task_result* API). *None* is returned if the request fails.
------------  -----------------------------------------------------------------------------------------
Execution     The method initiates the operation. Monitor *task_results_uid* status field and call
              *task_result* API to check for success.
============  =========================================================================================


.. _method_task_status:

**'task_status'**
^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'task_status'**
------------  -----------------------------------------------------------------------------------------
Description   Returns the status of one or more tasks executed by the worker process. The request
              must contain one or more valid task UIDs, returned by one of APIs that starts tasks.
              A single UID may be passed as a string, multiple UIDs must be passed as as a list of
              strings. If a UID is passed as a string, then the returned status is also a string,
              if a list of one or more UIDs is passed, then the status is a dictionary that maps
              task UIDs and their status. The completed tasks are stored at the server at least
              for the period determined by retention time (currently 120 seconds after completion
              of the task). The expired results could be automatically deleted at any time and
              the method will return the task status as *'not_found'*.
------------  -----------------------------------------------------------------------------------------
Parameters    **task_uid**: *str* or *list(str)*
                  Task UID or a list of task UIDs.
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **task_uid**: *str*, *list(str)* or *None*
                  task UID or a list of task UIDs (expected to be the same as the input parameter).
                  May be *None* if the request fails.

              **status**: *str* or *dict*
                  status of the task(s) or *None* if the request (not task) failed. If **task_uid**
                  is a string representing single UID, then **status** is a string, which is one of
                  of *'running'*, *'completed'* or *'not_found'*. If **task_uid** is a list of strings,
                  then *'status'* is a dictionary that maps task UIDs to status of the respective tasks.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_task_result:

**'task_result'**
^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'task_result'**
------------  -----------------------------------------------------------------------------------------
Description   Get the status and results of task execution. The completed tasks are stored at
              the server at least for the period determined by retention time (currently 120 seconds
              after completion of the task). The expired results could be automatically deleted
              at any time and the method will return the task status as *'not_found'*.
------------  -----------------------------------------------------------------------------------------
Parameters    **task_uid**: *str*
                  Task UID.
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **task_uid**: *str* or *None*
                  task UID (expected to be the same as the input parameter). May be *None* if
                  the request fails.

              **status**: *'running'*, *'completed'*, *'not_found'* or *None*
                  status of the task or *None* if the request (not task) failed.

              **result**: *dict* or *None*
                  Dictionary containing the information on a running task, results of the completed
                  task or *None* if the request failed. The contents of the dictionary depends on the returned
                  *'status'*:

                  - **'running'** - Keys: *'task_uid'*, *'start_time'* and *'run_in_background'*.

                  - **'completed'** - Keys: *'task_uid'*, *'success'* (*True*/*False*),
                    *'msg'* (short error message, empty string if execution was successful),
                    *'traceback'* (full traceback in case of error, empty string otherwise),
                    *'return_value'* (value returned by the task, e.g. by the executed function,
                    *None* if the task failed), *'time_start'* and *'time_stop'*.

                  - **'not_found'** - Empty dictionary.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_lock:

**'lock'**
^^^^^^^^^^

============  =========================================================================================
Method        **'lock'**
------------  -----------------------------------------------------------------------------------------
Description   Lock RE Manager with the provided lock key to prevent other clients from modifying
              the environment, starting plans or tasks or editing the queue. The lock is not intended
              for access control. The read-only API are not affected by the lock, therefore all monitoring
              client applications are expected to remain functional after the lock is applied. The lock
              does not influence internal operation of the manager, e.g. the running queue will continue
              running and has to be explicitly stopped if needed.

              Each lockable API has an optional parameter **'lock_key'**. Passing a valid lock key
              (used to lock RE Manager) with the API requests allows to control RE Manager while it is
              locked. This supports the scenarios when a beamline scientists locks RE Manager with
              a unique code before entering the hutch to change samples or make adjustments and then
              safely runs a series of calibration or testing plans without interference from automated
              agents or remote users. A remote operators may still control locked RE Manager if
              the beamline scientists provides them with the lock key.

              The API parameters allow to choose between locking the **environment**, the **queue** or both.
              Locking the **environment** affects the following API:

              - :ref:`method_environment_open`
              - :ref:`method_environment_close`
              - :ref:`method_environment_destroy`
              - :ref:`method_queue_start`
              - :ref:`method_queue_stop`
              - :ref:`method_queue_stop_cancel`
              - :ref:`method_queue_item_execute`
              - :ref:`method_re_pause`
              - :ref:`'re_resume' <method_re_resume_stop_abort_halt>`
              - :ref:`'re_stop' <method_re_resume_stop_abort_halt>`
              - :ref:`'re_abort' <method_re_resume_stop_abort_halt>`
              - :ref:`'re_halt' <method_re_resume_stop_abort_halt>`
              - :ref:`method_script_upload`
              - :ref:`method_function_execute`

              Locking the **queue** affects the following API:

              - :ref:`method_queue_mode_set`
              - :ref:`method_queue_item_add`
              - :ref:`method_queue_item_add_batch`
              - :ref:`method_queue_item_update`
              - :ref:`method_queue_item_remove`
              - :ref:`method_queue_item_remove_batch`
              - :ref:`method_queue_item_move`
              - :ref:`method_queue_item_move_batch`
              - :ref:`method_queue_clear`
              - :ref:`method_history_clear`
              - :ref:`method_permissions_reload`
              - :ref:`method_permissions_set`

              The additional parameters include the name of the user (**user**, required) who locked
              RE Manager and the message to other users (**note**, optional) which may explain
              the reason why the manager is locked. The user name and the note is returned by
              **lock_info** API and included in the *'Invalid lock key'* error messages.

              An emergency lock key may be optionally set using
              ``QSERVER_EMERGENCY_LOCK_KEY_FOR_SERVER`` environment variable. The emergency key
              could be used to unlock RE Manager if the lock key is lost. The emergency lock key is
              accepted only by the **unlock** API.

              .. note::

                Restarting RE Manager does not change the lock status. The manager has to be unlocked
                using the valid lock key or the emergency lock key.
------------  -----------------------------------------------------------------------------------------
Parameters    **lock_key**: *str*
                  The lock key is an arbitrary non-empty string. Users/clients are expected to keep
                  the key used to lock RE Manager and use it to unlock the manager or make API requests.
                  If the lock key is lost by accident, then RE Manager may be unlocked using the
                  emergency lock key.

              **environment**: *boolean* (optional, default: *False*)
                  Enable lock for the API that control RE Worker environment. The request fails
                  if both **environment** and **queue** are missing or *False*.

              **queue**: *boolean* (optinal, default: *False*)
                  Enable lock for the API that control the queue. The request fails
                  if both **environment** and **queue** are missing or *False*.

              **user**: *str*
                  Name of the user who submits the request. The user name is returned as part of
                  *lock_info* and included in error messages.

              **note**: *str* or *None* (optional, default: *None*)
                  A text message to other users that explains the reason why RE Manager is locked.
                  The note is returned as part of *lock_info* and included in error messages.
                  If the value is *None*, then no message submitted.
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **lock_info**: *dict*
                  Dictionary containing the information on the status of the lock. The dictionary
                  is also returned by **lock_info** API and includes the following fields:

                  - **'environment'** (*boolean*) - indicates if the RE Worker environment is locked.

                  - **'queue'** (*boolean*) - indicates if the queue is locked.

                  - **'user'** (*str* or *None*) - the name of the user who locked RE Manager,
                    *None* if the lock is not set.

                  - **'note'** (*str* or *None*) - the text note left by the user who locked RE Manager,
                    *None* if the lock is not set.

                  - **'time'** (*float* or *None*) - timestamp (time when RE Manager was locked),
                    *None* if the lock is not set.

                  - **'time_str'** (*str*) - human-readable representation of the timestamp,
                    empty string if the lock is not set.

                  - **'emergency_lock_key_is_set'** (*boolean*) - indicates if the optional emergency
                    lock key is set.

              **lock_info_uid**: *str*
                  UID of *lock_info*. The UID is also returned in RE Manager status and could be
                  monitored to detect updates of *lock_info*.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_lock_info:

**'lock_info'**
^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'lock_info'**
------------  -----------------------------------------------------------------------------------------
Description   Load the lock status of RE Manager and optionally validate the lock key. Monitor
              *lock_info_uid* field of RE Manager status (see documentation for the :ref:`method_status`
              API) to detect changes in lock status and download the lock status only when the UID changes.
------------  -----------------------------------------------------------------------------------------
Parameters    **lock_key**: *str* or *None* (optional, default: *None*)
                  The parameter is used to validate the lock key (not the emergency lock key, see
                  the documentation for :ref:`method_lock` API). If the lock key is a string that matches
                  the key used to lock RE Manager, then the request succeeds. If no lock key is passed
                  with the request, the lock key is *None* or RE Manager is not locked, then no validation
                  is performed and the request always succeeds. The request fails if RE Manager is
                  locked and *lock_key* is not valid.
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully. The request succeeds if RE Manager
                  is unlocked or the *lock_key* parameter is not specified or *None*. If *lock_key* is
                  a string, then the request succeeds if the key is valid and fails otherwise.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **lock_info**: *dict*
                  Dictionary containing the information on the status of the lock. See the documentation
                  on :ref:`method_lock` API for the detailed description.

              **lock_info_uid**: *str*
                  UID of *lock_info*. The UID is also returned in RE Manager status and could be
                  monitored to detect updates of *lock_info*.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_unlock:

**'unlock'**
^^^^^^^^^^^^

============  =========================================================================================
Method        **'unlock'**
------------  -----------------------------------------------------------------------------------------
Description   Unlock RE Manager with the lock key used to lock the manager or the emergency lock
              key. See the documentation for :ref:`method_lock` API for more details.
------------  -----------------------------------------------------------------------------------------
Parameters    **lock_key**: *str*
                  The lock key must match the key used to lock RE Manager (with :ref:`method_lock` API)
                  or the emergency lock key.
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **lock_info**: *dict*
                  Dictionary containing the updated information on the lock status after the request
                  is processed. See the documentation on :ref:`method_lock` API for the detailed
                  description.

              **lock_info_uid**: *str*
                  UID of *lock_info*. The UID is also returned in RE Manager status and could be
                  monitored to detect updates of *lock_info*.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================



.. _method_manager_stop:

**'manager_stop'**
^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'manager_stop'**
------------  -----------------------------------------------------------------------------------------
Description   Exit RE Manager application. Clients will probably not need to initiate exit remotely,
              but ability to do so is extremely useful for automated testing.
------------  -----------------------------------------------------------------------------------------
Parameters    **option**: *'safe_on'* or *'safe_off'* (optional)
                  if the option of 'safe_on' is selected (default), then the request fails
                  unless 'manager_state' status field is 'idle' (no plans are running).
                  If the option is 'safe_off' then RE Worker environment is destroyed
                  (worker process is terminated and all data that was not saved is discarded).
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.
------------  -----------------------------------------------------------------------------------------
Execution     The request only initiates the operation of exiting RE Manager. If the request succeeds
              it may be expected that RE Manager application will eventually be exited and stops it
              stops responding to requests.
============  =========================================================================================


.. _method_manager_kill:

**'manager_kill'**
^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'manager_kill'**
------------  -----------------------------------------------------------------------------------------
Description   Freezes RE Manager process by stopping the asyncio event loop. By design, the manager
              process is expected to restart after 5 seconds of inactivity. The restart of the manager
              process should not affect the state of the queue or running plans. This function is
              implemented for testing purposes only. There is no practical reason for a client
              application to send this request.
------------  -----------------------------------------------------------------------------------------
Parameters    ---
------------  -----------------------------------------------------------------------------------------
Returns       RE Manager will not respond to the request.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================
