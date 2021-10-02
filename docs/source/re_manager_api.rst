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

Allowed plans and devices:

- :ref:`method_plans_allowed`
- :ref:`method_devices_allowed`
- :ref:`method_permissions_reload`

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
                  always returns 'RE Manager'

              **items_in_queue**: *int*
                 the number of items in the plan queue

              **items_in_history**: *int*
                 the number of items in the plan history

              **running_item_uid**: *str* or *None*
                 item UID of the currently running plan or *None* if no plan is currently running.

              **plan_queue_uid**: *str*
                 plan queue UID, which is updated each time the contents of the queue is changed.
                 Monitor this parameter to determine when the queue data should be downloaded.

              **plan_history_uid**: *str*
                 plan history UID, which is updated each time the contents of the history is changed.
                 Monitor this parameter to determine when the history data should be downloaded.

              **plans_allowed_uid**: *str*
                 UID for the list of allowed plans. UID is updated each time the contents of
                 the list is changed. Monitor the UID to detect changes in the list of allowed
                 plans and download the list from the server only when it is updated.

              **devices_allowed_uid**: *str*
                 UID for the list of allowed devices. Similar to **plans_allowed_uid**.

              **manager_state**: *str*
                  state of RE Manager. Supported states:

                  - **'initializing'** - RE Manager is initializing (the RE Manager is starting
                    or restarting)

                  - **'idle'** - RE Manager is idle and ready to execute requests. Many requests will fail
                    if RE Manager is not in the idle state.

                  - **'paused'** - a plan was paused and Run Engine is in the paused state.
                    The plan needs to be resumed, stopped, halted or aborted.

                  - **'creating_environment'** - RE Worker environment is in the process of being created.

                  - **'executing_queue'** - queue is being executed.

                  - **'closing_environment'** - RE Worker environment is in the process of being
                    closed (safe).

                  - **'destroying_environment'** - RE Worker environment is in the process of being
                    destroyed (emergency).

                  - **'re_state'** - state of Run Engine (see Bluesky documentation).

                  - **'run_list_uid'** - UID of the list of the active runs. Monitor this UID and
                    load the updated list of active runs once the UID is changed.

              **re_state**: *str* or *None*
                  current state of Bluesky Run Engine (see Blue Sky documentation) or *None* if
                  RE Worker environment does not exist (or closed).

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
                  the name of the user group (e.g. 'admin').
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **plans_allowed**: *dict*
                  the dictionary that contains information on the allowed plans.
                  Dictionary keys are plan names.

              **plans_allowed_uid**: *str*
                  UID of the list of allowed plans
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
                  the name of the user group (e.g. 'admin').
------------  -----------------------------------------------------------------------------------------
Returns       **success**: *boolean*
                  indicates if the request was processed successfully.

              **msg**: *str*
                  error message in case of failure, empty string ('') otherwise.

              **devices_allowed**: *dict*
                  the dictionary that contains information on the allowed devices.
                  Dictionary keys are device names.

              **devices_allowed_uid**: *str*
                  UID of the list of allowed devices
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


.. _method_permissions_reload:

**'permissions_reload'**
^^^^^^^^^^^^^^^^^^^^^^^^

============  =========================================================================================
Method        **'permissions_reload'**
------------  -----------------------------------------------------------------------------------------
Description   Reloads the list of allowed plans and devices and user group permission from
              the default location or the location set using command line parameters. Use this
              method to reload permissions if the files were changed on disk.
------------  -----------------------------------------------------------------------------------------
Parameters    ---
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
Parameters    ---
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
Parameters    ---
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
Parameters    ---
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
------------  -----------------------------------------------------------------------------------------
Parameters    ---
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
                  the name of the user group (e.g. 'admin').

              **user**: *str*
                  the name of the user (e.g. 'John Doe'). The name is included in the item metadata
                  and may be used to identify the user who added the item to the queue. It is not
                  passed to the Run Engine or included in run metadata.

              **pos**: *int*, *'front'* or *'back'* (optional)
                  position of the item in the queue. RE Manager will attempt to insert the item
                  at the specified position. The position may be positive or negative (counted
                  from the back of the queue) integer. If 'pos' value is a string 'front' or 'back',
                  then the item is inserted at the front or the back of the queue.

              **before_uid**, **after_uid**: *str* (optional)
                  insert the item before or after the item with the given item UID.

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
                  the name of the user group (e.g. 'admin').

              **user**: *str*
                  the name of the user (e.g. 'John Doe'). The name is included in the item metadata
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
                  the name of the user group (e.g. 'admin').

              **user**: *str*
                  the name of the user (e.g. 'John Doe'). The name is included in the item metadata
                  and may be used to identify the user who added the item to the queue. It is not
                  passed to the Run Engine or included in run metadata.

              **replace**: *boolean* (optional)
                  replace the updated item UID with the new random UID (True) or keep the original
                  UID (False). Default value is (False).
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

              Internally the API request adds the submitted item to the front of the queue
              and immediately attempts to start its execution. The item is removed from the queue
              almost immediately and never pushed back into the queue. If the item is a plan,
              the results of execution are added to plan history as usual. The respective history
              item could be accessed to check if the plan was executed successfully.

              The API **does not start execution of the queue**. Once execution of the submitted
              item is finished, RE Manager is switched to the IDLE state.
------------  -----------------------------------------------------------------------------------------
Parameters    **item**: *dict*
                  the dictionary of plan or instruction parameters. Plans are distinguished from
                  instructions based the value of the required parameter 'item_type'. Currently
                  supported item types are 'plan' and 'instruction'.

              **user_group**: *str*
                  the name of the user group (e.g. 'admin').

              **user**: *str*
                  the name of the user (e.g. 'John Doe'). The name is included in the item metadata
                  and may be used to identify the user who added the item to the queue. It is not
                  passed to the Run Engine or included in run metadata.
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
Parameters    ---
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
Parameters    ---
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
Parameters    ---
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
Parameters    ---
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
Parameters    **option**: *'immediate'* or *'deferred'*
                  pause the plan immediately (roll back to the previous checkpoint) or continue
                  to the next checkpoint.
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
