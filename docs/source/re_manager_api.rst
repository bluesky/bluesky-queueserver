======================
Run Engine Manager API
======================

============  =========================================================================================
Method        **''** (empty string)
------------  -----------------------------------------------------------------------------------------
Description   'Ping' request: causes RE Manager to send some response. Currently 'ping' request is
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

              **queue_stop_pending**: *boolean*
                  indicates if the request to stop the queue after completion of the current plan is pending.

              **worker_environment_exists**: *boolean*
                  indicates if RE Worker environment was created and plans could be executed.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


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

              **queue**: *list*
                  list of queue items

              **running_item**: *dict*
                  parameters of the item representing currently running plan, empty dictionary ({}) is
                  returned if no plan is currently running.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


============  =========================================================================================
Method        **'plans_allowed'**
------------  -----------------------------------------------------------------------------------------
Description   Returns a dictionary that contains information on the allowed plans for a given user
              group.
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
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


============  =========================================================================================
Method        **'devices_allowed'**
------------  -----------------------------------------------------------------------------------------
Description   Returns a dictionary that contains information on the allowed devices for a given user
              group.
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
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


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

              **history**: *list*
                  list of items in the plan history, each item is represented by a dictionary of
                  item parameters. Currently the plan history may contain only plans.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


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
              while operation is in process and change to 'idle' when the operation completes and
              'worker_environment_exists' is set True if environment was created successfully.
============  =========================================================================================


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
              while operation is in process and switch to 'idle' when the operation completes
              and 'worker_environment_exists' is set False if environment was closed successfully.
============  =========================================================================================


============  =========================================================================================
Method        **'environment_destroy'**
------------  -----------------------------------------------------------------------------------------
Description   Initiate the operation of destroying of (unresponsive) the existing RE Worker
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
              'destroying_environment' while operation is in process and switch to 'idle' when
              the operation completes and 'worker_environment_exists' is set False if environment
              was destroyed successfully.
============  =========================================================================================


============  =========================================================================================
Method        **'queue_item_add'**
------------  -----------------------------------------------------------------------------------------
Description   Add item to the queue. The item may be a plan or an instruction. By default the
              item is added to the back of the queue. Alternatively the item can be placed at
              the desired position in the queue or before or after one of the existing items.
------------  -----------------------------------------------------------------------------------------
Parameters    **plan or instruction**: *dict*
                  the dictionary of plan or instruction parameters. Plans are distinguished from
                  instructions based on whether 'plan' or 'instruction' parameter is included.

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
                  the operation is successful, *None* otherwise
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


============  =========================================================================================
Method        **'queue_item_get'**
------------  -----------------------------------------------------------------------------------------
Description   Read item from the queue. By default the item from the back of the queue is returned.
              Alternatively the item at the given position or the item with the given UID
              may be requested.
------------  -----------------------------------------------------------------------------------------
Parameters    **pos**: *int*, *'front'* or *'back'* (optional)
                  position of the item in the queue. RE Manager will attempt to insert the item
                  at the specified position. The position may be positive or negative (counted
                  from the back of the queue) integer. If 'pos' value is a string 'front' or 'back',
                  then the item is inserted at the front or the back of the queue.

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


============  =========================================================================================
Method        **'queue_item_remove'**
------------  -----------------------------------------------------------------------------------------
Description   Remove item from the queue. By default the last item in the queue is removed.
              Alternatively the position or UID of the item can be specified.
------------  -----------------------------------------------------------------------------------------
Parameters    **pos**: *int*, *'front'* or *'back'* (optional)
                  position of the item in the queue. RE Manager will attempt to insert the item
                  at the specified position. The position may be positive or negative (counted
                  from the back of the queue) integer. If 'pos' value is a string 'front' or 'back',
                  then the item is inserted at the front or the back of the queue.

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
                  the operation is successful, *None* otherwise
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


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
