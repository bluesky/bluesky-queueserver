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
