======================
Run Engine Manager API
======================

============  =========================================================================================
Method        '' (empty string)
------------  -----------------------------------------------------------------------------------------
Description   'Ping' request: causes RE Manager to send some response. Currently 'ping' request is
              equivalent to 'status' request. The functionality may be changed in the future.
              Use 'status' method to request status information from RE Manager.
------------  -----------------------------------------------------------------------------------------
Parameters    ---
------------  -----------------------------------------------------------------------------------------
Returns       See 'status' method for information on returned data.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================


============  =========================================================================================
Method        'status'
------------  -----------------------------------------------------------------------------------------
Description   Returns status of RE Manager.
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

              **queue_stop_pending**: *bool*
                  indicates if the request to stop the queue after completion of the current plan is pending.

              **worker_environment_exists**: *bool*
                  indicates if RE Worker environment was created and plans could be executed.
------------  -----------------------------------------------------------------------------------------
Execution     Immediate: no follow-up requests are required.
============  =========================================================================================

