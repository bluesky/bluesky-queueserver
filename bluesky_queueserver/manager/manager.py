import asyncio
import zmq
import zmq.asyncio
from multiprocessing import Process
import time as ttime
import pprint
import uuid
import enum

from .comms import PipeJsonRpcSendAsync, CommTimeoutError
from .profile_ops import load_allowed_plans_and_devices, validate_plan
from .plan_queue_ops import PlanQueueOperations


import logging

logger = logging.getLogger(__name__)


# TODO: this is incomplete set of states. Expect it to be expanded.
class MState(enum.Enum):
    INITIALIZING = "initializing"
    IDLE = "idle"
    PAUSED = "paused"  # Paused plan
    CREATING_ENVIRONMENT = "creating_environment"
    EXECUTING_QUEUE = "executing_queue"
    CLOSING_ENVIRONMENT = "closing_environment"
    DESTROYING_ENVIRONMENT = "destroying_environment"


class RunEngineManager(Process):
    """
    The class implementing Run Engine Worker thread.

    Parameters
    ----------
    conn_watchdog: multiprocessing.Connection
        One end of bidirectional (input/output) for communication to Watchdog process.
    conn_worker: multiprocessing.Connection
        One end of bidirectional (input/output) for communication to RE Worker process.
    args, kwargs
        `args` and `kwargs` of the `multiprocessing.Process`
    """

    def __init__(self, *args, conn_watchdog, conn_worker, config=None, **kwargs):

        if not conn_watchdog:
            raise RuntimeError(
                "Value of the parameter 'conn_watchdog' is invalid: %s.",
                str(conn_watchdog),
            )
        if not conn_worker:
            raise RuntimeError("Value of the parameter 'conn_worker' is invalid: %s.", str(conn_worker))

        super().__init__(*args, **kwargs)

        self._watchdog_conn = conn_watchdog
        self._worker_conn = conn_worker

        # The following attributes hold the state of the system
        self._manager_stopping = False  # Set True to exit manager (by _manager_stop_handler)
        self._environment_exists = False  # True if RE Worker environment exists
        self._manager_state = MState.INITIALIZING
        self._queue_stop_pending = False  # Queue is in the process of being stopped
        self._worker_state_info = None  # Copy of the last downloaded state of RE Worker

        self._loop = None

        # Communication with the server using ZMQ
        self._ctx = None
        self._zmq_socket = None
        self._ip_zmq_server = "tcp://*:5555"

        self._plan_queue = None  # Object of class plan_queue_ops.PlanQueueOperations

        self._heartbeat_generator_task = None  # Task for heartbeat generator
        self._worker_status_task = None  # Task for periodic checks of Worker status

        self._fut_manager_task_completed = None  # Used for multiple purposes

        # The objects of PipeJsonRpcSendAsync used for communciation with
        #   Watchdog and Worker modules. The object must be instantiated in the loop.
        self._comm_to_watchdog = None
        self._comm_to_worker = None

        # Data on a task executed in the background
        self._background_task = None  # asyncio.Task
        self._background_task_status = {"status": "success", "err_msg": ""}

        self._config = config or {}
        self._allowed_plans, self._allowed_devices = {}, {}

    async def _heartbeat_generator(self):
        """
        Heartbeat generator for Watchdog (indicates that the loop is running)
        """
        t_period = 0.5
        while True:
            await asyncio.sleep(t_period)
            await self._watchdog_send_heartbeat()

    # ======================================================================
    #          Functions that implement functionality of the server

    async def _execute_background_task(self, bckg_fut):
        """
        Monitors execution of a task in the background. Catches unhandled exceptions and
        maintains the status of the background task.

        Parameters
        ----------
        bckg_task: asyncio.Task
            The task scheduled to execute on the background

        Examples
        --------

        .. code-block:: python

            # Call in the loop
            asyncio.ensure_future(self._execute_background_task(some_coroutine()))
        """
        self._background_task_status["status"] = "running"
        self._background_task_status["err_msg"] = ""

        try:
            # 'self._background_task' may be used to cancel the task
            self._background_task = asyncio.ensure_future(bckg_fut)
            await self._background_task
            success, err_msg = self._background_task.result()
        except Exception as ex:
            success = False
            err_msg = f"Unhandled exception: {str(ex)}"
            logger.exception("Unhandled exception during background task execution: %s", str(ex))

        self._background_task_status["status"] = "success" if success else "failed"
        self._background_task_status["err_msg"] = err_msg

    async def _start_re_worker(self):
        """
        Initiate creation of RE Worker environment. The function does not wait until
        the environment is created. Returns True/False depending on whether
        the command is accepted.
        """
        if self._environment_exists:
            accepted = False
        else:
            accepted = True
            asyncio.ensure_future(self._execute_background_task(self._start_re_worker_task()))
        return accepted

    async def _start_re_worker_task(self):
        """
        Creates worker process.
        """
        if self._environment_exists:
            return False, "Rejected: environment already exists"

        self._fut_manager_task_completed = self._loop.create_future()
        self._manager_state = MState.CREATING_ENVIRONMENT

        try:
            success = await self._watchdog_start_re_worker()
            if not success:
                raise RuntimeError("Failed to create Worker process")
            logger.debug("Waiting for RE worker to start ...")
            success = await self._fut_manager_task_completed  # TODO: timeout may be needed here
            if success:
                self._environment_exists = True
                logger.debug("Worker started successfully.")
                success, err_msg = True, ""
            else:
                self._environment_exists = True
                await self._confirm_re_worker_exit()
                self._environment_exists = False
                logger.debug("Error occurred while opening RE Worker environment.")
                success, err_msg = False, "Error occurred while opening RE Worker environment."

        except Exception as ex:
            logger.exception("Failed to start_Worker: %s", str(ex))
            success, err_msg = False, f"Failed to start_Worker {str(ex)}"

        self._manager_state = MState.IDLE
        return success, err_msg

    async def _stop_re_worker(self):
        """
        Initiate closing of RE Worker environment.
        """
        if not self._environment_exists:
            accepted = False
            err_msg = "RE Worker environment does not exist."
        elif self._manager_state != MState.IDLE:
            accepted = False
            err_msg = "A plan is currently running"
        else:
            accepted = True
            err_msg = ""
            asyncio.ensure_future(self._execute_background_task(self._stop_re_worker_task()))
        return accepted, err_msg

    async def _stop_re_worker_task(self):
        """
        Closing of the RE Worker.
        """
        if not self._environment_exists:
            return False, "Environment does not exists"

        if self._manager_state != MState.IDLE:
            return False, "A plan is currently running"

        self._fut_manager_task_completed = self._loop.create_future()

        success, err_msg = await self._worker_command_close_env()
        if success:
            # Wait for RE Worker to be prepared to close
            self._event_worker_closed = asyncio.Event()

            self._manager_state = MState.CLOSING_ENVIRONMENT
            await self._fut_manager_task_completed  # TODO: timeout may be needed here
            self._manager_state = MState.IDLE

            if not await self._confirm_re_worker_exit():
                success = False
                err_msg = "Failed to confirm closing of RE Worker thread"

        return success, err_msg

    async def _kill_re_worker(self):
        """
        Kill the process in which RE worker is running.
        """
        if self._environment_exists:
            accepted = True
            err_msg = ""
            asyncio.ensure_future(self._execute_background_task(self._kill_re_worker_task()))
        else:
            accepted = False
            err_msg = "RE environment does not exist"
        return accepted, err_msg

    async def _kill_re_worker_task(self):
        """
        Stop RE Worker. Returns the result as "success", "rejected" or "failed"
        """
        success = False
        self._manager_state = MState.DESTROYING_ENVIRONMENT
        await self._watchdog_kill_re_worker()
        # Wait for at most 10 seconds. Consider the environment destroyed after this.
        #   This should never fail unless there is a bug in the Manager or Watchdog,
        #   since killing process can be done in any state of the Worker.
        # TODO: think about handling timeout errors.
        for n in range(10):
            await asyncio.sleep(1)
            if not await self._watchdog_is_worker_alive():
                success = True
                break

        self._manager_state = MState.IDLE
        self._environment_exists = False
        # If a plan is running, it needs to be pushed back into the queue
        await self._plan_queue.set_processed_plan_as_stopped(exit_status="environment_destroyed")

        err_msg = "" if success else "Failed to properly destroy RE Worker environment."
        logger.info("RE Worker environment is destroyed")
        if not success:

            logger.error(err_msg)
        return success, err_msg

    async def _confirm_re_worker_exit(self):
        """
        Confirm RE worker exit and make sure the worker thread exits.
        """
        success = True
        if self._environment_exists:
            logger.debug("Waiting for exit confirmation from RE worker ...")
            success, err_msg = await self._worker_command_confirm_exit()

            # Environment is not in valid state anyway. So assume it does not exist.
            self._environment_exists = False
            if success:
                logger.debug("Wait for RE Worker process to close (join)")

                if not await self._watchdog_join_re_worker(timeout_join=0.5):
                    success, err_msg = False, "Failed to join RE Worker thread."
                    # TODO: this error should probably be handled differently than this,
                    #   since it may indicate that the worker process is stalled.
                    logger.error(
                        "Failed to properly join the worker process. The process may not be properly closed."
                    )
            else:
                success, err_msg = False, "RE Worker failed to exit (no confirmation)"
        else:
            success, err_msg = False, "RE Worker environment does not exist"
        return success, err_msg

    async def _is_worker_alive(self):
        return await self._watchdog_is_worker_alive()

    async def _periodic_worker_state_request(self):
        """
        Periodically update locally stored RE Worker status
        """
        t_period = 0.5
        while True:
            await asyncio.sleep(t_period)
            if self._environment_exists or (self._manager_state == MState.CREATING_ENVIRONMENT):

                if self._manager_state == MState.DESTROYING_ENVIRONMENT:
                    continue

                ws, _ = await self._worker_request_state()
                if ws is not None:
                    self._worker_state_info = ws
                    if self._manager_state == MState.CLOSING_ENVIRONMENT:
                        if ws["environment_state"] == "closing":
                            self._fut_manager_task_completed.set_result(None)

                    elif self._manager_state == MState.CREATING_ENVIRONMENT:
                        # If RE Worker environment fails to open, then it switchins to 'closing' status.
                        #   Closing must be confirmed by Manager before it is closed.
                        if ws["environment_state"] in "ready":
                            self._fut_manager_task_completed.set_result(True)
                        if ws["environment_state"] == "closing":
                            self._fut_manager_task_completed.set_result(False)

                    elif self._manager_state == MState.EXECUTING_QUEUE:
                        if ws["re_report_available"]:
                            self._loop.create_task(self._process_plan_report())

    async def _process_plan_report(self):
        """
        Process plan report. Called when plan report is available.
        """
        # TODO: `set_processed_plan_as_stopped` needs more precise exit status
        #   current values are not final selection, they are just temporarily for the demo
        # Read report first
        plan_report, err_msg = await self._worker_request_plan_report()
        if plan_report is None:
            # TODO: this would typically mean a bug (communciation error). Probably more
            #       complicated processing is needed
            logger.error(f"Failed to download plan report: {err_msg}. Stopping queue processing.")
            await self._plan_queue.set_processed_plan_as_stopped(exit_status="manager_error")
            self._manager_state = MState.IDLE
        else:
            plan_state = plan_report["plan_state"]
            success = plan_report["success"]
            result = plan_report["result"]
            err_msg = plan_report["err_msg"]

            msg_display = result if result else err_msg
            logger.info(
                "Report received from RE Worker:\nplan_state=%s\nsuccess=%s\n%s\n)",
                plan_state,
                str(success),
                str(msg_display),
            )

            if plan_state == "completed":
                # Executed plan is removed from the queue only after it is successfully completed.
                # If a plan was not completed or not successful (exception was raised), then
                # execution of the queue is stopped. It can be restarted later (failed or
                # interrupted plan will still be in the queue.
                await self._plan_queue.set_processed_plan_as_completed(exit_status=plan_state)
                await self._start_plan_task()
            elif plan_state in ("stopped", "error"):
                # Paused plan was stopped/aborted/halted
                await self._plan_queue.set_processed_plan_as_stopped(exit_status=plan_state)
                self._manager_state = MState.IDLE
            elif plan_state == "paused":
                # The plan was paused (nothing should be done).
                self._manager_state = MState.PAUSED
            else:
                logger.error("Unknown plan state %s was returned by RE Worker.", plan_state)

        if self._manager_state == MState.IDLE:
            # No plans are running: deactivate the stop sequence.
            self._queue_stop_deactivate()

    async def _start_plan(self):
        """
        Initiate creation of RE Worker environment. The function does not wait until
        the environment is created. Returns True/False depending on whether
        the command is accepted.
        """
        if not self._environment_exists:
            success, err_msg = False, "RE Worker environment does not exist."
        elif not self._manager_state == MState.IDLE:
            success, err_msg = False, "RE Manager is busy."
        else:
            self._queue_stop_deactivate()  # Just in case
            asyncio.ensure_future(self._execute_background_task(self._start_plan_task()))
            success, err_msg = True, ""
        return success, err_msg

    async def _start_plan_task(self):
        """
        Upload the plan to the worker process for execution.
        Plan in the queue is represented as a dictionary with the keys "name" (plan name),
        "args" (list of args), "kwargs" (list of kwargs). Only the plan name is mandatory.
        Names of plans and devices are strings.
        """
        n_pending_plans = await self._plan_queue.get_plan_queue_size()
        logger.info("Starting a new plan: %d plans are left in the queue", n_pending_plans)

        if not n_pending_plans:
            self._manager_state = MState.IDLE
            success, err_msg = False, "Queue is empty."
            logger.info(err_msg)

        elif self._queue_stop_pending:
            self._manager_state = MState.IDLE
            success, err_msg = False, "Queue is stopped."
            logger.info(err_msg)

        else:
            # Reset RE environment (worker)
            success, err_msg = await self._worker_command_reset_worker()
            if not success:
                self._manager_state = MState.IDLE
                err_msg = f"Failed to reset RE Worker: {err_msg}"
                logger.error(err_msg)
                return success, err_msg

            self._manager_state = MState.EXECUTING_QUEUE

            new_plan = await self._plan_queue.set_next_plan_as_running()

            plan_name = new_plan["name"]
            args = new_plan["args"] if "args" in new_plan else []
            kwargs = new_plan["kwargs"] if "kwargs" in new_plan else {}
            plan_uid = new_plan["plan_uid"]

            plan_info = {
                "name": plan_name,
                "args": args,
                "kwargs": kwargs,
                "plan_uid": plan_uid,
            }

            success, err_msg = await self._worker_command_run_plan(plan_info)
            if not success:
                self._manager_state = MState.IDLE
                logger.error(
                    "Failed to start the plan %s.\nError: %s",
                    pprint.pformat(plan_info),
                    err_msg,
                )
                err_msg = f"Failed to start the plan: {err_msg}"

        if self._manager_state == MState.IDLE:
            # No plans are running: deactivate the stop sequence.
            self._queue_stop_deactivate()

        return success, err_msg

    def _queue_stop_activate(self):
        if self._manager_state != MState.EXECUTING_QUEUE:
            msg = f"Failed to pause the queue. Queue is not running. Manager state: {self._manager_state}"
            return False, msg
        else:
            self._queue_stop_pending = True
            return True, ""

    def _queue_stop_deactivate(self):
        self._queue_stop_pending = False
        return True, ""

    async def _pause_run_engine(self, option):
        """
        Pause execution of a running plan. Run Engine must be in 'running' state in order for
        the request to pause to be accepted by RE Worker.
        """
        success, err_msg = await self._worker_command_pause_plan(option)
        if not success:
            logger.error("Failed to pause the running plan: %s", err_msg)
        else:
            self._manager_state = MState.EXECUTING_QUEUE
        return success, err_msg

    async def _continue_run_engine(self, *, option):
        """
        Continue handling of a paused plan
        """

        available_options = ("resume", "abort", "stop", "halt")
        if self._manager_state != MState.PAUSED:
            success = False
            err_msg = f"RE Manager is not paused: current state is '{self._manager_state.value}'"

        elif option not in available_options:
            # This function is called only within the class: allow exception to be raised.
            #   It should make the unit tests fail.
            raise ValueError(f"Option '{option}' is not supported. " f"Available options: {available_options}")

        elif self._environment_exists:
            success, err_msg = await self._worker_command_continue_plan(option)
            if success:
                self._manager_state = MState.EXECUTING_QUEUE
            else:
                logger.error("Failed to pause the running plan: %s", err_msg)

        else:
            success, err_msg = (
                False,
                "Environment does not exist. Can not pause Run Engine.",
            )

        return {"success": success, "msg": err_msg}

    # ===============================================================================
    #         Functions that send commands/request data from Worker process

    async def _worker_request_state(self):
        try:
            re_state = await self._comm_to_worker.send_msg("request_state")
            err_msg = ""
        except CommTimeoutError:
            re_state, err_msg = None, "Timeout occurred"
        return re_state, err_msg

    async def _worker_request_plan_report(self):
        try:
            plan_report = await self._comm_to_worker.send_msg("request_plan_report")
            err_msg = ""
            if plan_report is None:
                err_msg = "Report is not available at RE Worker"
        except CommTimeoutError:
            plan_report, err_msg = None, "Timeout occurred"
        return plan_report, err_msg

    async def _worker_command_close_env(self):
        try:
            response = await self._comm_to_worker.send_msg("command_close_env")
            success = response["status"] == "accepted"
            err_msg = response["err_msg"]
        except CommTimeoutError:
            success, err_msg = None, "Timeout occurred"
        return success, err_msg

    async def _worker_command_confirm_exit(self):
        try:
            response = await self._comm_to_worker.send_msg("command_confirm_exit")
            success = response["status"] == "accepted"
            err_msg = response["err_msg"]
        except CommTimeoutError:
            success, err_msg = None, "Timeout occurred"
        return success, err_msg

    async def _worker_command_run_plan(self, plan_info):
        try:
            response = await self._comm_to_worker.send_msg("command_run_plan", {"plan_info": plan_info})
            success = response["status"] == "accepted"
            err_msg = response["err_msg"]
        except CommTimeoutError:
            success, err_msg = None, "Timeout occurred"
        return success, err_msg

    async def _worker_command_pause_plan(self, option):
        try:
            response = await self._comm_to_worker.send_msg("command_pause_plan", {"option": option})
            success = response["status"] == "accepted"
            err_msg = response["err_msg"]
        except CommTimeoutError:
            success, err_msg = None, "Timeout occurred"
        return success, err_msg

    async def _worker_command_continue_plan(self, option):
        try:
            response = await self._comm_to_worker.send_msg("command_continue_plan", {"option": option})
            success = response["status"] == "accepted"
            err_msg = response["err_msg"]
        except CommTimeoutError:
            success, err_msg = None, "Timeout occurred"
        return success, err_msg

    async def _worker_command_reset_worker(self):
        try:
            response = await self._comm_to_worker.send_msg("command_reset_worker")
            success = response["status"] == "accepted"
            err_msg = response["err_msg"]
        except CommTimeoutError:
            success, err_msg = None, "Timeout occurred"
        return success, err_msg

    # ===============================================================================
    #         Functions that send commands/request data from Watchdog process

    async def _watchdog_start_re_worker(self):
        """
        Initiate the startup of the RE Worker. Returned 'success==True' means that the process
        was created successfully and RE environment initialization is started.
        """
        try:
            response = await self._comm_to_watchdog.send_msg("start_re_worker")
            success = response["success"]
        except CommTimeoutError:
            success = False
        # TODO: add processing of CommJsonRpcError and RuntimeError to all handlers !!!
        return success

    async def _watchdog_join_re_worker(self, timeout_join=0.5):
        """
        Request Watchdog to join RE Worker process. The sequence of orderly closing of the process
        needs to be initiated before attempting to join the process.
        """
        try:
            # Communication timeout must be a little bit larger than join timeout.
            response = await self._comm_to_watchdog.send_msg(
                "join_re_worker", {"timeout": timeout_join}, timeout=timeout_join + 0.1
            )
            success = response["success"]
        except CommTimeoutError:
            success = False
        return success

    async def _watchdog_kill_re_worker(self):
        """
        Request Watchdog to kill RE Worker process (justified only if the process is not responsive).
        """
        try:
            response = await self._comm_to_watchdog.send_msg("kill_re_worker")
            success = response["success"]
        except CommTimeoutError:
            success = False
        return success

    async def _watchdog_is_worker_alive(self):
        """
        Check if RE Worker process is alive.
        """
        try:
            response = await self._comm_to_watchdog.send_msg("is_worker_alive")
            worker_alive = response["worker_alive"]
        except asyncio.TimeoutError:
            worker_alive = False
        return worker_alive

    async def _watchdog_manager_stopping(self):
        """
        Inform Watchdog process that the manager is intentionally being stopped and
        it should not be restarted.
        """
        await self._comm_to_watchdog.send_msg("manager_stopping", notification=True)

    async def _watchdog_send_heartbeat(self):
        """
        Send (periodic) heartbeat signal to Watchdog.
        """
        await self._comm_to_watchdog.send_msg("heartbeat", {"value": "alive"}, notification=True)

    # =========================================================================
    #                        ZMQ message handlers

    async def _ping_handler(self, request):
        """
        May be called to get some response from the Manager. Returns status of the manager.
        """
        return await self._status_handler(request)

    async def _status_handler(self, request):
        """
        Returns status of the manager.
        """
        logger.info("Processing 'status' request.")

        # Computed/retrieved data
        n_pending_plans = await self._plan_queue.get_plan_queue_size()
        running_plan_info = await self._plan_queue.get_running_plan_info()
        n_plans_in_history = await self._plan_queue.get_plan_history_size()

        # Prepared output data
        plans_in_queue = n_pending_plans
        plans_in_history = n_plans_in_history
        running_plan_uid = running_plan_info["plan_uid"] if running_plan_info else None
        manager_state = self._manager_state.value
        queue_stop_pending = self._queue_stop_pending
        worker_environment_exists = self._environment_exists
        # worker_state_info = self._worker_state_info

        # TODO: consider different levels of verbosity for ping or other command to
        #       retrieve detailed status.
        msg = {
            "msg": "RE Manager",
            "plans_in_queue": plans_in_queue,
            "plans_in_history": plans_in_history,
            "running_plan_uid": running_plan_uid,
            "manager_state": manager_state,
            "queue_stop_pending": queue_stop_pending,
            "worker_environment_exists": worker_environment_exists,
            # "worker_state_info": worker_state_info
        }
        return msg

    async def _plans_allowed_handler(self, request):
        """
        Returns the list of allowed plans.
        """
        logger.info("Returning the list of allowed plans.")

        try:
            if "user_group" not in request:
                raise Exception("Incorrect request format: user group is not specified")

            user_group = request["user_group"]

            if user_group not in self._allowed_plans:
                raise Exception(f"Unknown user group: '{user_group}'")

            plans_allowed = self._allowed_plans[user_group]
            success, msg = True, ""
        except Exception as ex:
            plans_allowed = {}
            success, msg = False, str(ex)

        return {
            "success": success,
            "msg": msg,
            "plans_allowed": plans_allowed,
        }

    async def _devices_allowed_handler(self, request):
        """
        Returns the list of allowed devices.
        """
        logger.info("Returning the list of allowed devices.")

        try:
            if "user_group" not in request:
                raise Exception("Incorrect request format: user group is not specified")

            user_group = request["user_group"]

            if user_group not in self._allowed_devices:
                raise Exception(f"Unknown user group: '{user_group}'")

            devices_allowed = self._allowed_devices[user_group]
            success, msg = True, ""
        except Exception as ex:
            devices_allowed = {}
            success, msg = False, str(ex)

        return {
            "success": success,
            "msg": msg,
            "devices_allowed": devices_allowed,
        }

    async def _queue_get_handler(self, request):
        """
        Returns the contents of the current queue.
        """
        logger.info("Returning current queue and running plan.")
        plan_queue = await self._plan_queue.get_plan_queue()
        running_plan = await self._plan_queue.get_running_plan_info()

        return {"queue": plan_queue, "running_plan": running_plan}

    async def _queue_plan_add_handler(self, request):
        """
        Adds new plan to the end of the queue. Required parameter ``plan`` must
        contains a dictionary with plan parameters. The plan parameters may not include
        UID, because the function always overwrites plan UID. If a plan already contains
        UID, then it is replaced with the new one. The returned plan contains new UID
        even if the function failed to add the plan to the queue.

        Optional key ``pos`` may be a string (choices "front", "back") or integer (positive
        or negative) that specifies the desired position in the queue. The default value
        is "back" (element is pushed to the back of the queue). If ``pos`` is integer and
        it is outside the range of available elements, then the new element pushed to
        the front or the back of the queue depending on the value of the index.

        It is recommended to use negative indices (counted from the back of the queue)
        when modifying a running queue.
        """
        logger.info("Adding new plan to the queue: %s", pprint.pformat(request))
        try:
            plan, qsize, msg = {}, None, ""
            if "plan" not in request:
                raise Exception("Incorrect request format: no plan is specified")

            if "user_group" not in request:
                raise Exception("Incorrect request format: user group is not specified")

            if "user" not in request:
                raise Exception("Incorrect request format: user name is not specified")

            user = request["user"]
            user_group = request["user_group"]

            if user_group not in self._allowed_plans:
                raise Exception(f"Unknown user group: '{user_group}'")

            plan = request["plan"]
            pos = request.get("pos", "back")  # Position is optional

            allowed_plans = self._allowed_plans[user_group] if self._allowed_plans else self._allowed_plans
            allowed_devices = self._allowed_devices[user_group] if self._allowed_devices else self._allowed_devices
            success, msg = validate_plan(plan, allowed_plans=allowed_plans, allowed_devices=allowed_devices)
            if not success:
                raise Exception(msg)

            # Create Plan UID (used internally by QServer, user is not expected to see it)
            # Note, Plan UID is not related to Scan UID generated by Run Engine
            plan["plan_uid"] = str(uuid.uuid4())

            # Add user name and user_id to the plan (for later reference)
            plan["user"] = user
            plan["user_group"] = user_group

            # Adding plan to queue may raise an exception
            plan, qsize = await self._plan_queue.add_plan_to_queue(plan, pos=pos)
            success = True

        except Exception as ex:
            success = False
            msg = f"Failed to add a plan: {str(ex)}"

        return {"success": success, "msg": msg, "plan": plan, "qsize": qsize}

    async def _queue_plan_get_handler(self, request):
        """
        Returns an item from the queue. The position of the item
        may be specified as an index (positive or negative) or a string
        from the set {``front``, ``back``}. The default option is ``back``
        """
        logger.info("Getting an item from the queue.")
        try:
            plan, msg = {}, ""
            pos = request.get("pos", "back")
            plan = await self._plan_queue.get_plan(pos=pos)
            success = True
        except Exception as ex:
            success = False
            msg = f"Failed to get a plan: {str(ex)}"

        return {"success": success, "msg": msg, "plan": plan}

    async def _queue_plan_remove_handler(self, request):
        """
        Removes (pops) item from the queue. The position of the item
        may be specified as an index (positive or negative) or a string
        from the set {``front``, ``back``}. The default option is ``back``
        """
        logger.info("Removing item from the queue.")
        try:
            plan, qsize, msg = {}, None, ""
            pos = request.get("pos", "back")
            plan, qsize = await self._plan_queue.pop_plan_from_queue(pos=pos)
            success = True
        except Exception as ex:
            success = False
            msg = f"Failed to remove a plan: {str(ex)}"

        return {"success": success, "msg": msg, "plan": plan, "qsize": qsize}

    async def _queue_clear_handler(self, request):
        """
        Remove all entries from the plan queue (does not affect currently executed run)
        """
        logger.info("Clearing the queue")
        await self._plan_queue.clear_plan_queue()
        return {"success": True, "msg": "Plan queue is now empty."}

    async def _history_get_handler(self, request):
        """
        Returns the contents of the plan history.
        """
        logger.info("Returning plan history.")
        plan_history = await self._plan_queue.get_plan_history()

        return {"history": plan_history}

    async def _history_clear_handler(self, request):
        """
        Remove all entries from the plan history
        """
        logger.info("Clearing the plan execution history")
        await self._plan_queue.clear_plan_history()
        return {"success": True, "msg": "Plan history is now empty."}

    async def _environment_open_handler(self, request):
        """
        Creates RE environment: creates RE Worker process, starts and configures Run Engine.
        """
        logger.info("Opening the new RE environment ...")
        success = await self._start_re_worker()
        msg = "Environment already exists." if not success else ""
        return {"success": success, "msg": msg}

    async def _environment_close_handler(self, request):
        """
        Orderly closes of RE environment. The command returns success only if no plan is running,
        i.e. RE Manager is in the idle state. The command is rejected if a plan is running.
        """
        logger.info("Closing existing RE environment ...")
        success, err_msg = await self._stop_re_worker()
        return {"success": success, "msg": err_msg}

    async def _environment_destroy_handler(self, request):
        """
        Destroys RE environment by killing RE Worker process. This is a last resort command which
        should be made available only to expert level users.
        """
        logger.info("Destroying current RE environment ...")
        success, err_msg = await self._kill_re_worker()
        return {"success": success, "msg": err_msg}

    async def _queue_start_handler(self, request):
        """
        Start execution of the loaded queue. Additional runs can be added to the queue while
        it is executed. If the queue is empty, then nothing will happen.
        """
        logger.info("Starting queue processing.")
        success, msg = await self._start_plan()
        return {"success": success, "msg": msg}

    async def _queue_stop_handler(self, request):
        """
        Stop execution of the running queue. Currently running plan will be completed
        and the next plan will not be started. Stopping the queue is a safe operation
        that should not lead to data loss.

        The request will fail if the queue is not running.
        """
        logger.info("Activating 'stop queue' sequence ...")
        success, msg = self._queue_stop_activate()
        return {"success": success, "msg": msg}

    async def _queue_stop_cancel_handler(self, request):
        """
        Deactivate the sequence of stopping the queue execution.

        The request always succeeds.
        """
        logger.info("Deactivating 'stop queue' sequence ...")
        success, msg = self._queue_stop_deactivate()
        return {"success": success, "msg": msg}

    async def _re_pause_handler(self, request):
        """
        Pause Run Engine
        """
        logger.info("Pausing the queue (currently running plan).")
        option = request["option"] if "option" in request else None
        available_options = ("deferred", "immediate")
        if option in available_options:
            if self._environment_exists:
                success, msg = await self._pause_run_engine(option)
            else:
                success, msg = (
                    False,
                    "Environment does not exist. Can not pause Run Engine.",
                )
        else:
            success, msg = (
                False,
                f"Option '{option}' is not supported. Available options: {available_options}",
            )
        return {"success": success, "msg": msg}

    async def _re_resume_handler(self, request):
        """
        Run Engine: resume execution of a paused plan
        """
        logger.info("Resuming paused plan ...")
        return await self._continue_run_engine(option="resume")

    async def _re_stop_handler(self, request):
        """
        Run Engine: stop execution of a paused plan
        """
        logger.info("Stopping paused plan ...")
        return await self._continue_run_engine(option="stop")

    async def _re_abort_handler(self, request):
        """
        Run Engine: abort execution of a paused plan
        """
        logger.info("Aborting paused plan ...")
        return await self._continue_run_engine(option="abort")

    async def _re_halt_handler(self, request):
        """
        Run Engine: halt execution of a paused plan
        """
        logger.info("Halting paused plan ...")
        return await self._continue_run_engine(option="halt")

    async def _manager_stop_handler(self, request):
        """
        Stop RE Manager in orderly way. The method may be called with option
        ``safe_off`` and ``safe_on``. If no option is provided, then the default
        option ``safe_on`` is used.

        If called with ``safe_on``, then the manager is closed only if it is in 'idle'
        state, i.e. no plan is currently running or paused. If called with ``safe_off`,
        then the closing sequence is initiated immediately. RE Worker will be terminated
        and running plan will be not be finished.

        This command should not be exposed to the users, but it may be valuable for
        testing.
        """
        allowed_options = ("safe_off", "safe_on")
        option = "safe_on"
        success = True

        if "option" in request:
            r_option = request["option"]
            if r_option:
                if r_option not in allowed_options:
                    success = False
                    msg = f"Option '{r_option}' is not allowed. Allowed options: {allowed_options}"
                else:
                    option = r_option

        if success and (option == "safe_on") and (self._manager_state != MState.IDLE):
            success = False
            msg = (
                f"Closing RE Manager with option '{option}' is allowed "
                f"only in 'idle' state. Current state: '{self._manager_state.value}'"
            )

        if success:
            msg = f"Initiating stopping of RE Manager with option '{option}'."
            self._manager_stopping = True

        return {"success": success, "msg": msg}

    async def _manager_kill_handler(self, request):
        """
        Testing API: blocks event loop of RE Manager process forever and
        causes Watchdog process to restart RE Manager.
        """
        # This is expected to block the event loop forever.
        while True:
            ttime.sleep(10)

    async def _zmq_execute(self, msg):
        method = msg["method"]
        params = msg["params"]
        handler_dict = {
            "": "_ping_handler",
            "status": "_status_handler",
            "queue_get": "_queue_get_handler",
            "plans_allowed": "_plans_allowed_handler",
            "devices_allowed": "_devices_allowed_handler",
            "history_get": "_history_get_handler",
            "history_clear": "_history_clear_handler",
            "environment_open": "_environment_open_handler",
            "environment_close": "_environment_close_handler",
            "environment_destroy": "_environment_destroy_handler",
            "queue_plan_add": "_queue_plan_add_handler",
            "queue_plan_get": "_queue_plan_get_handler",
            "queue_plan_remove": "_queue_plan_remove_handler",
            "queue_clear": "_queue_clear_handler",
            "queue_start": "_queue_start_handler",
            "queue_stop": "_queue_stop_handler",
            "queue_stop_cancel": "_queue_stop_cancel_handler",
            "re_pause": "_re_pause_handler",
            "re_resume": "_re_resume_handler",
            "re_stop": "_re_stop_handler",
            "re_abort": "_re_abort_handler",
            "re_halt": "_re_halt_handler",
            "manager_stop": "_manager_stop_handler",
            "manager_kill": "_manager_kill_handler",
        }

        try:
            handler_name = handler_dict[method]
            handler = getattr(self, handler_name)
            result = await handler(params)
        except KeyError:
            result = {"success": False, "msg": f"Unknown method '{method}'"}
        except AttributeError:
            result = {
                "success": False,
                "msg": f"Handler for the command '{method}' is not implemented",
            }
        return result

    # ======================================================================
    #          Functions that support communication via 0MQ

    async def _zmq_receive(self):
        msg_in = await self._zmq_socket.recv_json()
        return msg_in

    async def _zmq_send(self, msg):
        await self._zmq_socket.send_json(msg)

    async def zmq_server_comm(self):
        """
        This function is supposed to be executed by asyncio.run() to start the manager.
        """
        self._ctx = zmq.asyncio.Context()

        self._loop = asyncio.get_running_loop()

        self._comm_to_watchdog = PipeJsonRpcSendAsync(conn=self._watchdog_conn, name="RE Manager-Watchdog Comm")
        self._comm_to_worker = PipeJsonRpcSendAsync(conn=self._worker_conn, name="RE Manager-Worker Comm")
        self._comm_to_watchdog.start()
        self._comm_to_worker.start()

        # Start heartbeat generator
        self._heartbeat_generator_task = asyncio.ensure_future(self._heartbeat_generator(), loop=self._loop)
        self._worker_status_task = asyncio.ensure_future(self._periodic_worker_state_request(), loop=self._loop)

        self._plan_queue = PlanQueueOperations()
        await self._plan_queue.start()

        # Delete Redis entries (for testing and debugging)
        # self._plan_queue.delete_pool_entries()

        # Load lists of allowed plans and devices
        logger.info("Loading the lists of allowed plans and devices ...")
        path_pd = self._config["existing_plans_and_devices_path"]
        path_ug = self._config["user_group_permissions_path"]
        try:
            self._allowed_plans, self._allowed_devices = load_allowed_plans_and_devices(
                path_existing_plans_and_devices=path_pd, path_user_group_permissions=path_ug
            )
        except Exception as ex:
            logger.exception(
                "Error occurred while loading lists of allowed plans and devices from '%s': %s",
                path_pd,
                str(ex),
            )

        # Set the environment state based on whether the worker process is alive (request Watchdog)
        self._environment_exists = await self._is_worker_alive()

        # Now check if the plan is still being executed (if it was executed)
        if self._environment_exists:
            self._worker_state_info, err_msg = await self._worker_request_state()
            if self._worker_state_info:
                plan_uid_running = self._worker_state_info["running_plan_uid"]
                re_state = self._worker_state_info["re_state"]
                if plan_uid_running:
                    # Plan is running. Check if it is the same plan as in redis.
                    plan_stored = await self._plan_queue.get_running_plan_info()
                    if "plan_uid" in plan_stored:
                        plan_uid_stored = plan_stored["plan_uid"]
                        if re_state in ("paused", "pausing"):
                            self._manager_state = MState.PAUSED  # Paused and can be resumed
                        else:
                            self._manager_state = MState.EXECUTING_QUEUE  # Wait for plan completion
                        if plan_uid_stored != plan_uid_running:
                            # Guess is that the environment may still work, so restart is
                            #   only recommended if it is convenient.
                            logger.warning(
                                "Inconsistency of internal QServer data was detected: \n"
                                "UID of currently running plan is '%s', "
                                "instead of '%s'.\n"
                                "RE execution environment may need to be closed and created again \n"
                                "to restore data integrity.",
                                plan_uid_running,
                                plan_uid_stored,
                            )
            else:
                logger.error("Error while reading RE Worker status: %s", err_msg)
        if not self._manager_state == MState.EXECUTING_QUEUE:
            # TODO: there is no 'unknown' status. This is here temporarily. Different logic
            #   has to be applied here.
            await self._plan_queue.set_processed_plan_as_completed(exit_status="unknown")

        logger.info("Starting ZeroMQ server")
        self._zmq_socket = self._ctx.socket(zmq.REP)
        self._zmq_socket.bind(self._ip_zmq_server)
        logger.info("ZeroMQ server is waiting on %s", str(self._ip_zmq_server))

        if self._manager_state == MState.INITIALIZING:
            self._manager_state = MState.IDLE

        while True:
            #  Wait for next request from client
            msg_in = await self._zmq_receive()
            logger.info("ZeroMQ server received request: %s", pprint.pformat(msg_in))

            msg_out = await self._zmq_execute(msg_in)

            #  Send reply back to client
            logger.info("ZeroMQ server sending response: %s", pprint.pformat(msg_out))
            await self._zmq_send(msg_out)

            if self._manager_stopping:
                # This should stop RE Worker if no plan is currently running
                success, _ = await self._stop_re_worker_task()  # Quitting RE Manager
                if not success and self._environment_exists:
                    # Most likely the plan is currently running or RE Worker is 'frozen'
                    #   Killing RE Worker which is running a plan can be done only by
                    #   explicit permission. This option should not be available to
                    #   regular users.
                    # TODO: may be additional level of protection should be added here.
                    await self._kill_re_worker_task()

                await self._watchdog_manager_stopping()
                self._comm_to_watchdog.stop()
                self._comm_to_worker.stop()
                self._zmq_socket.close()
                logger.info("RE Manager was stopped by ZMQ command.")
                break

    # ======================================================================

    def run(self):
        """
        Overrides the `run()` function of the `multiprocessing.Process` class. Called
        by the `start` method.
        """
        logger.info("Starting RE Manager process")
        try:
            asyncio.run(self.zmq_server_comm())
        except Exception as ex:
            logger.exception("Exiting RE Manager with exception %s", str(ex))
        except KeyboardInterrupt:
            # TODO: RE Manager must be orderly closed before Watchdog module is stopped.
            #   Right now it is just killed by SIGINT.
            logger.info("RE Manager Process was stopped by SIGINT. Handling of Ctrl-C has to be revised!!!")
