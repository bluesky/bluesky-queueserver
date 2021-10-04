import asyncio
import zmq
import zmq.asyncio
from multiprocessing import Process
import time as ttime
import pprint
import enum
import uuid

from .comms import PipeJsonRpcSendAsync, CommTimeoutError, validate_zmq_key
from .profile_ops import load_allowed_plans_and_devices, validate_plan
from .plan_queue_ops import PlanQueueOperations
from .output_streaming import setup_console_output_redirection


import logging

logger = logging.getLogger(__name__)


def _generate_uid():
    """
    Generate a new Run List UID.

    Returns
    -------
    str
        Run List UID
    """
    return str(uuid.uuid4())


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

    def __init__(
        self, *args, conn_watchdog, conn_worker, config=None, msg_queue=None, log_level=logging.DEBUG, **kwargs
    ):

        if not conn_watchdog:
            raise RuntimeError(
                "Value of the parameter 'conn_watchdog' is invalid: %s.",
                str(conn_watchdog),
            )
        if not conn_worker:
            raise RuntimeError("Value of the parameter 'conn_worker' is invalid: %s.", str(conn_worker))

        super().__init__(*args, **kwargs)

        self._log_level = log_level
        self._msg_queue = msg_queue

        self._watchdog_conn = conn_watchdog
        self._worker_conn = conn_worker

        # The following attributes hold the state of the system
        self._manager_stopping = False  # Set True to exit manager (by _manager_stop_handler)
        self._environment_exists = False  # True if RE Worker environment exists
        self._manager_state = MState.INITIALIZING
        self._queue_stop_pending = False  # Queue is in the process of being stopped
        self._re_pause_pending = False  # True when worker process has accepted our pause request but
        # we (this manager process) have not yet seen it as 'paused', useful for the situations where the
        # worker did accept a request to pause (deferred) but had already passed its last checkpoint
        self._worker_state_info = None  # Copy of the last downloaded state of RE Worker

        self._re_run_list = []
        self._re_run_list_uid = _generate_uid()

        self._loop = None

        # Communication with the server using ZMQ
        self._ctx = None
        self._zmq_socket = None
        self._zmq_ip_server = "tcp://*:60615"
        self._zmq_private_key = None
        if config:
            if "zmq_addr" in config:
                self._zmq_ip_server = config["zmq_addr"]
            if "zmq_private_key" in config:
                self._zmq_private_key = config["zmq_private_key"]
                if self._zmq_private_key is not None:
                    validate_zmq_key(self._zmq_private_key)

        logger.info("Starting ZMQ server at '%s'", self._zmq_ip_server)
        logger.info(
            "ZMQ control channels: encryption %s", "disabled" if self._zmq_private_key is None else "enabled"
        )

        self._ip_redis_server = "localhost"
        if config and ("redis_addr" in config):
            self._ip_redis_server = config["redis_addr"]

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

        # Note: 'self._config' is a private attribute of 'multiprocessing.Process'. Overriding
        #   this variable may lead to unpredictable and hard to debug issues.
        self._config_dict = config or {}
        self._allowed_plans, self._allowed_devices = {}, {}
        self._allowed_plans_uid = _generate_uid()
        self._allowed_devices_uid = _generate_uid()

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
            accepted, msg = False, "RE Worker environment already exists."
        elif self._manager_state is MState.CREATING_ENVIRONMENT:
            accepted, msg = False, "Manager is already in the process of creating the RE Worker environment."
        elif self._manager_state is not MState.IDLE:
            accepted, msg = False, f"Manager state is not idle. Current state: {self._manager_state.value}"
        else:
            accepted, msg = True, ""
            self._manager_state = MState.CREATING_ENVIRONMENT
            asyncio.ensure_future(self._execute_background_task(self._start_re_worker_task()))
        return accepted, msg

    async def _start_re_worker_task(self):
        """
        Creates worker process.
        """
        # Repeat the checks, since it is important for the manager to be in the correct state.
        if self._environment_exists:
            return False, "Rejected: RE Worker environment already exists"

        if self._manager_state not in [MState.IDLE, MState.CREATING_ENVIRONMENT]:
            return False, f"Manager state is {self._manager_state.value}"

        self._fut_manager_task_completed = self._loop.create_future()

        try:
            success = await self._watchdog_start_re_worker()
            if not success:
                raise RuntimeError("Failed to create Worker process")
            logger.info("Waiting for RE worker to start ...")
            success = await self._fut_manager_task_completed  # TODO: timeout may be needed here
            if success:
                self._environment_exists = True
                self._re_pause_pending = False
                logger.info("Worker started successfully.")
                success, err_msg = True, ""
            else:
                self._environment_exists = True
                await self._confirm_re_worker_exit()
                self._environment_exists = False
                self._worker_state_info = None
                self._re_pause_pending = False

                logger.error("Error occurred while opening RE Worker environment.")
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
        elif self._manager_state is MState.CLOSING_ENVIRONMENT:
            accepted = False
            err_msg = "Manager is already in the process of closing the RE Worker environment."
        elif self._manager_state is MState.EXECUTING_QUEUE:
            accepted = False
            err_msg = "Queue execution is in progress."
        elif self._manager_state != MState.IDLE:
            accepted = False
            err_msg = f"Manager state is not idle. Current state: {self._manager_state.value}"
        else:
            accepted = True
            self._manager_state = MState.CLOSING_ENVIRONMENT
            err_msg = ""
            asyncio.ensure_future(self._execute_background_task(self._stop_re_worker_task()))
        return accepted, err_msg

    async def _stop_re_worker_task(self):
        """
        Closing of the RE Worker.
        """
        # Repeat the checks, since it is important for the manager to be in the correct state.
        if not self._environment_exists:
            self._manager_state = MState.IDLE
            return False, "Environment does not exist"

        if self._manager_state not in [MState.IDLE, MState.CLOSING_ENVIRONMENT]:
            return False, f"Manager state was {self._manager_state.value}"

        self._fut_manager_task_completed = self._loop.create_future()

        success, err_msg = await self._worker_command_close_env()
        if success:
            # Wait for RE Worker to be prepared to close
            self._event_worker_closed = asyncio.Event()

            self._manager_state = MState.CLOSING_ENVIRONMENT
            await self._fut_manager_task_completed  # TODO: timeout may be needed here

            if not await self._confirm_re_worker_exit():
                success = False
                err_msg = "Failed to confirm closing of RE Worker thread"

        self._manager_state = MState.IDLE
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
        self._worker_state_info = None

        # If a plan is running, it needs to be pushed back into the queue
        await self._plan_queue.set_processed_item_as_stopped(exit_status="environment_destroyed", run_uids=[])

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
            logger.info("Waiting for exit confirmation from RE worker ...")
            success, err_msg = await self._worker_command_confirm_exit()

            # Environment is not in valid state anyway. So assume it does not exist.
            self._environment_exists = False
            self._worker_state_info = None

            if success:
                logger.info("Wait for RE Worker process to close (join)")

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
                    if ws["re_state"] == "paused":
                        self._re_pause_pending = False

                    if self._manager_state == MState.CLOSING_ENVIRONMENT:
                        if ws["environment_state"] == "closing":
                            self._fut_manager_task_completed.set_result(None)

                    elif self._manager_state == MState.CREATING_ENVIRONMENT:
                        # If RE Worker environment fails to open, then it switches to 'closing' state.
                        #   Closing must be confirmed by Manager before it is closed.
                        if ws["environment_state"] in "ready":
                            self._fut_manager_task_completed.set_result(True)
                        if ws["environment_state"] == "closing":
                            self._fut_manager_task_completed.set_result(False)

                    elif self._manager_state in (MState.EXECUTING_QUEUE, MState.PAUSED):
                        if ws["re_report_available"]:
                            self._loop.create_task(self._process_plan_report())

                        if ws["run_list_updated"]:
                            self._loop.create_task(self._download_run_list())

    async def _process_plan_report(self):
        """
        Process plan report. Called when plan report is available.
        """
        # TODO: `set_processed_item_as_stopped` needs more precise exit status
        #   current values are not final selection, they are just temporarily for the demo
        # Read report first
        plan_report, err_msg = await self._worker_request_plan_report()
        if plan_report is None:
            # TODO: this would typically mean a bug (communciation error). Probably more
            #       complicated processing is needed
            logger.error("Failed to download plan report: %s. Stopping queue processing.", err_msg)
            await self._plan_queue.set_processed_item_as_stopped(exit_status="manager_error", run_uids=[])
            self._manager_state = MState.IDLE
            self._re_pause_pending = False
        else:
            plan_state = plan_report["plan_state"]
            success = plan_report["success"]
            result = plan_report["result"]
            err_msg = plan_report["err_msg"]

            msg_display = result if result else err_msg
            logger.debug(
                "Report received from RE Worker:\nplan_state=%s\nsuccess=%s\n%s\n)",
                plan_state,
                str(success),
                str(msg_display),
            )

            if plan_state == "completed":
                # Check if the plan was running in the 'immediate_execution' mode.
                item = await self._plan_queue.get_running_item_info()
                immediate_execution = item.get("properties", {}).get("immediate_execution", False)

                # Executed plan is removed from the queue only after it is successfully completed.
                # If a plan was not completed or not successful (exception was raised), then
                # execution of the queue is stopped. It can be restarted later (failed or
                # interrupted plan will still be in the queue.
                await self._plan_queue.set_processed_item_as_completed(exit_status=plan_state, run_uids=result)
                await self._start_plan_task(stop_queue=bool(immediate_execution))
            elif plan_state in ("stopped", "error"):
                # Paused plan was stopped/aborted/halted
                await self._plan_queue.set_processed_item_as_stopped(exit_status=plan_state, run_uids=result)
                self._manager_state = MState.IDLE
                self._re_pause_pending = False
            elif plan_state == "paused":
                # The plan was paused (nothing should be done).
                self._manager_state = MState.PAUSED
                self._re_pause_pending = False
            else:
                logger.error("Unknown plan state %s was returned by RE Worker.", plan_state)

        if self._manager_state == MState.IDLE:
            # No plans are running: deactivate the stop sequence.
            self._queue_stop_deactivate()

    async def _download_run_list(self):
        """
        Download the list of currently available runs when it is updated by the worker
        """
        # Read report first
        run_list, err_msg = await self._worker_request_run_list()
        if run_list is None:
            # TODO: this would typically mean a bug (communication error). Probably more
            #       complicated processing is needed
            logger.error(f"Failed to download plan report: {err_msg}.")
        else:
            self._re_run_list = run_list["run_list"]
            self._re_run_list_uid = _generate_uid()

    async def _start_plan(self):
        """
        Initiate upload of next plan to the worker process for execution.
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

    async def _start_plan_task(self, stop_queue=False):
        """
        Upload the plan to the worker process for execution.
        Plan in the queue is represented as a dictionary with the keys "name" (plan name),
        "args" (list of args), "kwargs" (list of kwargs). Only the plan name is mandatory.
        Names of plans and devices are strings.
        """
        n_pending_plans = await self._plan_queue.get_queue_size()
        if n_pending_plans:
            logger.info("Processing the next queue item: %d plans are left in the queue.", n_pending_plans)
        else:
            logger.info("No items are left in the queue.")

        if not n_pending_plans:
            self._manager_state = MState.IDLE
            self._re_pause_pending = False
            success, err_msg = False, "Queue is empty."
            logger.info(err_msg)

        elif self._queue_stop_pending or stop_queue:
            self._manager_state = MState.IDLE
            self._re_pause_pending = False
            success, err_msg = False, "Queue is stopped."
            logger.info(err_msg)

        elif self._re_pause_pending:
            self._manager_state = MState.IDLE
            self._re_pause_pending = False
            success, err_msg = False, "Queue is stopped due to unresolved outstanding RE pause request."
            logger.info(err_msg)

        else:
            next_item = await self._plan_queue.get_item(pos="front")

            self._re_pause_pending = False

            # The next items is PLAN
            if next_item["item_type"] == "plan":

                # Reset RE environment (worker)
                success, err_msg = await self._worker_command_reset_worker()
                if not success:
                    self._manager_state = MState.IDLE
                    err_msg = f"Failed to reset RE Worker: {err_msg}"
                    logger.error(err_msg)
                    return success, err_msg

                self._manager_state = MState.EXECUTING_QUEUE

                new_plan = await self._plan_queue.process_next_item()

                plan_name = new_plan["name"]
                args = new_plan["args"] if "args" in new_plan else []
                kwargs = new_plan["kwargs"] if "kwargs" in new_plan else {}
                user_name = new_plan["user"]
                user_group = new_plan["user_group"]
                meta = new_plan["meta"] if "meta" in new_plan else {}
                item_uid = new_plan["item_uid"]

                plan_info = {
                    "name": plan_name,
                    "args": args,
                    "kwargs": kwargs,
                    "user": user_name,
                    "user_group": user_group,
                    "meta": meta,
                    "item_uid": item_uid,
                }

                # TODO: Decide if we really want to have metadata in the log
                logger.info("Starting the plan:\n%s.", pprint.pformat(plan_info))

                success, err_msg = await self._worker_command_run_plan(plan_info)
                if not success:
                    await self._plan_queue.set_processed_item_as_stopped(exit_status="error", run_uids=[])
                    self._manager_state = MState.IDLE
                    logger.error(
                        "Failed to start the plan %s.\nError: %s",
                        pprint.pformat(plan_info),
                        err_msg,
                    )
                    err_msg = f"Failed to start the plan: {err_msg}"

            # The next items is INSTRUCTION
            elif next_item["item_type"] == "instruction":
                logger.info("Executing instruction:\n%s.", pprint.pformat(next_item))

                if next_item["name"] == "queue_stop":
                    await self._plan_queue.process_next_item()
                    self._manager_state = MState.EXECUTING_QUEUE
                    asyncio.ensure_future(self._start_plan_task(stop_queue=True))
                    success, err_msg = True, ""
                else:
                    success = False
                    err_msg = f"Unsupported action: '{next_item['name']}' (item {next_item})"

            else:
                success = False
                err_msg = f"Unrecognized item type: '{next_item['item_type']}' (item {next_item})"

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
            raise ValueError(f"Option '{option}' is not supported. Available options: {available_options}")

        elif self._environment_exists:
            success, err_msg = await self._worker_command_continue_plan(option)
            success = bool(success)  # Convert 'None' to False
            if success:
                self._manager_state = MState.EXECUTING_QUEUE
            else:
                logger.error("Failed to pause the running plan: %s", err_msg)

        else:
            success, err_msg = (
                False,
                "Environment does not exist. Can not pause Run Engine.",
            )

        return success, err_msg

    def _load_allowed_plans_and_devices(self):
        """
        Load the list of allowed plans and devices
        """
        try:
            path_pd = self._config_dict["existing_plans_and_devices_path"]
            path_ug = self._config_dict["user_group_permissions_path"]
            self._allowed_plans, self._allowed_devices = load_allowed_plans_and_devices(
                path_existing_plans_and_devices=path_pd, path_user_group_permissions=path_ug
            )
            self._allowed_plans_uid = _generate_uid()
            self._allowed_devices_uid = _generate_uid()
        except Exception as ex:
            raise Exception(
                f"Error occurred while loading lists of allowed plans and devices from '{path_pd}': {str(ex)}"
            )

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

    async def _worker_request_run_list(self):
        try:
            run_list = await self._comm_to_worker.send_msg("request_run_list")
            err_msg = ""
            if run_list is None:
                err_msg = "Failed to obtain the run list from the worker"
        except CommTimeoutError:
            run_list, err_msg = None, "Timeout occurred"
        return run_list, err_msg

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

    @staticmethod
    def _check_request_for_unsupported_params(*, request, param_names):
        """
        Check if the request contains any unsupported parameters. Unsupported parameters
        in the request may indicate an error in API call, therefore API should not be executed
        and the client must be informed of the error.

        Parameters
        ----------
        request : dict
            The dictionary of parameters. Keys are the parameter names.
        param_names : list
            The list of supported parameter names. Request is allowed to contain only the parameters
            contained in this list.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            Request contains unsupported parameters.
        """
        unsupported_params = []
        for name in request:
            if name not in param_names:
                unsupported_params.append(name)

        if unsupported_params:
            names = f"{unsupported_params[0]!r}" if len(unsupported_params) == 1 else f"{unsupported_params}"
            raise ValueError(
                f"API request contains unsupported parameters: {names}. Supported parameters: {param_names}"
            )

    async def _ping_handler(self, request):
        """
        May be called to get some response from the Manager. Returns status of the manager.
        """
        return await self._status_handler(request)

    async def _status_handler(self, request):
        """
        Returns status of the manager.
        """
        # Status is expected to be requested very often. Print the message only in the debug mode.
        logger.debug("Processing 'status' request ...")

        # Computed/retrieved data
        n_pending_items = await self._plan_queue.get_queue_size()
        running_item_info = await self._plan_queue.get_running_item_info()
        n_items_in_history = await self._plan_queue.get_history_size()

        # Prepared output data
        items_in_queue = n_pending_items
        items_in_history = n_items_in_history
        running_item_uid = running_item_info["item_uid"] if running_item_info else None
        manager_state = self._manager_state.value
        queue_stop_pending = self._queue_stop_pending
        worker_environment_exists = self._environment_exists
        re_state = self._worker_state_info["re_state"] if self._worker_state_info else None
        deferred_pause_pending = self._re_pause_pending
        run_list_uid = self._re_run_list_uid
        plan_queue_uid = self._plan_queue.plan_queue_uid
        plan_history_uid = self._plan_queue.plan_history_uid
        devices_allowed_uid = self._allowed_devices_uid
        plans_allowed_uid = self._allowed_plans_uid
        plan_queue_mode = self._plan_queue.plan_queue_mode
        # worker_state_info = self._worker_state_info

        # TODO: consider different levels of verbosity for ping or other command to
        #       retrieve detailed status.
        msg = {
            "msg": "RE Manager",
            "items_in_queue": items_in_queue,
            "items_in_history": items_in_history,
            "running_item_uid": running_item_uid,
            "manager_state": manager_state,
            "queue_stop_pending": queue_stop_pending,
            "worker_environment_exists": worker_environment_exists,
            "re_state": re_state,  # State of Run Engine
            "pause_pending": deferred_pause_pending,  # True/False - Cleared once pause processed
            # If Run List UID change, download the list of runs for the current plan.
            # Run List UID is updated when the list is cleared as well.
            "run_list_uid": run_list_uid,
            "plan_queue_uid": plan_queue_uid,
            "plan_history_uid": plan_history_uid,
            "devices_allowed_uid": devices_allowed_uid,
            "plans_allowed_uid": plans_allowed_uid,
            "plan_queue_mode": plan_queue_mode,
            # "worker_state_info": worker_state_info
        }
        return msg

    async def _plans_allowed_handler(self, request):
        """
        Returns the list of allowed plans.
        """
        logger.info("Returning the list of allowed plans ...")

        try:
            supported_param_names = ["user_group"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

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
            "plans_allowed_uid": self._allowed_plans_uid,
        }

    async def _devices_allowed_handler(self, request):
        """
        Returns the list of allowed devices.
        """
        logger.info("Returning the list of allowed devices ...")

        try:
            supported_param_names = ["user_group"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

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
            "devices_allowed_uid": self._allowed_devices_uid,
        }

    async def _permissions_reload_handler(self, request):
        """
        Reloads the list of allowed plans and devices and user group permission from the default location
        or location set using command line parameters.
        """
        logger.info("Reloading lists of allowed plans and devices ...")
        try:
            supported_param_names = []
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._load_allowed_plans_and_devices()
            success, msg = True, ""
        except Exception as ex:
            success = False
            msg = f"Error: {str(ex)}"
        return {"success": success, "msg": msg}

    async def _queue_get_handler(self, request):
        """
        Returns the contents of the current queue.
        """
        logger.info("Returning current queue and running plan ...")

        try:
            supported_param_names = []
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            plan_queue, running_item, plan_queue_uid = await self._plan_queue.get_queue_full()
            success, msg = True, ""
        except Exception as ex:
            plan_queue = []
            running_item = {}
            plan_queue_uid = ""
            success, msg = False, f"Error: {str(ex)}"

        return {
            "success": success,
            "msg": msg,
            "items": plan_queue,
            "running_item": running_item,
            "plan_queue_uid": plan_queue_uid,
        }

    def _get_item_from_request(self, *, request=None, item=None):
        """
        Extract ``item`` and ``item_type`` from the request, validate the values and report errors
        """
        msg_prefix = "Incorrect request format: "
        supported_item_types = ("plan", "instruction")

        # The following two error reports represent serious bug, which needs to be fixed
        n_request_or_item = sum([(request is None), (item is None)])
        if n_request_or_item != 1:
            raise RuntimeError(
                "Runtime error: Only one of 'request' or 'item' parameters "
                f"may be not None: request={request}, item={item}"
            )

        # Generate error message instead of raising exception: we still want to return
        #   'item' if it exists so that we could send it to the client with error message.
        msg, item_type = "", None

        if request is not None:
            if not isinstance(request, dict):
                raise ValueError(
                    f"Incorrect type of 'request' parameter: "
                    f"type(request)='{type(request)}', expected type is 'dict'"
                )
            if "item" not in request:
                raise ValueError(f"{msg_prefix}request contains no item info")
            item = request["item"]

        if isinstance(item, dict):
            item_type = item.get("item_type", None)
            if item_type is None:
                msg = f"{msg_prefix}'item_type' key is not found"
            elif item_type not in supported_item_types:
                msg = (
                    f"{msg_prefix}unsupported 'item_type' value '{item_type}', "
                    f"supported item types {supported_item_types}"
                )
        else:
            msg = (
                f"{msg_prefix}incorrect type ('{type(item)}') of item parameter: "
                "item parameter must have type 'dict'"
            )

        success = not bool(msg)
        return item, item_type, success, msg

    def _get_user_info_from_request(self, *, request):
        if "user_group" not in request:
            raise ValueError("Incorrect request format: user group is not specified")

        if "user" not in request:
            raise ValueError("Incorrect request format: user name is not specified")

        user = request["user"]
        user_group = request["user_group"]

        if user_group not in self._allowed_plans:
            raise ValueError(f"Unknown user group: '{user_group}'")

        return user, user_group

    def _prepare_item(self, *, item, item_type, user, user_group, generate_new_uid):
        """
        Prepare item before it could be added to the queue or used to update/replace existing item
        in the queue. Preparation includes identification of item type and validation of the item.
        The ``user`` and ``user_group`` are set to the values passed in the request. The new
        ``item_uid`` is generated if requested.

        Parameters
        ----------
        item : dict
            original item passed to RE Manager
        item_type : str
            item type (``plan`` or ``instruction``)
        user : str
            name of the user who submitted or modified the plan
        user_group : str
            name of the user group to which the user belongs
        generate_new_uid : boolean
            generate new ``item_uid`` if True, otherwise keep existing ``item_uid``.
            Raise ``RuntimeError`` if ``generate_new_uid==False`` and the item has no assigned ``item_uid``.

        Returns
        -------
        item : dict
            prepared item
        item_uid_original : str or None
            copy of the original ``item_uid`` saved before it is replaced with new UID, ``None`` if the
            original item (passed as part of ``request``) has no UID.

        Raises
        ------
        RuntimeError
            raised if (1) item validation failed or (2) item has no UID and new UID is not generated
        """
        item = item.copy()  # Create a copy to avoid modifying the original item

        if item_type == "plan":
            allowed_plans = self._allowed_plans[user_group] if self._allowed_plans else self._allowed_plans
            allowed_devices = self._allowed_devices[user_group] if self._allowed_devices else self._allowed_devices
            success, msg = validate_plan(item, allowed_plans=allowed_plans, allowed_devices=allowed_devices)
        elif item_type == "instruction":
            # At this point we support only one instruction ('queue_stop'), so validation is trivial.
            if ("name" in item) and (item["name"] == "queue_stop"):
                success, msg = True, ""
            else:
                success, msg = False, f"Unrecognized instruction: {item}"
        else:
            success, msg = False, f"Invalid item: {item}"

        if not success:
            raise RuntimeError(msg)

        # Add user name and user_id to the plan (for later reference)
        item["user"] = user
        item["user_group"] = user_group

        # Preserve original item UID
        item_uid_original = item.get("item_uid", None)

        if generate_new_uid:
            item["item_uid"] = PlanQueueOperations.new_item_uid()
        else:
            if "item_uid" not in item:
                raise RuntimeError("Item description contains no UID and UID generation is skipped")

        return item, item_uid_original

    def _generate_item_log_msg(self, prefix_msg, success, item_type, item, qsize):
        """
        Generate short log message for reporting results of ``queue_item_add``, ``queue_item_add_batch``
        and ``queue_item_update`` operations.
        """
        log_msg = f"{prefix_msg}: success={success} item_type='{item_type}'"
        if item:
            if "name" in item:
                log_msg += f" name='{item['name']}'"
            if "item_uid" in item:
                log_msg += f" item_uid='{item['item_uid']}'"
        log_msg += f" qsize={qsize}."
        return log_msg

    async def _queue_mode_set_handler(self, request):
        """
        Set plan queue mode. The request must contain the parameter ``mode``,
        which may be a dictionary containing mode options that need to be updated (not all
        the parameters that define the mode) or a string ``default``. (The function will
        accept ``mode={}``: the mode will not be changed in this case). If string value
        ``default`` is passed, then the queue mode is reset to the default mode.
        """
        logger.info("Setting queue mode ...")
        logger.debug("Request: %s", pprint.pformat(request))

        success, msg = True, ""
        try:
            supported_param_names = ["mode"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            if "mode" not in request:
                raise Exception(f"Parameter 'mode' is not found in request {request}")

            plan_queue_mode = request["mode"]
            await self._plan_queue.set_plan_queue_mode(plan_queue_mode, update=True)

        except Exception as ex:
            success = False
            msg = f"Failed to set queue mode: {str(ex)}"

        rdict = {"success": success, "msg": msg}
        return rdict

    async def _queue_item_add_handler(self, request):
        """
        Adds new item to the queue. Item may be a plan or an instruction. Request must
        include the element with the key ``plan`` if the added item is a plan or ``instruction``
        if it is an instruction. The element with the key is a dictionary of plan or instruction
        parameters. The parameters may not include UID, because the function always overwrites
        plan UID. If an item is already assigned UID, it is replaced with the new one.
        The returned plan/instruction contains new UID even if the function failed to add
        the plan to the queue.

        Optional key ``pos`` may be a string (choices "front", "back") or integer (positive
        or negative) that specifies the desired position in the queue. The default value
        is "back" (element is pushed to the back of the queue). If ``pos`` is integer and
        it is outside the range of available elements, then the new element pushed to
        the front or the back of the queue depending on the value of the index.

        It is recommended to use negative indices (counted from the back of the queue)
        when modifying a running queue.
        """
        logger.info("Adding new item to the queue ...")
        logger.debug("Request: %s", pprint.pformat(request))

        item_type, item, qsize, msg = None, None, None, ""

        try:
            supported_param_names = ["user_group", "user", "item", "pos", "before_uid", "after_uid"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            item, item_type, _success, _msg = self._get_item_from_request(request=request)
            if not _success:
                raise Exception(_msg)

            user, user_group = self._get_user_info_from_request(request=request)

            # Always generate a new UID for the added plan!!!
            item, _ = self._prepare_item(
                item=item, item_type=item_type, user=user, user_group=user_group, generate_new_uid=True
            )

            pos = request.get("pos", None)  # Position is optional
            before_uid = request.get("before_uid", None)
            after_uid = request.get("after_uid", None)

            # Adding plan to queue may raise an exception
            item, qsize = await self._plan_queue.add_item_to_queue(
                item, pos=pos, before_uid=before_uid, after_uid=after_uid
            )
            success = True

        except Exception as ex:
            success = False
            msg = f"Failed to add an item: {str(ex)}"

        logger.info(self._generate_item_log_msg("Item added", success, item_type, item, qsize))

        rdict = {"success": success, "msg": msg, "qsize": qsize, "item": item}
        return rdict

    async def _queue_item_add_batch_handler(self, request):
        """
        Adds a batch of items to the end of the queue. The request is expected to contain the following
        elements: ``user`` and ``user_group`` (have the same meaning as for ``queue_item_add`` request);
        ``items`` contains a list of items, each item is a dictionary that contains an element with
        the key corresponding to one of currently supported types (``plan`` or ``instruction``) and
        the value representing properly formatted item parameters (dictionary with ``name`` (required),
        ``args``, ``kwargs`` and ``meta`` keys).

        The function is validating all items in the batch and adds the batch to the queue only if
        all items were validated successfully. Otherwise, the function returns ``success=False``
        and ``msg`` contains error message. The function also returns the list of items (``item_list``).
        For each item, ``success`` indicates if the item was validated successfully and ``msg`` contains
        error message showing the reason of validation failure for the item. The elements ``plan``
        or ``instruction`` contain item parameters that were extracted from the submitted item (if any).
        If items are inserted in the queue, the item parameters will also contain ``item_uid`` of
        the items. The returned item list may be empty if no items were submitted (still considered
        successful operation) or input parameters are invalid and the request could not be processed
        (operation failed).

        If 'global' value ``success`` is ``True`` for the message, then ``success`` values for all items
        are ``True`` and the items are inserted in the queue. There is no need to verify ``success``
        status of each item if 'global' ``success`` is ``True``.
        """
        logger.info("Adding a batch of items to the queue ...")
        logger.debug("Request: %s", pprint.pformat(request))

        success, msg, item_list, results, qsize = True, "", [], [], None

        try:
            supported_param_names = ["user_group", "user", "items", "pos", "before_uid", "after_uid"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            # Prepare items
            if "items" not in request:
                raise Exception("Invalid request format: the list of items is not found")
            items = request["items"]
            items_prepared, results, success = [], [], True

            user, user_group = self._get_user_info_from_request(request=request)

            # Optional parameters
            pos = request.get("pos", None)
            before_uid = request.get("before_uid", None)
            after_uid = request.get("after_uid", None)

            # First validate all the items
            for item_info in items:
                item, item_type = None, None
                try:
                    item, item_type, _success, _msg = self._get_item_from_request(item=item_info)
                    if not _success:
                        raise Exception(_msg)

                    # Always generate a new UID for the added plan!!!
                    item_prepared, _ = self._prepare_item(
                        item=item, item_type=item_type, user=user, user_group=user_group, generate_new_uid=True
                    )

                    items_prepared.append(item_prepared)
                    results.append({"success": True, "msg": ""})

                except Exception as ex:
                    success = False
                    items_prepared.append(item)  # Add unchanged item or None if no item is found
                    results.append({"success": False, "msg": f"Failed to add a plan: {ex}"})

            if len(results) != len(items) != len(items_prepared):
                # This error should never happen, but the message may be useful for debugging if it happens.
                raise Exception("Error in data processing algorithm occurred")

            if success:
                # 'success' may still change
                item_list, results, _, success = await self._plan_queue.add_batch_to_queue(
                    items_prepared, pos=pos, before_uid=before_uid, after_uid=after_uid
                )
            else:
                # Return the copy of the items received as part of the request without change
                item_list = items

            if not success:
                n_items = len(item_list)
                n_failed = sum([not _["success"] for _ in results])
                msg = f"Failed to add all items: validation of {n_failed} out of {n_items} submitted items failed"

        except Exception as ex:
            success = False
            msg = f"Failed to add an item: {str(ex)}"

        try:
            qsize = await self._plan_queue.get_queue_size()
        except Exception:
            pass

        logger.info(self._generate_item_log_msg("Batch of items added", success, None, None, qsize))

        # Note, that 'item_list' may be an empty list []
        rdict = {"success": success, "msg": msg, "qsize": qsize, "items": item_list, "results": results}

        return rdict

    async def _queue_item_update_handler(self, request):
        """
        Updates the existing item in the queue. Item may be a plan or an instruction. The request
        must contain item description (``plan`` or ``instruction``) in the same format as for
        ``queue_item_add`` request, except that the description must contain UID (``item_uid`` key).
        The queue must contain an item with the identical UID. This item will be replaced with
        the item passed as part of the request. The request may contain an optional parameter ``replace``.
        If ``replace`` is missing or evaluated as ``False``, then the item UID in the queue is
        not changed. If ``replace`` is ``True``, then the new UID is generated before the item is
        replaced. The original UID is still used to locate the item in the queue before replacing it.
        The ``replace`` parameter allows to distinguish between small changes to item parameters
        (``replace=False`` - UID is not changed) or complete replacement of the item (``replace=True`` -
        new UID is generated).
        """
        success, msg, qsize, item, item_type = True, "", 0, None, None

        try:
            supported_param_names = ["user_group", "user", "item", "replace"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            item, item_type, _success, _msg = self._get_item_from_request(request=request)
            if not _success:
                raise Exception(_msg)

            user, user_group = self._get_user_info_from_request(request=request)

            # Generate new UID if 'replace' flag is True, otherwise update the plan
            generate_new_uid = bool(request.get("replace", False))
            item, item_uid_original = self._prepare_item(
                item=item, item_type=item_type, user=user, user_group=user_group, generate_new_uid=generate_new_uid
            )

            # item["item_uid"] will change if uid is replaced, but we still need
            #   'original' UID to update the correct item
            item, qsize = await self._plan_queue.replace_item(item, item_uid=item_uid_original)
            success = True

        except Exception as ex:
            success = False
            msg = f"Failed to add an item: {str(ex)}"

        logger.info(self._generate_item_log_msg("Item updated", success, item_type, item, qsize))

        rdict = {"success": success, "msg": msg, "qsize": qsize, "item": item}

        return rdict

    async def _queue_item_get_handler(self, request):
        """
        Returns an item from the queue. The position of the item
        may be specified as an index (positive or negative) or a string
        from the set {``front``, ``back``}. The default option is ``back``
        """
        logger.info("Getting an item from the queue ...")
        item, msg = {}, ""
        try:
            supported_param_names = ["pos", "uid"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            pos = request.get("pos", None)
            uid = request.get("uid", None)
            item = await self._plan_queue.get_item(pos=pos, uid=uid)
            success = True
        except Exception as ex:
            success = False
            msg = f"Failed to get an item: {str(ex)}"

        return {"success": success, "msg": msg, "item": item}

    async def _queue_item_remove_handler(self, request):
        """
        Removes (pops) item from the queue. The position of the item
        may be specified as an index (positive or negative) or a string
        from the set {``front``, ``back``}. The default option is ``back``.
        If ``uid`` is specified, then the position is ignored. A plan with
        the UID must exist in the queue, otherwise operation fails.
        """
        logger.info("Removing item from the queue ...")
        item, qsize, msg = {}, None, ""
        try:
            supported_param_names = ["pos", "uid"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            pos = request.get("pos", None)
            uid = request.get("uid", None)
            item, qsize = await self._plan_queue.pop_item_from_queue(pos=pos, uid=uid)
            success = True
        except Exception as ex:
            success = False
            msg = f"Failed to remove an item: {str(ex)}"

        return {"success": success, "msg": msg, "item": item, "qsize": qsize}

    async def _queue_item_remove_batch_handler(self, request):
        """
        Removes (pops) a batch of items from the queue. The batch of items is defined as a list
        of item UIDs (parameter ``uids``). The list of UIDs may be empty. By default the function
        ignores the errors (skips items that are not found in the queue and does not check
        the batch for repeated items). If the parameter ``ignore_missing=False``, then the
        method fails if the batch contains repeated items or if any of the items are not found
        in the queue. If the method fails, then no items are removed from the queue.
        """
        logger.info("Removing a batch of items from the queue ...")
        items, qsize, msg = [], None, ""
        try:
            supported_param_names = ["uids", "ignore_missing"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            uids = request.get("uids", None)
            ignore_missing = request.get("ignore_missing", True)

            if uids is None:
                raise Exception("Request does not contain the list of UIDs")

            items, qsize = await self._plan_queue.pop_item_from_queue_batch(
                uids=uids, ignore_missing=ignore_missing
            )
            success = True
        except Exception as ex:
            success = False
            msg = f"Failed to remove a batch of items: {str(ex)}"

        return {"success": success, "msg": msg, "items": items, "qsize": qsize}

    async def _queue_item_move_handler(self, request):
        """
        Moves a plan to a new position in the queue. Source and destination
        for the plan may be specified as position of the plan in the queue
        (positive or negative integer, ``front`` or ``back``) or as a plan
        UID. The 'source' plan is moved to the new position or placed before
        or after the 'destination' plan. Both source and destination must be specified.
        """
        logger.info("Moving a queue item ...")
        item, qsize, msg = {}, None, ""
        try:
            supported_param_names = ["pos", "uid", "pos_dest", "before_uid", "after_uid"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            pos = request.get("pos", None)
            uid = request.get("uid", None)
            pos_dest = request.get("pos_dest", None)
            before_uid = request.get("before_uid", None)
            after_uid = request.get("after_uid", None)
            item, qsize = await self._plan_queue.move_item(
                pos=pos, uid=uid, pos_dest=pos_dest, before_uid=before_uid, after_uid=after_uid
            )
            success = True
        except Exception as ex:
            success = False
            msg = f"Failed to move the item: {str(ex)}"

        return {"success": success, "msg": msg, "item": item, "qsize": qsize}

    async def _queue_item_move_batch_handler(self, request):
        """
        Moves a batch of items to a new position in the queue. The batch if items
        is specified as a list of non-repeated UIDs. All items in the batch must
        exist in the queue, otherwise the operation fails. The batch may be empty
        (``uids`` is an empty list). The destination may be specified as a position
        ``pos_dest`` (accepts string values ``front`` and ``back``) or UID of the
        existing item that is not included in the batch. The batch items are moved
        after the item (parameter ``after_uid``) or before the item (parameter
        ``before_uid``). If ``reorder==False``, then the items in the batch are arranged
        in the order in which they are listed in ``uids``, otherwise they are moved
        in the same order they are in the queue (the order in ``uids`` is ignored).
        The parameters ``pos_dest``, ``before_uid`` and ``after_uid`` are mutually
        exclusive. The destionation MUST be specified, otherwise the operation fails.
        """
        logger.info("Moving a batch of queue items ...")
        items, qsize, msg = [], None, ""
        try:
            supported_param_names = ["uids", "pos_dest", "before_uid", "after_uid", "reorder"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            uids = request.get("uids", None)
            pos_dest = request.get("pos_dest", None)
            before_uid = request.get("before_uid", None)
            after_uid = request.get("after_uid", None)
            reorder = request.get("reorder", False)

            if uids is None:
                raise Exception("Request does not contain the list of UIDs")

            items, qsize = await self._plan_queue.move_batch(
                uids=uids, pos_dest=pos_dest, before_uid=before_uid, after_uid=after_uid, reorder=reorder
            )
            success = True
        except Exception as ex:
            success = False
            msg = f"Failed to move the batch of items: {str(ex)}"

        return {"success": success, "msg": msg, "qsize": qsize, "items": items}

    async def _queue_item_execute_handler(self, request):
        """
        Immediately start execution of the submitted item. The item may be a plan or an
        instruction. The request fails if item execution can not be started immediately
        (RE Manager is not in IDLE state, RE Worker environment does not exist, etc.).
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

        The API DOES NOT START EXECUTION OF THE QUEUE. Once execution of the submitted
        item is finished, RE Manager is switched to the IDLE state.
        """
        logger.info("Starting immediate executing a queue item ...")
        logger.debug("Request: %s", pprint.pformat(request))

        item_type, item, qsize, msg = None, None, None, ""
        item_uid = None

        try:
            supported_param_names = ["item", "user_group", "user"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            item, item_type, _success, _msg = self._get_item_from_request(request=request)
            if not _success:
                raise Exception(_msg)

            user, user_group = self._get_user_info_from_request(request=request)

            # Always generate a new UID for the added plan!!!
            item, _ = self._prepare_item(
                item=item, item_type=item_type, user=user, user_group=user_group, generate_new_uid=True
            )

            # Execution of an item can not be immediately started if RE Manager is not idle
            if self._manager_state != MState.IDLE:
                raise RuntimeError(f"RE Manager is not idle. RE Manager state is '{self._manager_state.value}'")

            # Adding plan to queue may raise an exception
            item["properties"] = {"immediate_execution": True}
            item, qsize_with_new_item = await self._plan_queue.add_item_to_queue(item, pos="front")
            item_uid = item.get("item_uid", None)  # The item WAS added to the queue

            # Start the queue
            start_success, start_msg = await self._start_plan()
            if not start_success:
                raise RuntimeError(start_msg)

            # The returned queue size is not supposed to include the plan that was started.
            qsize = max(qsize_with_new_item - 1, 0)
            success = True

        except Exception as ex:
            # If execution of the plan fails to start, the plan remains in the queue and needs to be deleted.
            #   We use UID to delete the plan. It guarantees that only recently added plan is deleted.
            #   Exception may be raised if the plan is not in the queue and must be properly handled.
            if item_uid is not None:
                try:
                    await self._plan_queue.pop_item_from_queue(uid=item_uid)
                except Exception as ex:
                    logger.exception("Failed to delete plan with UID '%s' from queue: %s", item_uid, str(ex))
            success = False
            msg = f"Failed to start execution of the item: {str(ex)}"

        logger.info(self._generate_item_log_msg("Item execution started", success, item_type, item, qsize))

        rdict = {"success": success, "msg": msg, "qsize": qsize, "item": item}
        return rdict

    async def _queue_clear_handler(self, request):
        """
        Remove all entries from the plan queue (does not affect currently executed run)
        """
        logger.info("Clearing the queue ...")
        try:
            supported_param_names = []
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            await self._plan_queue.clear_queue()
            success, msg = True, ""
        except Exception as ex:
            success, msg = False, f"Error: {ex}"
        return {"success": success, "msg": msg}

    async def _history_get_handler(self, request):
        """
        Returns the contents of the plan history.
        """
        logger.info("Returning plan history ...")
        try:
            supported_param_names = []
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            plan_history, plan_history_uid = await self._plan_queue.get_history()
            success, msg = True, ""
        except Exception as ex:
            success, msg = False, f"Error: {ex}"
            plan_history, plan_history_uid = [], ""

        return {"success": success, "msg": msg, "items": plan_history, "plan_history_uid": plan_history_uid}

    async def _history_clear_handler(self, request):
        """
        Remove all entries from the plan history
        """
        logger.info("Clearing the plan execution history ...")
        try:
            supported_param_names = []
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            await self._plan_queue.clear_history()
            success, msg = True, ""
        except Exception as ex:
            success, msg = False, f"Error: {ex}"

        return {"success": success, "msg": msg}

    async def _environment_open_handler(self, request):
        """
        Creates RE environment: creates RE Worker process, starts and configures Run Engine.
        """
        logger.info("Opening the new RE environment ...")
        try:
            supported_param_names = []
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            success, msg = await self._start_re_worker()
        except Exception as ex:
            success, msg = False, f"Error: {ex}"

        return {"success": success, "msg": msg}

    async def _environment_close_handler(self, request):
        """
        Orderly closes of RE environment. The command returns success only if no plan is running,
        i.e. RE Manager is in the idle state. The command is rejected if a plan is running.
        """
        logger.info("Closing existing RE environment ...")
        try:
            supported_param_names = []
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            success, msg = await self._stop_re_worker()
        except Exception as ex:
            success, msg = False, f"Error: {ex}"

        return {"success": success, "msg": msg}

    async def _environment_destroy_handler(self, request):
        """
        Destroys RE environment by killing RE Worker process. This is a last resort command which
        should be made available only to expert level users.
        """
        logger.info("Destroying current RE environment ...")
        try:
            supported_param_names = []
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            success, msg = await self._kill_re_worker()
        except Exception as ex:
            success, msg = False, f"Error: {ex}"

        return {"success": success, "msg": msg}

    async def _queue_start_handler(self, request):
        """
        Start execution of the loaded queue. Additional runs can be added to the queue while
        it is executed. If the queue is empty, then nothing will happen.
        """
        logger.info("Starting queue processing ...")
        try:
            supported_param_names = []
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            success, msg = await self._start_plan()
        except Exception as ex:
            success, msg = False, f"Error: {ex}"

        return {"success": success, "msg": msg}

    async def _queue_stop_handler(self, request):
        """
        Stop execution of the running queue. Currently running plan will be completed
        and the next plan will not be started. Stopping the queue is a safe operation
        that should not lead to data loss.

        The request will fail if the queue is not running.
        """
        logger.info("Activating 'stop queue' sequence ...")
        try:
            supported_param_names = []
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            success, msg = self._queue_stop_activate()
        except Exception as ex:
            success, msg = False, f"Error: {ex}"

        return {"success": success, "msg": msg}

    async def _queue_stop_cancel_handler(self, request):
        """
        Deactivate the sequence of stopping the queue execution.

        The request always succeeds.
        """
        logger.info("Deactivating 'stop queue' sequence ...")
        try:
            supported_param_names = []
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            success, msg = self._queue_stop_deactivate()
        except Exception as ex:
            success, msg = False, f"Error: {ex}"

        return {"success": success, "msg": msg}

    async def _re_pause_handler(self, request):
        """
        Pause Run Engine
        """
        logger.info("Pausing the queue (currently running plan) ...")

        try:
            supported_param_names = ["option"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

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
            if success:
                self._re_pause_pending = True
        except Exception as ex:
            success, msg = False, f"Error: {ex}"

        return {"success": success, "msg": msg}

    async def _re_resume_handler(self, request):
        """
        Run Engine: resume execution of a paused plan
        """
        logger.info("Resuming paused plan ...")

        try:
            supported_param_names = []
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            success, msg = await self._continue_run_engine(option="resume")
        except Exception as ex:
            success, msg = False, f"Error: {ex}"

        return {"success": success, "msg": msg}

    async def _re_stop_handler(self, request):
        """
        Run Engine: stop execution of a paused plan
        """
        logger.info("Stopping paused plan ...")
        try:
            supported_param_names = []
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            success, msg = await self._continue_run_engine(option="stop")
        except Exception as ex:
            success, msg = False, f"Error: {ex}"

        return {"success": success, "msg": msg}

    async def _re_abort_handler(self, request):
        """
        Run Engine: abort execution of a paused plan
        """
        logger.info("Aborting paused plan ...")
        try:
            supported_param_names = []
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            success, msg = await self._continue_run_engine(option="abort")
        except Exception as ex:
            success, msg = False, f"Error: {ex}"

        return {"success": success, "msg": msg}

    async def _re_halt_handler(self, request):
        """
        Run Engine: halt execution of a paused plan
        """
        logger.info("Halting paused plan ...")
        try:
            supported_param_names = []
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            success, msg = await self._continue_run_engine(option="halt")
        except Exception as ex:
            success, msg = False, f"Error: {ex}"

        return {"success": success, "msg": msg}

    async def _re_runs_handler(self, request):
        """
        Return the list of runs for the currently running plan. The list includes open and already
        closed runs.
        """
        logger.info("Returning the list of runs for the running plan ...")

        success, msg, run_list, run_list_uid = True, "", [], ""
        try:
            supported_param_names = ["option"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            option = request["option"] if "option" in request else "active"
            available_options = ("active", "open", "closed")

            if option in available_options:
                if option == "open":
                    run_list = [_ for _ in self._re_run_list if _["is_open"]]
                elif option == "closed":
                    run_list = [_ for _ in self._re_run_list if not _["is_open"]]
                else:
                    run_list = self._re_run_list
                success, msg = True, ""
            else:
                raise ValueError(f"Option '{option}' is not supported. Available options: {available_options}")

            run_list_uid = self._re_run_list_uid

        except Exception as ex:
            success, msg = False, f"Error: {ex}"

        return {"success": success, "msg": msg, "run_list": run_list, "run_list_uid": run_list_uid}

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
        success, msg = True, ""

        try:
            supported_param_names = ["option"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            # Default option
            option = "safe_on"

            # If option is specified in request, then use the value from the request
            r_option = request.get("option", None)
            if r_option:
                if r_option in allowed_options:
                    option = r_option
                else:
                    raise ValueError(f"Option '{r_option}' is not allowed. Allowed options: {allowed_options}")

            if (option == "safe_on") and (self._manager_state != MState.IDLE):
                raise RuntimeError(
                    f"Closing RE Manager with option '{option}' is allowed "
                    f"only in 'idle' state. Current state: '{self._manager_state.value}'"
                )

            self._manager_stopping = True
        except Exception as ex:
            success, msg = False, f"Error: {ex}"

        return {"success": success, "msg": msg}

    async def _manager_kill_handler(self, request):
        """
        Testing API: blocks event loop of RE Manager process forever and
        causes Watchdog process to restart RE Manager.
        """
        try:
            # Verification of parameters are mostly for consistency with other API.
            # This API is expected to be used exclusively for testing and debugging.
            supported_param_names = []
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            # Block the event loop forever. The manager process should be automatically restarted.
            while True:
                ttime.sleep(10)
        except Exception as ex:
            success, msg = False, f"Error: {ex}"

        return {"success": success, "msg": msg}

    async def _zmq_execute(self, msg):
        method = msg["method"]
        params = msg["params"]
        handler_dict = {
            "ping": "_ping_handler",
            "status": "_status_handler",
            "queue_get": "_queue_get_handler",
            "plans_allowed": "_plans_allowed_handler",
            "devices_allowed": "_devices_allowed_handler",
            "permissions_reload": "_permissions_reload_handler",
            "history_get": "_history_get_handler",
            "history_clear": "_history_clear_handler",
            "environment_open": "_environment_open_handler",
            "environment_close": "_environment_close_handler",
            "environment_destroy": "_environment_destroy_handler",
            "queue_mode_set": "_queue_mode_set_handler",
            "queue_item_add": "_queue_item_add_handler",
            "queue_item_add_batch": "_queue_item_add_batch_handler",
            "queue_item_update": "_queue_item_update_handler",
            "queue_item_get": "_queue_item_get_handler",
            "queue_item_remove": "_queue_item_remove_handler",
            "queue_item_remove_batch": "_queue_item_remove_batch_handler",
            "queue_item_move": "_queue_item_move_handler",
            "queue_item_move_batch": "_queue_item_move_batch_handler",
            "queue_item_execute": "_queue_item_execute_handler",
            "queue_clear": "_queue_clear_handler",
            "queue_start": "_queue_start_handler",
            "queue_stop": "_queue_stop_handler",
            "queue_stop_cancel": "_queue_stop_cancel_handler",
            "re_pause": "_re_pause_handler",
            "re_resume": "_re_resume_handler",
            "re_stop": "_re_stop_handler",
            "re_abort": "_re_abort_handler",
            "re_halt": "_re_halt_handler",
            "re_runs": "_re_runs_handler",
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

        self._plan_queue = PlanQueueOperations(redis_host=self._ip_redis_server)
        await self._plan_queue.start()

        # Delete Redis entries (for testing and debugging)
        # self._plan_queue.delete_pool_entries()

        # Load lists of allowed plans and devices
        logger.info("Loading the lists of allowed plans and devices ...")
        try:
            self._load_allowed_plans_and_devices()
        except Exception as ex:
            logger.exception("Exception: %s", ex)

        # Set the environment state based on whether the worker process is alive (request Watchdog)
        self._environment_exists = await self._is_worker_alive()

        # Now check if the plan is still being executed (if it was executed)
        if self._environment_exists:
            # Attempt to load the list of active runs.
            re_run_list, _ = await self._worker_request_run_list()
            self._re_run_list = re_run_list["run_list"] if re_run_list else []
            self._re_run_list_uid = _generate_uid()

            self._worker_state_info, err_msg = await self._worker_request_state()
            if self._worker_state_info:
                item_uid_running = self._worker_state_info["running_item_uid"]
                re_state = self._worker_state_info["re_state"]
                re_report_available = self._worker_state_info["re_report_available"]
                re_deferred_pause_requested = self._worker_state_info["re_deferred_pause_requested"]
                if item_uid_running and (re_report_available or (re_state != "idle")):
                    # If 're_state' is 'idle', then consider the queue as running only if
                    #   there is unprocessed report. If report was processed, then assume that
                    #   the queue is not running.
                    self._re_pause_pending = re_deferred_pause_requested
                    # Plan is running. Check if it is the same plan as in redis.
                    plan_stored = await self._plan_queue.get_running_item_info()
                    if "item_uid" in plan_stored:
                        item_uid_stored = plan_stored["item_uid"]
                        if re_state in ("paused", "pausing"):
                            self._manager_state = MState.PAUSED  # Paused and can be resumed
                        else:
                            self._manager_state = MState.EXECUTING_QUEUE  # Wait for plan completion
                        if item_uid_stored != item_uid_running:
                            # Guess is that the environment may still work, so restart is
                            #   only recommended if it is convenient.
                            logger.warning(
                                "Inconsistency of internal QServer data was detected: \n"
                                "UID of currently running plan is '%s', "
                                "instead of '%s'.\n"
                                "RE execution environment may need to be closed and created again \n"
                                "to restore data integrity.",
                                item_uid_running,
                                item_uid_stored,
                            )
            else:
                logger.error("Error while reading RE Worker status: %s", err_msg)
        else:
            self._worker_state_info = None

        if self._manager_state not in (MState.EXECUTING_QUEUE, MState.PAUSED):
            # TODO: logic may need to be revised
            await self._plan_queue.set_processed_item_as_completed(exit_status="unknown", run_uids=[])

        logger.info("Starting ZeroMQ server ...")
        self._zmq_socket = self._ctx.socket(zmq.REP)

        if self._zmq_private_key is not None:
            self._zmq_socket.set(zmq.CURVE_SERVER, 1)
            self._zmq_socket.set(zmq.CURVE_SECRETKEY, self._zmq_private_key.encode("utf-8"))

        self._zmq_socket.bind(self._zmq_ip_server)
        logger.info("ZeroMQ server is waiting on %s", str(self._zmq_ip_server))

        if self._manager_state == MState.INITIALIZING:
            self._manager_state = MState.IDLE

        while True:
            #  Wait for next request from client
            msg_in = await self._zmq_receive()
            logger.debug("ZeroMQ server received request: %s", pprint.pformat(msg_in))

            msg_out = await self._zmq_execute(msg_in)

            #  Send reply back to client
            logger.debug("ZeroMQ server sending response: %s", pprint.pformat(msg_out))
            await self._zmq_send(msg_out)

            if self._manager_stopping:
                # The pause is needed for reliable execution of unit tests. When unit tests
                #   are executed with GitHub actions, the socket is often destroyed before
                #   the confirmation message is delivered via ZMQ, causing tests to fail with
                #   substantial probability.
                await asyncio.sleep(0.1)

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
        setup_console_output_redirection(msg_queue=self._msg_queue)

        logging.basicConfig(level=max(logging.WARNING, self._log_level))
        logging.getLogger(__name__).setLevel(self._log_level)

        logger.info("Starting RE Manager process")
        try:
            asyncio.run(self.zmq_server_comm())
        except Exception as ex:
            logger.exception("Exiting RE Manager with exception %s", str(ex))
        except KeyboardInterrupt:
            # TODO: RE Manager must be orderly closed before Watchdog module is stopped.
            #   Right now it is just killed by SIGINT.
            logger.info("RE Manager Process was stopped by SIGINT. Handling of Ctrl-C has to be revised!!!")
