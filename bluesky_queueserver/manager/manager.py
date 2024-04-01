import asyncio
import copy
import enum
import logging
import time as ttime
import uuid
from datetime import datetime
from multiprocessing import Process

import zmq
import zmq.asyncio

import bluesky_queueserver

from .comms import CommTimeoutError, PipeJsonRpcSendAsync, validate_zmq_key
from .logging_setup import PPrintForLogging as ppfl
from .logging_setup import setup_loggers
from .output_streaming import setup_console_output_redirection
from .plan_queue_ops import PlanQueueOperations
from .profile_ops import (
    check_if_function_allowed,
    load_allowed_plans_and_devices,
    load_existing_plans_and_devices,
    load_user_group_permissions,
    validate_plan,
    validate_user_group_permissions,
)
from .task_results import TaskResults

logger = logging.getLogger(__name__)

qserver_version = bluesky_queueserver.__version__


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
    STARTING_QUEUE = "starting_queue"  # Starting the first plan of the queue
    EXECUTING_QUEUE = "executing_queue"
    EXECUTING_TASK = "executing_task"
    CLOSING_ENVIRONMENT = "closing_environment"
    DESTROYING_ENVIRONMENT = "destroying_environment"


class LockInfo:
    def __init__(self):
        self._lock_key_emergency = None
        self.clear()

    def set_emergency_lock_key(self, lock_key):
        """
        Emergency lock key may be ``None`` (not set) or non-empty string.
        """
        if isinstance(lock_key, type(None)) or (isinstance(lock_key, str) and lock_key):
            self._lock_key_emergency = lock_key
        else:
            raise ValueError(f"Invalid emergency lock key: {lock_key!r}")

    @property
    def lock_key_emergency(self):
        """
        Readable property that returns the emergency lock key.
        """
        return self._lock_key_emergency

    @property
    def time_str(self):
        if self.time:
            return datetime.fromtimestamp(self.time).strftime("%m/%d/%Y %H:%M:%S")
        else:
            return ""

    def is_set(self):
        return self.environment or self.queue

    def set(self, *, environment, queue, lock_key, note, user_name):
        """
        Set the lock. The values of ``environment`` and/or ``queue`` must be ``True``.
        ``note`` may be a string or ``None``.
        """
        if not environment and not queue:
            raise ValueError(
                "Invalid option: environment and/or queue must be selected: "
                f"environment={environment} queue={queue}"
            )
        self.environment = environment
        self.queue = queue
        self.lock_key = lock_key
        self.note = note
        self.user_name = user_name
        self.time = ttime.time()
        self.uid = _generate_uid()

    def clear(self):
        """
        Clear the lock (lock info). This unlocks RE Manager.
        """
        self.environment = False
        self.queue = False
        self.lock_key = None
        self.note = None
        self.time = None
        self.user_name = None
        self.uid = _generate_uid()

    def check_lock_key(self, lock_key, *, use_emergency_key=False):
        """
        Returns ``True`` if RE Manager is not locked (any key is valid) or if the key matches.
        Optionally check match with the emergency lock key if the key is set.
        """
        k = self.lock_key
        key_valid = not self.is_set() or (lock_key == k)
        if not key_valid and use_emergency_key:
            ek = self.lock_key_emergency
            key_valid = ek and (lock_key == ek)
        return key_valid

    def to_str(self):
        """
        Format information as a text. The lock key is intentionally not printed!
        """
        s = f"RE Manager is locked by {self.user_name} at {self.time_str}\n"
        s += f"Environment is locked: {self.environment}\n"
        s += f"Queue is locked:       {self.queue}\n"
        s += f"Emergency lock key:    {'set' if self.lock_key_emergency else 'not set'}\n"
        s += f"Note: {self.note or '---'}"
        return s

    def from_dict(self, lock_info_dict):
        """
        Load lock info from dictionary.
        """
        keys_required = {"environment", "queue", "lock_key", "note", "user", "time", "uid"}
        keys = set(lock_info_dict.keys())
        keys_missing = keys_required - keys
        if keys_missing:
            raise KeyError(f"Failed to load lock info from a dictionary: keys {list(keys_missing)} are missing")
        else:
            self.environment = lock_info_dict["environment"]
            self.queue = lock_info_dict["queue"]
            self.lock_key = lock_info_dict["lock_key"]
            self.note = lock_info_dict["note"]
            self.user_name = lock_info_dict["user"]
            self.time = lock_info_dict["time"]
            self.uid = lock_info_dict["uid"]

    def to_dict(self):
        """
        Represent lock info as a dictionary.
        """
        return {
            "environment": self.environment,
            "queue": self.queue,
            "lock_key": self.lock_key,
            "note": self.note,
            "user": self.user_name,
            "time": self.time,
            "uid": self.uid,
        }


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
        self,
        *args,
        conn_watchdog,
        conn_worker,
        config=None,
        msg_queue=None,
        log_level=logging.DEBUG,
        number_of_restarts,
        **kwargs,
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

        # The number of time RE Manager was started (including the first attempt to start it).
        #   Numbering starts from 1.
        self._number_of_restarts = number_of_restarts

        self._comm_to_worker_timeout = 0.5  # Timeout for regular requests, s
        self._comm_to_worker_timeout_long = 10  # Timeout for potentially long requests, s

        self._lock_info = LockInfo()  # Lock/unlock environment and/or queue
        self._lock_info.set_emergency_lock_key(config["lock_key_emergency"])

        # The following attributes hold the state of the system
        self._manager_stopping = False  # Set True to exit manager (by _manager_stop_handler)
        self._environment_exists = False  # True if RE Worker environment exists
        self._manager_state = MState.INITIALIZING
        self.__queue_stop_pending = False  # Queue is in the process of being stopped
        self._re_pause_pending = False  # True when worker process has accepted our pause request but
        # we (this manager process) have not yet seen it as 'paused', useful for the situations where the
        # worker did accept a request to pause (deferred) but had already passed its last checkpoint
        self._worker_state_info = None  # Copy of the last downloaded state of RE Worker

        self.__queue_autostart_enabled = False
        self._queue_autostart_event = None

        self._exec_loop_deactivated_event = None  # Used to defer manager status change
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

        self._redis_name_prefix = "qs_default"
        if config and ("redis_name_prefix" in config):
            self._redis_name_prefix = config["redis_name_prefix"]

        self._plan_queue = None  # Object of class plan_queue_ops.PlanQueueOperations

        self._heartbeat_generator_task = None  # Task for heartbeat generator
        self._worker_status_task = None  # Task for periodic checks of Worker status

        # The future is used for opening/closing RE Worker environment. Those task always run separately.
        self._fut_manager_task_completed = None

        # The objects of PipeJsonRpcSendAsync used for communciation with
        #   Watchdog and Worker modules. The object must be instantiated in the loop.
        self._comm_to_watchdog = None
        self._comm_to_worker = None

        self._use_ipython_kernel = config["use_ipython_kernel"]

        # Note: 'self._config' is a private attribute of 'multiprocessing.Process'. Overriding
        #   this variable may lead to unpredictable and hard to debug issues.
        self._config_dict = config or {}
        self._user_group_permissions = {}
        self._existing_plans, self._existing_devices = {}, {}
        self._existing_plans_uid = _generate_uid()
        self._existing_devices_uid = _generate_uid()
        self._allowed_plans, self._allowed_devices = {}, {}
        self._allowed_plans_uid = _generate_uid()
        self._allowed_devices_uid = _generate_uid()

        self._running_task_uid = None  # UID of currently running foreground task (if any)
        self._task_results = None  # TaskResults(), uses threading.Lock

        # Indicates when to update the existing plans and devices
        ug_permissions_reload = self._config_dict["user_group_permissions_reload"]
        if ug_permissions_reload not in ("NEVER", "ON_REQUEST", "ON_STARTUP"):
            logger.error(
                "Unknown option for reloading user group permissions: '%s'. "
                "The permissions will be reloaded on each startup of RE Manager.",
                ug_permissions_reload,
            )
            ug_permissions_reload = "ON_STARTUP"
        self._user_group_permissions_reload_option = ug_permissions_reload

    @property
    def queue_stop_pending(self):
        """
        It is not necessary to read the value from Redis each time, since it happens too often.
        """
        return self.__queue_stop_pending

    async def set_queue_stop_pending(self, enabled):
        """
        Saves autostart_enabled to Redis.
        """
        enabled = bool(enabled)
        self.__queue_stop_pending = enabled
        if self._plan_queue:
            await self._plan_queue.stop_pending_save({"enabled": enabled})

    @property
    def queue_autostart_enabled(self):
        """
        It is not necessary to read the value from Redis each time, since it happens too often.
        """
        return self.__queue_autostart_enabled

    async def set_queue_autostart_enabled(self, enabled):
        """
        Saves autostart_enabled to Redis.
        """
        enabled = bool(enabled)
        self.__queue_autostart_enabled = enabled
        if self._plan_queue:
            await self._plan_queue.autostart_mode_save({"enabled": enabled})

    async def _heartbeat_generator(self):
        """
        Heartbeat generator for Watchdog (indicates that the loop is running)
        """
        t_period = 0.5
        while True:
            try:
                await asyncio.sleep(t_period)
                await self._watchdog_send_heartbeat()
            except asyncio.CancelledError:
                break
            except Exception as ex:
                logger.warning(f"Exception occurred while sending heartbeat: {ex}")

    # ======================================================================
    #          Functions that implement functionality of the server

    async def _execute_background_task(self, bckg_fut, *, background_task_status=None):
        """
        Monitors execution of a task in the background. Catches unhandled exceptions and
        maintains the status of the background task.

        Parameters
        ----------
        bckg_task: asyncio.Task
            The task scheduled to execute on the background
        background_task_status: dict or None (optional)
            Reference to a dictionary that may be used to monitor the status of the task.
            Initialize the parameter with an empty dictionary and poll the dictionary for
            the following keys: ``"task"`` (reference to the task, may be used to cancel
            the task), ``"status"`` (values ``"running"``, ``"success"``, ``"failed"``)
            and ``"err_msg"`` (error message or an empty string).

        Examples
        --------

        .. code-block:: python

            # Call in the loop
            asyncio.ensure_future(self._execute_background_task(some_coroutine()))
        """

        if background_task_status is None:
            background_task_status = {}
        background_task_status.update({"task": None, "status": "running", "err_msg": ""})

        try:
            background_task = asyncio.ensure_future(bckg_fut)
            background_task_status["task"] = background_task
            await background_task
            success, err_msg = background_task.result()
        except Exception as ex:
            success = False
            err_msg = f"Unhandled exception: {str(ex)}"
            logger.exception("Unhandled exception during background task execution: %s", ex)

        background_task_status.update({"status": "success" if success else "failed", "err_msg": err_msg})

    async def _start_re_worker(self):
        """
        Initiate creation of RE Worker environment. The function does not wait until
        the environment is created. Returns True/False depending on whether
        the command is accepted.
        """
        try:
            if self._environment_exists:
                raise RuntimeError("RE Worker environment already exists.")
            elif self._manager_state is MState.CREATING_ENVIRONMENT:
                raise RuntimeError("Manager is already in the process of creating the RE Worker environment.")
            elif self._manager_state is not MState.IDLE:
                raise RuntimeError(f"Manager state is not idle. Current state: {self._manager_state.value}")
            else:
                accepted, msg = True, ""
                self._manager_state = MState.CREATING_ENVIRONMENT

                asyncio.ensure_future(self._execute_background_task(self._start_re_worker_task()))

        except Exception as ex:
            accepted, msg = False, str(ex)

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
            logger.exception("Failed to start_Worker: %s", ex)
            success, err_msg = False, f"Failed to start_Worker {str(ex)}"

        self._manager_state = MState.IDLE

        if success:
            self._autostart_push()

        return success, err_msg

    async def _stop_re_worker(self):
        """
        Initiate closing of the RE Worker environment.
        """
        try:
            ip_kernel_is_busy, ws = False, self._worker_state_info
            if self._use_ipython_kernel and ws:
                # This is not 100% reliable. The request may still be rejected because of busy kernel.
                ip_kernel_is_busy = ws["ip_kernel_state"] == "busy"

            if not self._environment_exists:
                raise RuntimeError("RE Worker environment does not exist.")
            elif self._manager_state is MState.CLOSING_ENVIRONMENT:
                raise RuntimeError("Manager is already in the process of closing the RE Worker environment.")
            elif self._manager_state in (MState.EXECUTING_QUEUE, MState.STARTING_QUEUE):
                raise RuntimeError("Queue execution is in progress.")
            elif self._manager_state is MState.EXECUTING_TASK:
                raise RuntimeError("Foreground task execution is in progress.")
            elif self._manager_state != MState.IDLE:
                raise RuntimeError(f"Manager state is not idle. Current state: {self._manager_state.value}")
            elif ip_kernel_is_busy:
                raise RuntimeError("Worker IPython kernel is busy")
            else:
                accepted, err_msg = True, ""
                self._manager_state = MState.CLOSING_ENVIRONMENT

                asyncio.ensure_future(self._execute_background_task(self._stop_re_worker_task()))

        except Exception as ex:
            accepted, err_msg = False, str(ex)

        return accepted, err_msg

    async def _stop_re_worker_task(self):
        """
        Closing the RE Worker environment.
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
            self._event_worker_closed = asyncio.Event()  # !!

            self._manager_state = MState.CLOSING_ENVIRONMENT
            await self._fut_manager_task_completed  # TODO: timeout may be needed here

            if not await self._confirm_re_worker_exit():
                success = False
                err_msg = "Failed to confirm closing of RE Worker thread"
            else:
                await self._task_results.clear_running_tasks()
        else:
            logger.error("Environment can not be closed: %s", err_msg)

        self._manager_state = MState.IDLE
        self._running_task_uid = None

        return success, err_msg

    async def _kill_re_worker(self):
        """
        Kill the process in which RE worker is running.
        """
        if self._environment_exists or (self._manager_state == MState.CREATING_ENVIRONMENT):
            accepted, err_msg = True, ""
            # Cancel any operation of opening/closing the environment
            if self._fut_manager_task_completed and not self._fut_manager_task_completed.done():
                self._fut_manager_task_completed.set_result(False)
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
        await self._plan_queue.set_processed_item_as_stopped(
            exit_status="failed",
            run_uids=[],
            scan_ids=[],
            err_msg="RE Worker environment was destroyed",
            err_tb="",
        )

        err_msg = "" if success else "Failed to properly destroy RE Worker environment."
        logger.info("RE Worker environment is destroyed")
        if not success:
            logger.error(err_msg)
        else:
            await self._task_results.clear_running_tasks()

        self._running_task_uid = None

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

                    if ws["plans_and_devices_list_updated"]:
                        self._loop.create_task(self._load_existing_plans_and_devices_from_worker())

                    if ws["completed_tasks_available"]:
                        self._loop.create_task(self._load_task_results_from_worker())

                    if ws["unexpected_shutdown"]:
                        # Shutdown was not requested by the manager (caused by external client).
                        self._loop.create_task(self._confirm_re_worker_exit())

                    if not self._exec_loop_deactivated_event.is_set() and not ws["ip_kernel_captured"]:
                        # Expected to be used only if IPython kernel is used.
                        self._exec_loop_deactivated_event.set()

                    if self._manager_state == MState.CLOSING_ENVIRONMENT:
                        if ws["environment_state"] == "closing":
                            self._fut_manager_task_completed.set_result(True)

                    elif self._manager_state == MState.CREATING_ENVIRONMENT:
                        # If RE Worker environment fails to open, then it switches to 'closing' state.
                        #   Closing must be confirmed by Manager before it is closed.
                        done = not self._use_ipython_kernel or not ws["ip_kernel_captured"]
                        if done and ws["environment_state"] in ("idle", "executing_plan", "executing_task"):
                            self._fut_manager_task_completed.set_result(True)
                        if done and ws["environment_state"] == "closing":
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
            await self._plan_queue.set_processed_item_as_stopped(
                exit_status="failed",
                run_uids=[],
                scan_ids=[],
                err_msg="Internal RE Manager error occurred. Report the error to the development team",
                err_tb="",
            )
            self._manager_state = MState.IDLE
            self._re_pause_pending = False
        else:
            plan_state = plan_report["plan_state"]
            success = plan_report["success"]
            result = plan_report["result"]
            uids = plan_report["uids"]
            scan_ids = plan_report["scan_ids"]
            err_msg = plan_report["err_msg"]
            err_tb = plan_report["traceback"]
            stop_queue = plan_report["stop_queue"]  # Worker tells the manager to stop the queue

            msg_display = result if result else err_msg
            logger.debug(
                "Report received from RE Worker:\nplan_state=%s\nsuccess=%s\n%s\n)",
                plan_state,
                success,
                msg_display,
            )

            ignore_failures = self._plan_queue.plan_queue_mode["ignore_failures"]
            continue_failed = (plan_state == "failed") and ignore_failures

            if plan_state in ("completed", "unknown") or continue_failed:
                # Check if the plan was running in the 'immediate_execution' mode.
                item = await self._plan_queue.get_running_item_info()
                immediate_execution = item.get("properties", {}).get("immediate_execution", False)

                # Executed plan is removed from the queue only after it is successfully completed.
                # If a plan was not completed or not successful (exception was raised), then
                # execution of the queue is stopped. It can be restarted later (failed or
                # interrupted plan will still be in the queue.
                await self._plan_queue.set_processed_item_as_completed(
                    exit_status=plan_state, run_uids=uids, scan_ids=scan_ids, err_msg=err_msg, err_tb=err_tb
                )
                await self._start_plan_task(stop_queue=stop_queue or bool(immediate_execution))
            elif plan_state in ("failed", "stopped", "aborted", "halted"):
                # Paused plan was stopped/aborted/halted
                await self._plan_queue.set_processed_item_as_stopped(
                    exit_status=plan_state, run_uids=uids, scan_ids=scan_ids, err_msg=err_msg, err_tb=err_tb
                )
                self._loop.create_task(self._set_manager_state(MState.IDLE, autostart_disable=True))
            elif plan_state == "paused":
                # The plan was paused (nothing should be done).
                self._loop.create_task(self._set_manager_state(MState.PAUSED))
            else:
                logger.error("Unknown plan state %s was returned by RE Worker.", plan_state)

    async def _set_manager_state(self, state, *, coro=None, autostart_disable=False):
        """
        Set manager state to ``MState.IDLE`` or ``MState.PAUSE``. When using IPython kernel,
        the manager should wait for the execution loop to be stopped before setting the state.
        """

        if state not in (MState.IDLE, MState.PAUSED):
            raise ValueError(
                f"Attempting to set invalid state: {state!r}. " "Only 'idle' or 'paused' states are allowed."
            )

        if self._use_ipython_kernel:
            await self._worker_command_exec_loop_stop()
            await self._worker_request_state()

            self._exec_loop_deactivated_event.clear()
            await self._exec_loop_deactivated_event.wait()

        self._manager_state = state
        self._re_pause_pending = False  # MState.EXECUTING_QUEUE
        self._running_task_uid = None  # MState.EXECUTING_TASK

        if state == MState.IDLE:
            await self._queue_stop_deactivate()  # MState.EXECUTING_QUEUE
        if autostart_disable:
            await self._autostart_disable()

        if coro:
            await coro()

    async def _download_run_list(self):
        """
        Download the list of currently available runs when it is updated by the worker
        """
        # Read report first
        run_list, err_msg = await self._worker_request_run_list()
        if run_list is None:
            # TODO: this would typically mean a bug (communication error). Probably more
            #       complicated processing is needed
            logger.error("Failed to download plan report: %s.", err_msg)
        else:
            self._re_run_list = run_list["run_list"]
            self._re_run_list_uid = _generate_uid()

    def _set_existing_plans_and_devices(self, *, existing_plans, existing_devices, always_update_uids=False):
        """
        Sets the lists of existing plans and devices and updates UIDs if necessary.
        """
        # First update UIDs if necessary.
        try:
            if always_update_uids or (existing_plans != self._existing_plans):
                self._existing_plans_uid = _generate_uid()
        except Exception as ex:
            logger.warning("Failed to compare lists of existing plans: %s", ex)
            self._existing_plans_uid = _generate_uid()

        try:
            if always_update_uids or (existing_devices != self._existing_devices):
                self._existing_devices_uid = _generate_uid()
        except Exception as ex:
            logger.warning("Failed to compare lists of existing devices: %s", ex)
            self._existing_devices_uid = _generate_uid()

        # Now update the references
        self._existing_plans = existing_plans
        self._existing_devices = existing_devices

    async def _load_existing_plans_and_devices_from_worker(self):
        """
        Download the updated list of existing plans and devices from the worker environment.
        User group permissions are also downloaded from the worker and could be updated if needed.
        """
        logger.info("Downloading the lists of existing plans and devices from the worker environment")
        plan_and_devices_list, err_msg = await self._worker_request_plans_and_devices_list()
        if plan_and_devices_list is None:
            # TODO: this would typically mean a bug (communication error). Probably more
            #       complicated processing is needed
            logger.error(
                "Failed to download the list of existing plans and devices from the worker process: %s", err_msg
            )
        else:
            self._set_existing_plans_and_devices(
                existing_plans=plan_and_devices_list["existing_plans"],
                existing_devices=plan_and_devices_list["existing_devices"],
            )

            try:
                self._generate_lists_of_allowed_plans_and_devices()
            except Exception as ex:
                logger.exception("Failed to compute the list of allowed plans and devices: %s", ex)

    async def _load_task_results_from_worker(self):
        """
        Download results of the completed tasks from worker process.
        """
        logger.debug("Downloading the results of completed tasks from the worker environment.")
        results, err_msg = await self._worker_request_task_results()
        if results is None:
            # TODO: this would typically mean a bug (communication error). Probably more
            #       complicated processing is needed
            logger.error("Failed to download the results of completed tasks from the worker process: %s", err_msg)
        else:
            task_results = results["task_results"]
            for task_res in task_results:
                if "task_uid" not in task_res:
                    logger.error("Missing 'task_uid' data in task results: %s", ppfl(task_res))
                    continue

                task_uid = task_res["task_uid"]

                def factory(*, task_uid, task_res):
                    async def inner():
                        await self._task_results.add_completed_task(task_uid=task_uid, payload=task_res)

                    return inner

                coro = factory(task_uid=task_uid, task_res=task_res)

                if (self._manager_state == MState.EXECUTING_TASK) and (task_uid == self._running_task_uid):
                    self._loop.create_task(self._set_manager_state(MState.IDLE, coro=coro))
                else:
                    await coro()

                logger.debug("Loaded the results for task '%s': %s", task_uid, ppfl(task_results))

    async def _start_plan(self):
        """
        Initiate upload of next plan to the worker process for execution.
        """
        try:
            if not self._environment_exists:
                raise RuntimeError("RE Worker environment does not exist.")
            elif self._manager_state != MState.IDLE:
                raise RuntimeError("RE Manager is busy.")
            elif self._use_ipython_kernel and self._is_ipkernel_external_task():
                raise RuntimeError("IPython kernel (RE Worker) is busy.")
            else:
                # Attempt to reserve IPython kernel.
                _success, _msg = await self._worker_command_reserve_kernel()
                if not _success:
                    raise RuntimeError(f"Failed to capture IPython kernel: {_msg}")

                await self._queue_stop_deactivate()  # Just in case
                self._manager_state = MState.STARTING_QUEUE
                asyncio.ensure_future(self._execute_background_task(self._start_plan_task()))
                success, err_msg = True, ""

        except Exception as ex:
            success, err_msg = False, str(ex)

        return success, err_msg

    async def _start_single_plan(self, *, item):
        """
        Initiate upload of next plan to the worker process for execution.
        """
        qsize = None

        try:
            if not self._environment_exists:
                raise RuntimeError("RE Worker environment does not exist.")
            elif self._manager_state != MState.IDLE:
                raise RuntimeError("RE Manager is busy.")
            elif self._use_ipython_kernel and self._is_ipkernel_external_task():
                raise RuntimeError("IPython kernel (RE Worker) is busy.")
            else:
                # Attempt to reserve IPython kernel.
                _success, _msg = await self._worker_command_reserve_kernel()
                if not _success:
                    raise RuntimeError(f"Failed to capture IPython kernel: {_msg}")

                await self._queue_stop_deactivate()  # Just in case
                self._manager_state = MState.STARTING_QUEUE

                item = self._plan_queue.set_new_item_uuid(item)
                qsize = await self._plan_queue.get_queue_size()

                asyncio.ensure_future(self._execute_background_task(self._start_plan_task(single_item=item)))

                success, err_msg = True, ""

        except Exception as ex:
            success, err_msg = False, str(ex)

        return success, err_msg, item, qsize

    async def _start_plan_task(self, stop_queue=False, autostart_disable=False, single_item=None):
        """
        Upload the plan to the worker process for execution.
        Plan in the queue is represented as a dictionary with the keys "name" (plan name),
        "args" (list of args), "kwargs" (list of kwargs). Only the plan name is mandatory.
        Names of plans and devices are strings.

        ``single_item`` is an item, which is immediately executed (bypassing the queue).
        """

        start_next_plan = False
        immediate_execution = bool(single_item)

        # Check if the queue should be stopped and stop the queue
        if not immediate_execution:
            n_pending_plans = await self._plan_queue.get_queue_size()
            if n_pending_plans:
                logger.info("Processing the next queue item: %d plans are left in the queue.", n_pending_plans)
            else:
                logger.info("No items are left in the queue.")

            if self.queue_stop_pending or stop_queue:
                autostart_disable = self.queue_stop_pending or autostart_disable
                self._loop.create_task(self._set_manager_state(MState.IDLE, autostart_disable=autostart_disable))
                success, err_msg = False, "Queue is stopped."
                logger.info(err_msg)

            elif not n_pending_plans:
                self._loop.create_task(self._set_manager_state(MState.IDLE))
                success, err_msg = False, "Queue is empty."
                logger.info(err_msg)

            elif self._re_pause_pending:
                self._loop.create_task(self._set_manager_state(MState.IDLE))
                success, err_msg = False, "Queue is stopped due to unresolved outstanding RE pause request."
                logger.info(err_msg)

            else:
                start_next_plan = True
        else:
            logger.info("Processing the plan submitted for immediate execution ...")
            start_next_plan = True

        # It is decided that the next plan should be started
        if start_next_plan:
            next_item = single_item if immediate_execution else await self._plan_queue.get_item(pos="front")

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

                new_plan = await self._plan_queue.process_next_item(item=single_item)

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
                logger.info("Starting the plan:\n%s.", ppfl(plan_info))

                success, err_msg = await self._worker_command_run_plan(plan_info)
                if not success:
                    await self._plan_queue.set_processed_item_as_stopped(
                        exit_status="failed", run_uids=[], scan_ids=[], err_msg=err_msg, err_tb=""
                    )
                    self._manager_state = MState.IDLE
                    logger.error("Failed to start the plan %s.\nError: %s", ppfl(plan_info), err_msg)
                    err_msg = f"Failed to start the plan: {err_msg}"
                else:
                    self._manager_state = MState.EXECUTING_QUEUE

            # The next items is INSTRUCTION
            elif next_item["item_type"] == "instruction":
                logger.info("Executing instruction:\n%s.", ppfl(next_item))

                if next_item["name"] == "queue_stop":
                    await self._plan_queue.process_next_item(item=single_item)
                    self._manager_state = MState.EXECUTING_QUEUE
                    asyncio.ensure_future(self._start_plan_task(stop_queue=True, autostart_disable=True))
                    success, err_msg = True, ""
                else:
                    success = False
                    err_msg = f"Unsupported action: '{next_item['name']}' (item {next_item})"

            else:
                success = False
                err_msg = f"Unrecognized item type: '{next_item['item_type']}' (item {next_item})"

        return success, err_msg

    async def _queue_stop_activate(self):
        if self._manager_state not in (MState.EXECUTING_QUEUE, MState.STARTING_QUEUE):
            msg = f"Failed to pause the queue. Queue is not running. Manager state: {self._manager_state}"
            return False, msg
        else:
            await self.set_queue_stop_pending(True)
            return True, ""

    async def _queue_stop_deactivate(self):
        await self.set_queue_stop_pending(False)
        return True, ""

    async def _pause_run_engine(self, option):
        """
        Pause execution of a running plan. Run Engine must be in 'running' state in order for
        the request to pause to be accepted by RE Worker.
        """
        if not self._environment_exists:
            success = False
            err_msg = "Environment does not exist."
        else:
            success, err_msg = await self._worker_command_pause_plan(option)

        if not success:
            logger.error("Failed to pause Run Engine: %s", err_msg)

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

        elif self._use_ipython_kernel and self._is_ipkernel_external_task():
            raise RuntimeError("IPython kernel (RE Worker) is busy.")

        elif self._environment_exists:
            # Attempt to reserve IPython kernel.
            success, err_msg = await self._worker_command_reserve_kernel()
            if success:
                success, err_msg = await self._worker_command_continue_plan(option)
            else:
                err_msg = f"Failed to capture IPython kernel: {err_msg}"

            success = bool(success)  # Convert 'None' to False
            if success:
                self._manager_state = MState.EXECUTING_QUEUE
            else:
                logger.error("Failed to continue or stop the running plan: %s", err_msg)

        else:
            success, err_msg = (
                False,
                "Environment does not exist. Can not pause Run Engine.",
            )

        return success, err_msg

    def _autostart_push(self):
        """
        Make autostart task immediately process the new item.
        """
        if self.queue_autostart_enabled and self._manager_state == MState.IDLE:
            self._queue_autostart_event.set()

    async def _autostart_task(self):
        """
        The task that is monitoring the queue in autostart mode and attempting to start
        the queue if it is not empty.
        """
        self._queue_autostart_event.clear()
        while True:
            queue_size = await self._plan_queue.get_queue_size()
            if not self.queue_autostart_enabled:
                break
            if queue_size and self._manager_state == MState.IDLE:
                success, err_msg = await self._start_plan()
                if not success:
                    logger.debug("Autostart: failed to start a plan: %s", err_msg)

            polling_period = 1

            try:
                await asyncio.wait_for(self._queue_autostart_event.wait(), timeout=polling_period)
            except asyncio.TimeoutError:
                pass

            self._queue_autostart_event.clear()

    async def _autostart_enable(self):
        """
        Enable autostart if it is not already enabled. The function always succeeds.
        """
        success, msg = True, ""
        if not self.queue_autostart_enabled:
            await self.set_queue_autostart_enabled(True)
            self._loop.create_task(self._autostart_task())

        return success, msg

    async def _autostart_disable(self):
        """
        Disable autostart if it is enabled. The function always succeeds.
        """
        success, msg = True, ""

        if self.queue_autostart_enabled:
            await self.set_queue_autostart_enabled(False)
            # Make the task quit as quickly as possible
            self._queue_autostart_event.set()

        return success, msg

    def _is_ipkernel_external_task(self):
        """
        Returns True if the worker exists, running in IPython mode and is currently busy executing
        task started by external client (e.g. Jupyter Console).
        """
        ws = self._worker_state_info
        return self._use_ipython_kernel and ws and ws["ip_kernel_state"] != "idle" and not ws["ip_kernel_captured"]

    async def _environment_upload_script(self, *, script, update_lists, update_re, run_in_background):
        """
        Upload Python script to RE Worker environment. The script is then executed into
        the worker namespace. The API call only inititiates the process of loading the
        script and return success if the request is accepted and the task can be started.
        Success does not mean that the script is successfully loaded.
        """
        if not self._environment_exists:
            success, err_msg, task_uid = False, "RE Worker environment is not open", None
        elif not run_in_background and (self._manager_state != MState.IDLE):
            success, err_msg, task_uid = (
                False,
                "Failed to start the task: RE Manager must be in idle state. "
                f"Current state: '{self._manager_state.value}'",
                None,
            )
        elif not run_in_background and self._is_ipkernel_external_task():
            raise RuntimeError("Failed to start the task: IPython kernel (RE Worker) is busy.")
        else:
            try:
                if not run_in_background:
                    self._manager_state = MState.EXECUTING_TASK

                    # Attempt to reserve IPython kernel.
                    _success, _msg = await self._worker_command_reserve_kernel()
                    if not _success:
                        raise RuntimeError(f"Failed to capture IPython kernel: {_msg}")

                success, err_msg, task_uid = await self._worker_command_load_script(
                    script=script,
                    update_lists=update_lists,
                    update_re=update_re,
                    run_in_background=run_in_background,
                )
                if not run_in_background:
                    self._running_task_uid = task_uid
            except Exception:
                success = False
                raise
            finally:
                if not success and not run_in_background:
                    self._manager_state = MState.IDLE

        return success, err_msg, task_uid

    async def _environment_function_execute(self, *, item, run_in_background):
        """
        Upload Python script to RE Worker environment. The script is then executed into
        the worker namespace. The API call only inititiates the process of loading the
        script and return success if the request is accepted and the task can be started.
        Success does not mean that the script is successfully loaded.
        """
        if not self._environment_exists:
            success, err_msg, task_uid = False, "RE Worker environment is not open", None
        elif not run_in_background and (self._manager_state != MState.IDLE):
            success, err_msg, task_uid = (
                False,
                "Failed to start the task: RE Manager must be in idle state. "
                f"Current state: '{self._manager_state.value}'",
                None,
            )
        elif not run_in_background and self._is_ipkernel_external_task():
            raise RuntimeError("Failed to start the task: IPython kernel (RE Worker) is busy.")
        else:
            try:
                if not run_in_background:
                    self._manager_state = MState.EXECUTING_TASK

                    # Attempt to reserve IPython kernel.
                    _success, _msg = await self._worker_command_reserve_kernel()
                    if not _success:
                        raise RuntimeError(f"Failed to capture IPython kernel: {_msg}")

                func_name = item["name"]
                args = item.get("args", [])
                kwargs = item.get("kwargs", {})
                user_name = item["user"]
                user_group = item["user_group"]
                item_uid = item["item_uid"]

                func_info = {
                    "name": func_name,
                    "args": args,
                    "kwargs": kwargs,
                    "user": user_name,
                    "user_group": user_group,
                    "item_uid": item_uid,
                }

                success, err_msg, item, task_uid = await self._worker_command_execute_function(
                    func_info=func_info, run_in_background=run_in_background
                )
                if not run_in_background:
                    self._running_task_uid = task_uid
            except Exception:
                success = False
                raise
            finally:
                if not success and not run_in_background:
                    self._manager_state = MState.IDLE

        return success, err_msg, item, task_uid

    def _generate_lists_of_allowed_plans_and_devices(self, *, always_update_uids=False):
        """
        Compute lists of allowed plans and devices based on the lists ``self._existing_plans``,
        ``self._existing_devices`` and user group permissions ``self._user_group_permissions``.

        The UIDS of the lists of allowed plans and devices are updated only if the computed
        lists are different from the existing ones or if ``always_update_uids`` is ``True``.
        """
        allowed_plans, allowed_devices = load_allowed_plans_and_devices(
            existing_plans=self._existing_plans,
            existing_devices=self._existing_devices,
            user_group_permissions=self._user_group_permissions,
        )

        try:
            if always_update_uids or (allowed_plans != self._allowed_plans):
                self._allowed_plans = allowed_plans
                self._allowed_plans_uid = _generate_uid()
        except Exception as ex:
            logger.error("Error occurred while comparing lists of allowed plans: %s", ex)

        try:
            if always_update_uids or (allowed_devices != self._allowed_devices):
                self._allowed_devices = allowed_devices
                self._allowed_devices_uid = _generate_uid()
        except Exception as ex:
            logger.error("Error occurred while comparing lists of allowed devices: %s", ex)

    async def _load_permissions_from_disk(self):
        """
        Load permissions from disk.
        """
        try:
            path_ug = self._config_dict["user_group_permissions_path"]
            self._user_group_permissions = load_user_group_permissions(path_ug)
            # Save loaded permissions to Redis
            await self._plan_queue.user_group_permissions_save(self._user_group_permissions)
        except Exception as ex:
            logger.exception("Error occurred while loading user permissions from file '%s': %s", path_ug, ex)

    async def _load_permissions_from_redis(self):
        """
        Load and validate user group permissions stored in Redis. If there is no permissions data in Redis
        or data validation fails, then attempt to load permissions from file. User group permissions are
        left unchanged if data can not be loaded from disk either.
        """
        try:
            ug_permissions = await self._plan_queue.user_group_permissions_retrieve()
            validate_user_group_permissions(ug_permissions)
            self._user_group_permissions = ug_permissions
        except Exception as ex:
            logger.error("Validation of user group permissions loaded from Redis failed: %s", ex)
            await self._load_permissions_from_disk()

    def _update_allowed_plans_and_devices(self, restore_plans_devices=False):
        """
        Update the lists of allowed plans and devices. UIDs for the lists of allowed plans and device
        are ALWAYS updated when this function is called. If ``restore_plans_devices`` is ``True``,
        then the list of existing plans and devices is reloaded from disk.
        """
        path_pd = self._config_dict["existing_plans_and_devices_path"]
        try:
            if restore_plans_devices:
                existing_plans, existing_devices = load_existing_plans_and_devices(path_pd)
                self._set_existing_plans_and_devices(
                    existing_plans=existing_plans,
                    existing_devices=existing_devices,
                    always_update_uids=True,
                )
            self._generate_lists_of_allowed_plans_and_devices(always_update_uids=True)
        except Exception as ex:
            raise Exception(
                f"Error occurred while loading lists of allowed plans and devices from '{path_pd}': {str(ex)}"
            )

    async def _save_lock_info_to_redis(self):
        try:
            logger.debug("Saving lock info to Redis ...")
            lock_info = self._lock_info.to_dict()
            await self._plan_queue.lock_info_save(lock_info)
        except Exception as ex:
            raise Exception(f"Error occurred while saving lock info to Redis: {ex}") from ex

    async def _load_lock_info_from_redis(self):
        try:
            logger.debug("Loading lock info from Redis ...")
            lock_info = await self._plan_queue.lock_info_retrieve()
            if lock_info:  # The lock info may or may not be saved to Redis.
                self._lock_info.from_dict(lock_info)
        except Exception as ex:
            raise Exception(f"Error occurred while loading lock info from Redis: {ex}") from ex

    async def _kernel_interrupt_send(self, *, interrupt_task, interrupt_plan):
        success, msg = True, ""
        try:
            if not self._use_ipython_kernel:
                raise RuntimeError("RE Manager is not in IPython mode: IPython kernel is not used")

            if not self._environment_exists:
                raise RuntimeError("Worker environment does not exist")

            if not interrupt_plan and self._manager_state in (MState.STARTING_QUEUE, MState.EXECUTING_QUEUE):
                raise RuntimeError("Not allowed to interrupt running plan")

            if not interrupt_task and self._manager_state == MState.EXECUTING_TASK:
                raise RuntimeError("Not allowed to interrupt running task")

            success, msg = await self._worker_command_kernel_interrupt(
                interrupt_task=interrupt_task, interrupt_plan=interrupt_plan
            )

        except Exception as ex:
            success, msg = False, f"Failed to interrupt IPython kernel: {str(ex)}"

        return success, msg

    # ===============================================================================
    #         Functions that send commands/request data from Worker process

    async def _worker_request_state(self):
        try:
            re_state = await self._comm_to_worker.send_msg("request_state")
            err_msg = ""
        except CommTimeoutError:
            re_state, err_msg = None, "Timeout occurred while processing the request"
        return re_state, err_msg

    async def _worker_request_ip_connect_info(self):
        try:
            ip_connect_info = await self._comm_to_worker.send_msg("request_ip_connect_info")
            err_msg = ""
        except CommTimeoutError:
            ip_connect_info, err_msg = {}, "Timeout occurred while processing the request"
        return ip_connect_info, err_msg

    async def _worker_request_plan_report(self):
        try:
            plan_report = await self._comm_to_worker.send_msg("request_plan_report")
            err_msg = ""
            if plan_report is None:
                err_msg = "Report is not available at RE Worker"
        except CommTimeoutError:
            plan_report, err_msg = None, "Timeout occurred while processing the request"
        return plan_report, err_msg

    async def _worker_request_run_list(self):
        try:
            run_list = await self._comm_to_worker.send_msg("request_run_list")
            err_msg = ""
            if run_list is None:
                err_msg = "Failed to obtain the run list from the worker"
        except CommTimeoutError:
            run_list, err_msg = None, "Timeout occurred while processing the request"
        return run_list, err_msg

    async def _worker_request_plans_and_devices_list(self):
        try:
            plans_and_devices_list = await self._comm_to_worker.send_msg("request_plans_and_devices_list")
            err_msg = ""
            if plans_and_devices_list is None:
                err_msg = "Failed to obtain the run list from the worker"
        except CommTimeoutError:
            plans_and_devices_list, err_msg = None, "Timeout occurred while processing the request"
        return plans_and_devices_list, err_msg

    async def _worker_request_task_results(self):
        try:
            tt = self._comm_to_worker_timeout_long
            results = await self._comm_to_worker.send_msg("request_task_results", timeout=tt)
            err_msg = ""
            if results is None:
                err_msg = "Failed to obtain the results of completed tasks from the worker"
        except CommTimeoutError:
            results, err_msg = None, "Timeout occurred while processing the request"
        return results, err_msg

    async def _worker_command_close_env(self):
        try:
            response = await self._comm_to_worker.send_msg("command_close_env")
            success = response["status"] == "accepted"
            err_msg = response["err_msg"]
        except CommTimeoutError:
            success, err_msg = None, "Timeout occurred while processing the request"
        return success, err_msg

    async def _worker_command_confirm_exit(self):
        try:
            response = await self._comm_to_worker.send_msg("command_confirm_exit")
            success = response["status"] == "accepted"
            err_msg = response["err_msg"]
        except CommTimeoutError:
            success, err_msg = None, "Timeout occurred while processing the request"
        return success, err_msg

    async def _worker_command_reserve_kernel(self):
        """
        Reserve the kernel (start execution loop in the kernel) if the worker is
        running IPython kernel. Return ``True`` if the worker is not using IPython
        kernel (execution loop is continuously running in this case, so the 'kernel'
        can always be considered reserved).
        """
        try:
            if self._use_ipython_kernel:
                response = await self._comm_to_worker.send_msg("command_reserve_kernel")
                success = response["status"] == "accepted"
                err_msg = response["err_msg"]
            else:
                success, err_msg = True, ""
        except CommTimeoutError:
            success, err_msg = None, "Timeout occurred while processing the request"
        return success, err_msg

    async def _worker_command_exec_loop_stop(self):
        """
        Initiate stopping the execution loop. Call fails if the worker is running on Python
        (not IPython kernel).
        """
        try:
            response = await self._comm_to_worker.send_msg("command_exec_loop_stop")
            success = response["status"] == "accepted"
            err_msg = response["err_msg"]
        except CommTimeoutError:
            success, err_msg = None, "Timeout occurred while processing the request"
        return success, err_msg

    async def _worker_command_run_plan(self, plan_info):
        try:
            tt = self._comm_to_worker_timeout_long
            response = await self._comm_to_worker.send_msg(
                "command_run_plan", {"plan_info": plan_info}, timeout=tt
            )
            success = response["status"] == "accepted"
            err_msg = response["err_msg"]
        except CommTimeoutError:
            success, err_msg = None, "Timeout occurred while processing the request"
        return success, err_msg

    async def _worker_command_pause_plan(self, option):
        try:
            response = await self._comm_to_worker.send_msg("command_pause_plan", {"option": option})
            success = response["status"] == "accepted"
            err_msg = response["err_msg"]
        except CommTimeoutError:
            success, err_msg = None, "Timeout occurred while processing the request"
        return success, err_msg

    async def _worker_command_continue_plan(self, option):
        try:
            response = await self._comm_to_worker.send_msg("command_continue_plan", {"option": option})
            success = response["status"] == "accepted"
            err_msg = response["err_msg"]
        except CommTimeoutError:
            success, err_msg = None, "Timeout occurred while processing the request"
        return success, err_msg

    async def _worker_command_reset_worker(self):
        try:
            response = await self._comm_to_worker.send_msg("command_reset_worker")
            success = response["status"] == "accepted"
            err_msg = response["err_msg"]
        except CommTimeoutError:
            success, err_msg = None, "Timeout occurred while processing the request"
        return success, err_msg

    async def _worker_command_permissions_reload(self):
        try:
            response = await self._comm_to_worker.send_msg(
                "command_permissions_reload", params={"user_group_permissions": self._user_group_permissions}
            )
            success = response["status"] == "accepted"
            err_msg = response["err_msg"]
            task_uid = response["task_uid"]
            payload = response["payload"]
            if success:
                await self._task_results.add_running_task(task_uid=task_uid, payload=payload)
        except CommTimeoutError:
            success, err_msg = None, "Timeout occurred while processing the request"
        return success, err_msg

    async def _worker_command_load_script(self, *, script, update_lists, update_re, run_in_background):
        try:
            tt = self._comm_to_worker_timeout_long
            response = await self._comm_to_worker.send_msg(
                "command_load_script",
                params={
                    "script": script,
                    "update_lists": update_lists,
                    "update_re": update_re,
                    "run_in_background": run_in_background,
                },
                timeout=tt,
            )
            success = response["status"] == "accepted"
            err_msg = response["err_msg"]
            task_uid = response["task_uid"]
            payload = response["payload"]
            if success:
                await self._task_results.add_running_task(task_uid=task_uid, payload=payload)
        except CommTimeoutError:
            success, err_msg, task_uid = None, "Timeout occurred while processing the request", None
        return success, err_msg, task_uid

    async def _worker_command_execute_function(self, *, func_info, run_in_background):
        try:
            tt = self._comm_to_worker_timeout_long
            response = await self._comm_to_worker.send_msg(
                "command_execute_function",
                params={"func_info": func_info, "run_in_background": run_in_background},
                timeout=tt,
            )
            success = response["status"] == "accepted"
            err_msg = response["err_msg"]
            task_uid = response["task_uid"]
            payload = response["payload"]
            if success:
                await self._task_results.add_running_task(task_uid=task_uid, payload=payload)
        except CommTimeoutError:
            success, err_msg, task_uid = (None, "Timeout occurred while processing the request", None)
        return success, err_msg, func_info, task_uid

    async def _worker_command_kernel_interrupt(self, *, interrupt_task, interrupt_plan):
        try:
            response = await self._comm_to_worker.send_msg(
                "command_interrupt_kernel",
                params={"interrupt_task": interrupt_task, "interrupt_plan": interrupt_plan},
            )
            success = response["status"] == "accepted"
            err_msg = response["err_msg"]
        except CommTimeoutError:
            success, err_msg = (None, "Timeout occurred while processing the request")
        return success, err_msg

    # ===============================================================================
    #         Functions that send commands/request data from Watchdog process

    async def _watchdog_enable(self):
        """
        Enable watchdog once manager initialization is complete. Always succeeds (returns ``True``).
        """
        try:
            response = await self._comm_to_watchdog.send_msg("watchdog_enable")
            success = response["success"]
        except CommTimeoutError:
            success = False
        # TODO: add processing of CommJsonRpcError and RuntimeError to all handlers !!!
        return success

    async def _watchdog_start_re_worker(self):
        """
        Initiate the startup of the RE Worker. Returned 'success==True' means that the process
        was created successfully and RE environment initialization is started.
        """
        try:
            response = await self._comm_to_watchdog.send_msg(
                "start_re_worker", params={"user_group_permissions": self._user_group_permissions}
            )
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
        response_msg = f"RE Manager v{qserver_version}"
        items_in_queue = n_pending_items
        items_in_history = n_items_in_history
        running_item_uid = running_item_info["item_uid"] if running_item_info else None
        manager_state = self._manager_state.value
        queue_stop_pending = self.queue_stop_pending
        queue_autostart_enabled = self.queue_autostart_enabled
        worker_environment_exists = self._environment_exists
        re_state = self._worker_state_info["re_state"] if self._worker_state_info else None
        ip_kernel_state = self._worker_state_info["ip_kernel_state"] if self._worker_state_info else None
        ip_kernel_captured = self._worker_state_info["ip_kernel_captured"] if self._worker_state_info else None
        env_state = self._worker_state_info["environment_state"] if self._worker_state_info else "closed"
        background_tasks = self._worker_state_info["background_tasks_num"] if self._worker_state_info else 0
        deferred_pause_pending = self._re_pause_pending
        run_list_uid = self._re_run_list_uid
        plan_queue_uid = self._plan_queue.plan_queue_uid
        plan_history_uid = self._plan_queue.plan_history_uid
        devices_existing_uid = self._existing_devices_uid
        plans_existing_uid = self._existing_plans_uid
        devices_allowed_uid = self._allowed_devices_uid
        plans_allowed_uid = self._allowed_plans_uid
        plan_queue_mode = self._plan_queue.plan_queue_mode
        task_results_uid = self._task_results.task_results_uid
        lock_info_uid = self._lock_info.uid
        locked_environment = self._lock_info.environment
        locked_queue = self._lock_info.queue
        # worker_state_info = self._worker_state_info

        # TODO: consider different levels of verbosity for ping or other command to
        #       retrieve detailed status.
        msg = {
            "msg": response_msg,
            "items_in_queue": items_in_queue,
            "items_in_history": items_in_history,
            "running_item_uid": running_item_uid,
            "manager_state": manager_state,
            "queue_stop_pending": queue_stop_pending,
            "queue_autostart_enabled": queue_autostart_enabled,
            "worker_environment_exists": worker_environment_exists,
            "worker_environment_state": env_state,  # State of the worker environment
            "worker_background_tasks": background_tasks,  # The number of background tasks
            "re_state": re_state,  # State of Run Engine
            "ip_kernel_state": ip_kernel_state,  # State of IPython kernel
            "ip_kernel_captured": ip_kernel_captured,
            "pause_pending": deferred_pause_pending,  # True/False - Cleared once pause processed
            # If Run List UID change, download the list of runs for the current plan.
            # Run List UID is updated when the list is cleared as well.
            "run_list_uid": run_list_uid,
            "plan_queue_uid": plan_queue_uid,
            "plan_history_uid": plan_history_uid,
            "devices_existing_uid": devices_existing_uid,
            "plans_existing_uid": plans_existing_uid,
            "devices_allowed_uid": devices_allowed_uid,
            "plans_allowed_uid": plans_allowed_uid,
            "plan_queue_mode": plan_queue_mode,
            "task_results_uid": task_results_uid,
            "lock_info_uid": lock_info_uid,
            "lock": {"environment": locked_environment, "queue": locked_queue},
            # "worker_state_info": worker_state_info
        }
        return msg

    async def _config_get_handler(self, request):
        """
        Returns config information.
        """
        success, msg = True, ""
        try:
            supported_param_names = []
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            if self._use_ipython_kernel and self._environment_exists:
                payload, msg = await self._worker_request_ip_connect_info()
                ip_connect_info = payload.get("ip_connect_info", {})
            else:
                ip_connect_info = {}

            config = {
                "ip_connect_info": ip_connect_info,
            }
        except Exception as ex:
            success, msg, config = False, str(ex), {}

        return {"success": success, "msg": msg, "config": config}

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

            plans_allowed, plans_allowed_uid = self._allowed_plans[user_group], self._allowed_plans_uid
            success, msg = True, ""
        except Exception as ex:
            plans_allowed, plans_allowed_uid = {}, None
            success, msg = False, str(ex)

        return {
            "success": success,
            "msg": msg,
            "plans_allowed": plans_allowed,
            "plans_allowed_uid": plans_allowed_uid,
        }

    async def _plans_existing_handler(self, request):
        """
        Returns the list of existing plans.
        """
        logger.info("Returning the list of existing plans ...")

        try:
            supported_param_names = []
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            plans_existing, plans_existing_uid = self._existing_plans, self._existing_plans_uid
            success, msg = True, ""
        except Exception as ex:
            plans_existing, plans_existing_uid = {}, None
            success, msg = False, str(ex)

        return {
            "success": success,
            "msg": msg,
            "plans_existing": plans_existing,
            "plans_existing_uid": plans_existing_uid,
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

            devices_allowed, devices_allowed_uid = self._allowed_devices[user_group], self._allowed_devices_uid
            success, msg = True, ""
        except Exception as ex:
            devices_allowed, devices_allowed_uid = {}, None
            success, msg = False, str(ex)

        return {
            "success": success,
            "msg": msg,
            "devices_allowed": devices_allowed,
            "devices_allowed_uid": devices_allowed_uid,
        }

    async def _devices_existing_handler(self, request):
        """
        Returns the list of existing devices.
        """
        logger.info("Returning the list of existing devices ...")

        try:
            supported_param_names = []
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            devices_existing = self._existing_devices
            devices_existing_uid = self._existing_devices_uid
            success, msg = True, ""
        except Exception as ex:
            devices_existing, devices_existing_uid = {}, None
            success, msg = False, str(ex)

        return {
            "success": success,
            "msg": msg,
            "devices_existing": devices_existing,
            "devices_existing_uid": devices_existing_uid,
        }

    async def _permissions_reload_handler(self, request):
        """
        Reloads user group permissions from the default location or the location set using command line
        parameters. UIDs of the lists of allowed plans and devices are always changed if the operation is
        successful even if the contents of the lists remain the same. By default, the function is using
        the current lists of existing plans and devices, which may or may not match the contents of
        the file on disk. If optional parameter ``restore_plans_devices`` is ``True``, then the list
        of existing plans and devices are loaded from disk file.
        """
        logger.info("Reloading lists of allowed plans and devices ...")
        try:
            supported_param_names = ["restore_plans_devices", "restore_permissions", "lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_queue=True)

            # Do not reload the lists of existing plans and devices from disk file by default
            restore_plans_devices = request.get("restore_plans_devices", False)
            restore_permissions = request.get("restore_permissions", True)

            if restore_permissions and (
                self._user_group_permissions_reload_option not in ("ON_REQUEST", "ON_STARTUP")
            ):
                raise RuntimeError(
                    "Restoring user group permissions from disk is not allowed: RE Manager was started "
                    f"with option user_group_permissions_reload={self._user_group_permissions_reload_option!r}",
                )

            if restore_permissions:
                await self._load_permissions_from_disk()
            else:
                await self._load_permissions_from_redis()
            self._update_allowed_plans_and_devices(restore_plans_devices=restore_plans_devices)

            # If environment exists, then tell the worker to reload permissions. This is optional.
            if self._environment_exists:
                success_opt, msg_opt = await self._worker_command_permissions_reload()
                if not success_opt:
                    logger.warning("Permissions failed to reload by RE Worker process: %s", msg_opt)

            success, msg = True, ""
        except Exception as ex:
            success = False
            msg = f"Error: {str(ex)}"
        return {"success": success, "msg": msg}

    async def _permissions_set_handler(self, request):
        logger.info("Request to set user group permission ...")
        try:
            supported_param_names = ["user_group_permissions", "lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_queue=True)

            if "user_group_permissions" not in request:
                raise KeyError("Parameter 'user_group_permissions' is missing")

            user_group_permissions = request["user_group_permissions"]

            if user_group_permissions != self._user_group_permissions:
                validate_user_group_permissions(user_group_permissions)
                self._user_group_permissions = user_group_permissions
                await self._plan_queue.user_group_permissions_save(self._user_group_permissions)
                self._update_allowed_plans_and_devices()

                # If environment exists, then tell the worker to reload permissions. This is optional.
                if self._environment_exists:
                    success_opt, msg_opt = await self._worker_command_permissions_reload()
                    if not success_opt:
                        logger.warning("Permissions failed to reload by RE Worker process: %s", msg_opt)

            success, msg = True, ""
        except Exception as ex:
            success = False
            msg = f"Error: {str(ex)}"

        return {"success": success, "msg": msg}

    async def _permissions_get_handler(self, request):
        """
        Returns the current user group permissions (dictionary). The method is expected to always succeed.
        """
        logger.info("Request to get information on current user group permission ...")
        try:
            supported_param_names = []
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            user_group_permissions = copy.deepcopy(self._user_group_permissions)
            success, msg = True, ""
        except Exception as ex:
            user_group_permissions = {}
            success = False
            msg = f"Error: {str(ex)}"

        return {"success": success, "msg": msg, "user_group_permissions": user_group_permissions}

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

    def _get_item_from_request(self, *, request=None, item=None, supported_item_types=None):
        """
        Extract ``item`` and ``item_type`` from the request, validate the values and report errors
        """
        msg_prefix = "Incorrect request format: "
        supported_item_types = supported_item_types or ("plan", "instruction")

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
                    f"{msg_prefix}unsupported 'item_type' value: '{item_type}', "
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
            item type (``plan``, ``instruction`` or ``function``)
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
        elif item_type == "function":
            if "name" not in item:
                success, msg = False, f"Function name is not specified: {item}"
            else:
                func_name = item["name"]
                success = False
                if ("args" in item) and not isinstance(item["args"], (list, tuple)):
                    msg = f"Parameter 'args' is not a tuple or a list: {type(item['args'])}"
                elif ("kwargs" in item) and not isinstance(item["kwargs"], dict):
                    msg = f"Parameter 'kwargs' is not a dictionary: {type(item['kwargs'])}"
                # Only check that file name is passed the checks based on the defined permissions
                elif not check_if_function_allowed(
                    func_name, group_name=user_group, user_group_permissions=self._user_group_permissions
                ):
                    msg = f"Function {func_name!r} is not allowed for users from {user_group!r} group."
                else:
                    success, msg = True, ""
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
        logger.debug("Request: %s", ppfl(request))

        success, msg = True, ""
        try:
            supported_param_names = ["mode", "lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_queue=True)

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
        logger.debug("Request: %s", ppfl(request))

        item_type, item, qsize, msg = None, None, None, ""

        try:
            supported_param_names = ["user_group", "user", "item", "pos", "before_uid", "after_uid", "lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_queue=True)

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
            self._autostart_push()

            success = True

        except Exception as ex:
            success = False
            msg = f"Failed to add an item: {str(ex)}"

        logger.info(self._generate_item_log_msg("Item added", success, item_type, item, qsize))

        rdict = {"success": success, "msg": msg, "qsize": qsize, "item": item}
        return rdict

    def validate_item_batch(self, items, user, user_group):
        logger.debug("Starting validation of item batch ...")

        success, items_prepared, results = True, [], []
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

        logger.debug("Validation of item batch completed: success=%s.", success)

        return success, items_prepared, results

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
        logger.debug("Request: %s", ppfl(request))

        success, msg, item_list, results, qsize = True, "", [], [], None

        try:
            supported_param_names = ["user_group", "user", "items", "pos", "before_uid", "after_uid", "lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_queue=True)

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
            success, items_prepared, results = await self._loop.run_in_executor(
                None, self.validate_item_batch, items, user, user_group
            )

            if len(results) != len(items) != len(items_prepared):
                # This error should never happen, but the message may be useful for debugging if it happens.
                raise Exception("Error in data processing algorithm occurred")

            if success:
                # 'success' may still change
                item_list, results, _, success = await self._plan_queue.add_batch_to_queue(
                    items_prepared, pos=pos, before_uid=before_uid, after_uid=after_uid
                )
                self._autostart_push()

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
            supported_param_names = ["user_group", "user", "item", "replace", "lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_queue=True)

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
            supported_param_names = ["pos", "uid", "lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_queue=True)

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
            supported_param_names = ["uids", "ignore_missing", "lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_queue=True)

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
            supported_param_names = ["pos", "uid", "pos_dest", "before_uid", "after_uid", "lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_queue=True)

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
            supported_param_names = ["uids", "pos_dest", "before_uid", "after_uid", "reorder", "lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_queue=True)

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
        If the request succeeds, the item is executed once. The item is never added to
        the queue and it is not pushed back into the queue in case its execution fails/stops.
        If the queue is in the *LOOP* mode, the executed item is not added to the back of
        the queue after completion. The API request does not alter the sequence of enqueued plans
        or change plan queue UID.

        The API is primarily intended for implementing of interactive workflows, in which
        users are controlling the experiment using client GUI application and user actions
        (such as mouse click on a plot) are converted into the requests to execute plans
        in RE Worker environment. Interactive workflows may be used for calibration of
        the instrument, while the queue may be used to run sequences of scheduled experiments.

        If the item is a plan, the results of execution are added to plan history as usual.
        The respective history item could be accessed to check if the plan was executed
        successfully.

        The API DOES NOT START EXECUTION OF THE QUEUE. Once execution of the submitted
        item is finished, RE Manager is switched to the IDLE state.
        """
        logger.info("Starting immediate executing a queue item ...")
        logger.debug("Request: %s", ppfl(request))

        item_type, item, qsize, msg = None, None, None, ""

        try:
            supported_param_names = ["item", "user_group", "user", "lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_environment=True)

            item, item_type, _success, _msg = self._get_item_from_request(request=request)
            if not _success:
                raise Exception(_msg)

            user, user_group = self._get_user_info_from_request(request=request)

            # Always generate a new UID for the added plan!!!
            item, _ = self._prepare_item(
                item=item, item_type=item_type, user=user, user_group=user_group, generate_new_uid=True
            )

            success, msg, item, qsize = await self._start_single_plan(item=item)
            if not success:
                raise RuntimeError(msg)

        except Exception as ex:
            success, msg = False, f"Failed to start execution of the item: {str(ex)}"

        logger.info(self._generate_item_log_msg("Item execution started", success, item_type, item, qsize))

        rdict = {"success": success, "msg": msg, "qsize": qsize, "item": item}
        return rdict

    async def _queue_clear_handler(self, request):
        """
        Remove all entries from the plan queue (does not affect currently executed run)
        """
        logger.info("Clearing the queue ...")
        try:
            supported_param_names = ["lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_queue=True)

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
            supported_param_names = ["lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_queue=True)

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
            supported_param_names = ["lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_environment=True)

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
            supported_param_names = ["lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_environment=True)

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
            supported_param_names = ["lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_environment=True)

            success, msg = await self._kill_re_worker()
        except Exception as ex:
            success, msg = False, f"Error: {ex}"

        return {"success": success, "msg": msg}

    async def _environment_update_handler(self, request):
        """
        Update the environment (lists of existing and allowed plans and devices and RE) based
        on the currents contents of the worker namespace. The namespace could be changed
        by uploading scripts (``script_upload`` API), running a function (``function_execute``)
        or executing commands using Jupyter Console (only IPython kernel mode). This API
        creates new lists of existing plans and devices and updates cached reference to RE object.

        By default, the operation is performed as a foreground task and the API call fails
        unless RE Manager is idle. To run the update in the background thread, call the API with
        ``run_in_background=True``.
        """
        logger.info("Updating RE environment ...")
        try:
            supported_param_names = ["run_in_background", "lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_environment=True)

            run_in_background = request.get("run_in_background", False)

            success, msg, task_uid = await self._environment_upload_script(
                script="", update_lists=True, update_re=True, run_in_background=run_in_background
            )

        except Exception as ex:
            success, msg, task_uid = False, f"Error: {ex}", None

        return {"success": success, "msg": msg, "task_uid": task_uid}

    async def _script_upload_handler(self, request):
        """
        Upload script to RE worker environment. If ``update_lists==True`` (default), then lists
        of existing and available plans and devices are updated after the execution of the script.
        If ``update_re==False`` (default), the Run Engine (``RE``) and Data Broker (``db``) objects
        are not updated in RE worker namespace even if they are defined (or redefined) in the uploaded script.
        If ``run_in_background==False`` (default), then the request is rejected unless RE Manager and
        RE Worker environment are in IDLE state, otherwise the script will be loaded in a separate thread
        (not recommended in most practical cases).

        The API call only inititiates the process of loading the script and return success if the request
        is accepted and the task can be started. Success does not mean that the script is successfully loaded.
        """
        logger.info("Uploading script to RE environment ...")
        try:
            supported_param_names = ["script", "update_lists", "update_re", "run_in_background", "lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_environment=True)

            script = request.get("script", None)
            if script is None:
                raise ValueError("Required 'script' parameter is is missing in API call.")
            if not isinstance(script, str):
                raise TypeError("Type of the 'script' parameter in API call is incorrect.")

            update_lists = request.get("update_lists", True)
            update_re = request.get("update_re", False)
            run_in_background = request.get("run_in_background", False)

            success, msg, task_uid = await self._environment_upload_script(
                script=script, update_lists=update_lists, update_re=update_re, run_in_background=run_in_background
            )

        except Exception as ex:
            success, msg, task_uid = False, f"Error: {ex}", None

        return {"success": success, "msg": msg, "task_uid": task_uid}

    async def _function_execute_handler(self, request):
        """
        Starts immediate execution of a function in RE Worker environment. The function must be defined
        in startup script and exist in the worker namespace. The function may be started in the foreground
        (default) or in the background(``run_in_background=True``). If RE Manager is busy executing
        a plan or another foreground task, the request is rejected. The background tasks are executed
        in separate threads, therefore thread safety must be taken into account in planning the workflow
        that requires background tasks and in developing the background functions. The API implementation
        does not guarantee thread safety of the code running in RE Worker namespace. It is strongly
        recommended that the functions do not contain infinite loops or indefinite waits (always set
        timeout). Since Python threads can not be destroyed, the function with infinite wait may require
        closing the environment (function running in the background) or destroying environment (infinite
        wait in forground task). Foreground tasks and plans can be started and executed when
        background tasks are running.

        The API call only inititiates the process of starting execution of the function and returns success
        if the request is accepted. Success does not mean that the function was successfully started
        or successfully run to completion. Use the returned ``task_uid`` to check for the status of
        the task and load the result.
        """
        logger.debug("Starting execution of a function in RE Worker namespace ...")

        item = None
        try:
            supported_param_names = ["item", "user_group", "user", "run_in_background", "lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_environment=True)

            item, item_type, _success, _msg = self._get_item_from_request(
                request=request, supported_item_types=("function",)
            )
            if not _success:
                raise Exception(_msg)

            user, user_group = self._get_user_info_from_request(request=request)
            run_in_background = bool(request.get("run_in_background", False))

            item, _ = self._prepare_item(
                item=item, item_type=item_type, user=user, user_group=user_group, generate_new_uid=True
            )

            success, msg, item, task_uid = await self._environment_function_execute(
                item=item, run_in_background=run_in_background
            )
            if not success:
                raise RuntimeError(msg)

        except Exception as ex:
            success, msg, item, task_uid = False, f"Error: {ex}", item, None

        return {"success": success, "msg": msg, "item": item, "task_uid": task_uid}

    async def _task_result_handler(self, request):
        """
        Returns the information of a task executed by the worker process. The request must contain
        valid ``task_uid``, returned by one of APIs that starts tasks. Returned
        parameters: ``success`` and ``msg`` indicate success of the API call and error message in
        case of API call failure; ``status`` is the status of the task (``running``, ``completed``,
        ``not_found``), ``result`` is a dictionary with information about the task. The information
        is be different for the completed and running tasks. If ``status=='not_found'``, then is
        ``result`` is ``{}``.
        """
        logger.debug("Request for the result of the task executed by RE worker ...")

        task_uid = None

        try:
            supported_param_names = ["task_uid"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            task_uid = request.get("task_uid", None)
            if task_uid is None:
                raise ValueError("Required 'task_uid' parameter is missing in the API call.")

            status, result = await self._task_results.get_task_info(task_uid=task_uid)
            success, msg = True, ""

        except Exception as ex:
            success, msg, status, result = False, f"Error: {ex}", None, None

        return {"success": success, "msg": msg, "task_uid": task_uid, "status": status, "result": result}

    async def _task_status_handler(self, request):
        """
        Returns the status of one or more tasks executed by the worker process. The request must contain
        one or more valid task UIDs, returned by one of APIs that starts tasks. A single UID is passed
        as a string, multiple UIDs - as a list of strings. If a UID is passed as a string, then
        the returned status is also a string, if a list of one or more UIDs is passed, then
        the status is a dictionary that maps task UIDs and their status.

        Returned parameters: ``success`` and ``msg`` indicate success of the API call and error message in
        case of API call failure; ``status`` is the status of the tasks (``running``, ``completed``,
        ``not_found``).
        """
        logger.debug("Request the status of one or more tasks executed by RE Worker ...")

        task_uid = None

        try:
            supported_param_names = ["task_uid"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            task_uid = request.get("task_uid", None)
            if task_uid is None:
                raise ValueError("Required 'task_uid' parameter is missing in the API call.")

            if isinstance(task_uid, str):
                status = (await self._task_results.get_task_info(task_uid=task_uid))[0]
            elif isinstance(task_uid, list):
                status = {_: (await self._task_results.get_task_info(task_uid=_))[0] for _ in task_uid}
            else:
                raise Exception(
                    f"'task_uid' must be a string or a list of strings: type(task_uid)={type(task_uid)!r}"
                )
            success, msg = True, ""

        except Exception as ex:
            success, msg, status = False, f"Error: {ex}", None

        return {"success": success, "msg": msg, "task_uid": task_uid, "status": status}

    async def _queue_start_handler(self, request):
        """
        Start execution of the loaded queue. Additional runs can be added to the queue while
        it is executed. If the queue is empty, then nothing will happen.
        """
        logger.info("Starting queue processing ...")
        try:
            supported_param_names = ["lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_environment=True)

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
            supported_param_names = ["lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_environment=True)

            success, msg = await self._queue_stop_activate()
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
            supported_param_names = ["lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_environment=True)

            success, msg = await self._queue_stop_deactivate()
        except Exception as ex:
            success, msg = False, f"Error: {ex}"

        return {"success": success, "msg": msg}

    async def _queue_autostart_handler(self, request):
        """
        Turn the 'autostart' mode on or off.
        """
        logger.info("Enabling/disabling autostart mode ...")
        try:
            supported_param_names = ["enable", "lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_environment=True)

            if "enable" not in request:
                raise ValueError("Required 'enable' parameter is missing in 'queue_autostart' API call.")

            enable = request.get("enable", None)

            if not isinstance(enable, bool):
                raise TypeError(f"Required 'enable' parameter must be boolean: type(enable)={type(enable)}.")

            if enable:
                success, msg = await self._autostart_enable()
            else:
                success, msg = await self._autostart_disable()

        except Exception as ex:
            success, msg = False, f"Error: {ex}"

        return {"success": success, "msg": msg}

    async def _kernel_interrupt_handler(self, request):
        """
        Unlock RE Manager using ``lock_key``. The ``lock_key`` must match the key used to lock
        the environment and/or the queue. Optionally, RE Manager may be unlocked with
        the emergency lock key (set using  ``QSERVER_EMERGENCY_LOCK_KEY_FOR_SERVER`` environment
        variable).
        """
        logger.info("Processing request to interrupt IPython kernel ...")
        success, msg = True, ""

        try:
            supported_param_names = ["lock_key", "interrupt_task", "interrupt_plan"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_environment=True)

            interrupt_task = bool(request.get("interrupt_task", False))
            interrupt_plan = bool(request.get("interrupt_plan", False))

            success, msg = await self._kernel_interrupt_send(
                interrupt_task=interrupt_task, interrupt_plan=interrupt_plan
            )

        except Exception as ex:
            success, msg = False, f"Error: {ex}"

        return {"success": success, "msg": msg}

    async def _re_pause_handler(self, request):
        """
        Pause Run Engine. The options of immediate and deferred pause is supported.
        Deferred pause is used if no option is specified.
        """
        logger.info("Pausing the queue (currently running plan) ...")

        try:
            supported_param_names = ["option", "lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_environment=True)

            option = request.get("option", "deferred")
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

        return {"success": bool(success), "msg": msg}

    async def _re_resume_handler(self, request):
        """
        Run Engine: resume execution of a paused plan
        """
        logger.info("Resuming paused plan ...")

        try:
            supported_param_names = ["lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_environment=True)

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
            supported_param_names = ["lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_environment=True)

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
            supported_param_names = ["lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_environment=True)

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
            supported_param_names = ["lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            self._validate_lock_key(request.get("lock_key", None), check_environment=True)

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

    def _lock_key_invalid_msg(self):
        """
        Format error message, which reports an invalid lock key.
        """
        return f"Invalid lock key: \n{self._lock_info.to_str()}"

    def _validate_lock_key(self, lock_key, *, check_environment=False, check_queue=False):
        """
        Validate the lock key if the environment and/or queue are locked. The choice
        of ``check_environment`` and ``check_queue`` depend on whether an API is locked
        by ``environment`` or ``queue`` option.
        """
        # Decide if the API is locked
        locked = check_environment and self._lock_info.environment
        locked = locked or (check_queue and self._lock_info.queue)

        if lock_key is None:
            if locked:
                raise ValueError(self._lock_key_invalid_msg())
        elif isinstance(lock_key, str):
            if locked:
                if not self._lock_info.check_lock_key(lock_key):
                    raise ValueError(self._lock_key_invalid_msg())
        else:
            raise ValueError(f"Lock key must be a non-empty string or None: lock_key = {lock_key!r}")

    def _format_lock_info(self):
        """
        Format lock info as a dictionary, which is returned as ``lock_info`` by multiple API.
        """
        return {
            "environment": self._lock_info.environment,
            "queue": self._lock_info.queue,
            "user": self._lock_info.user_name,
            "time": self._lock_info.time,
            "time_str": self._lock_info.time_str,
            "note": self._lock_info.note,
            "emergency_lock_key_is_set": bool(self._lock_info.lock_key_emergency),
        }

    async def _lock_handler(self, request):
        """
        ``lock`` API. Lock RE Manager with the key provided by the client. The API
        provides options to lock the environment, the queue or both. The client
        must select at least one of the options by setting ``True``the parameters
        ``environment`` and ``queue``. If no option is selected, then the API call
        fails.

        The client must provide and keep the ``lock_key``. The key is a non-empty
        string used to unlock RE Manager later. The client may still call API that modify
        the locked environment or the queue by passing the valid ``lock_key`` as a parameter.
        The respective API calls without a valid ``lock_key`` parameter will fail until
        RE Manager is unlocked with ``unlock`` API. RE Manager supports the emergency
        lock key (set using  ``QSERVER_EMERGENCY_LOCK_KEY_FOR_SERVER`` environment variable),
        which could be used to unlock RE Manager if the lock key is lost. The emergency lock
        key is not accepted by any API other than ``unlock``.

        The client must provide the name of the user (``user``) locking RE Manager.
        Optionally, the client may provide ``note``, which is an arbitrary text
        describing the reason why RE Manager is locked. User name and the note are returned
        as part of ``lock_info`` and included in error messages.
        """
        logger.info("Processing request to lock RE Manager ...")
        success, msg, lock_info, lock_info_uid = True, "", {}, None

        try:
            supported_param_names = ["lock_key", "note", "environment", "queue", "user"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            # Validate 'user'
            if "user" not in request:
                raise ValueError("User name is not specified: 'user' is a required parameter")
            user_name = request["user"]
            if not isinstance(user_name, str) or not user_name:
                raise ValueError(f"User name must be a non-empty string: user = {user_name!r}")

            # Validate 'lock_key'
            if "lock_key" not in request:
                raise ValueError("Lock key is not specified: 'lock_key' is a required parameter")
            lock_key = request["lock_key"]
            if not isinstance(lock_key, str) or not lock_key:
                raise ValueError(f"Lock key must be a non-empty string: lock_key = {lock_key!r}")

            # Validate 'note'
            note = request.get("note", None)
            if not isinstance(note, (str, type(None))):
                raise ValueError("Note must be a string or None: note = {note!r}")

            environment = request.get("environment", False)
            queue = request.get("queue", False)

            if self._lock_info.check_lock_key(lock_key):
                self._lock_info.set(
                    environment=environment, queue=queue, lock_key=lock_key, note=note, user_name=user_name
                )
                lock_info = self._format_lock_info()
                lock_info_uid = self._lock_info.uid

                await self._save_lock_info_to_redis()
                logger.info(
                    "RE Manager was locked by the user '%s': environment=%s " "queue=%s. Note: %s",
                    user_name,
                    environment,
                    queue,
                    note,
                )
            else:
                raise ValueError(f"RE Manager was locked with a different key: \n{self._lock_info.to_str()}")

        except Exception as ex:
            logger.info("Failed to lock RE Manager: %s", ex)
            success, msg = False, f"Error: {ex}"

        return {"success": success, "msg": msg, "lock_info": lock_info, "lock_info_uid": lock_info_uid}

    async def _lock_info_handler(self, request):
        """
        Return information on the status of RE Manager lock. Optionally, it can validate the lock key.
        If the value of the optional parameter ``lock_key`` has a string value and RE Manager is locked,
        then the function verifies if the string matches current lock key used to lock the manager.
        The API call succeeds (``'success': True``) if the keys match and fails otherwise. If ``lock_key``
        is missing or ``None``, then the API call always succeeds. The API call always succeeds if
        RE Manager is unlocked. The function does not match ``lock_key`` with the emergency lock key,
        which is used only to unlock RE Manager if the lock key is lost.
        """
        success, msg, lock_info, lock_info_uid = True, "", {}, None

        try:
            supported_param_names = ["lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            lock_info = self._format_lock_info()
            lock_info_uid = self._lock_info.uid

            lock_key = request.get("lock_key", None)
            if lock_key is not None:
                self._validate_lock_key(lock_key, check_environment=True, check_queue=True)

        except Exception as ex:
            success, msg = False, f"Error: {ex}"

        return {"success": success, "msg": msg, "lock_info": lock_info, "lock_info_uid": lock_info_uid}

    async def _unlock_handler(self, request):
        """
        Unlock RE Manager using ``lock_key``. The ``lock_key`` must match the key used to lock
        the environment and/or the queue. Optionally, RE Manager may be unlocked with
        the emergency lock key (set using  ``QSERVER_EMERGENCY_LOCK_KEY_FOR_SERVER`` environment
        variable).
        """
        logger.info("Processing request to unlock RE Manager ...")
        success, msg, lock_info, lock_info_uid = True, "", {}, None

        try:
            supported_param_names = ["lock_key"]
            self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

            if "lock_key" not in request:
                raise ValueError("Request contains no lock key: 'lock_key' parameter is required")
            lock_key = request["lock_key"]
            if not isinstance(lock_key, str) or not lock_key:
                raise ValueError(f"Lock key must be a non-empty string: lock_key = {lock_key!r}")

            if not self._lock_info.is_set():
                lock_info = self._format_lock_info()
                lock_info_uid = self._lock_info.uid
                logger.info("RE Manager is already unlocked. No action is required.")
            elif self._lock_info.check_lock_key(lock_key, use_emergency_key=True):
                self._lock_info.clear()
                lock_info = self._format_lock_info()
                lock_info_uid = self._lock_info.uid
                await self._save_lock_info_to_redis()
                logger.info("RE Manager is successfully unlocked.")
            else:
                lock_info = self._format_lock_info()
                lock_info_uid = self._lock_info.uid
                raise ValueError(self._lock_key_invalid_msg())

        except Exception as ex:
            success, msg = False, f"Error: {ex}"

        return {"success": success, "msg": msg, "lock_info": lock_info, "lock_info_uid": lock_info_uid}

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
        success, msg = True, ""

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

    async def _manager_test_handler(self, request):
        """
        This API is intended exclusively for unit testing. Available tests (selected using 'test_name'):

        - ``reserve_kernel`` - calls the function that reserves kernel running in the worker space.

        """
        success, msg = True, ""

        try:
            test_name = request.get("test_name", None)

            supported_param_names = ["test_name"]
            known_tests = ["reserve_kernel"]

            if test_name == "reserve_kernel":
                self._check_request_for_unsupported_params(request=request, param_names=supported_param_names)

                if not self._use_ipython_kernel:
                    raise RuntimeError("IPython kernel mode is not enabled")

                if not self._environment_exists:
                    raise RuntimeError("Worker environment does not exist")

                # Attempt to reserve IPython kernel.
                _success, _msg = await self._worker_command_reserve_kernel()
                if not _success:
                    raise RuntimeError(f"Failed to capture IPython kernel: {_msg}")

            else:
                raise ValueError(f"Unknown test: {test_name!r}. Known tests: {known_tests}")

        except Exception as ex:
            success, msg = False, f"Error: {ex}"

        return {"success": success, "msg": msg}

    async def _zmq_execute(self, msg):
        handler_dict = {
            "ping": "_ping_handler",
            "status": "_status_handler",
            "config_get": "_config_get_handler",
            "queue_get": "_queue_get_handler",
            "plans_allowed": "_plans_allowed_handler",
            "plans_existing": "_plans_existing_handler",
            "devices_allowed": "_devices_allowed_handler",
            "devices_existing": "_devices_existing_handler",
            "permissions_reload": "_permissions_reload_handler",
            "permissions_get": "_permissions_get_handler",
            "permissions_set": "_permissions_set_handler",
            "history_get": "_history_get_handler",
            "history_clear": "_history_clear_handler",
            "environment_open": "_environment_open_handler",
            "environment_close": "_environment_close_handler",
            "environment_destroy": "_environment_destroy_handler",
            "environment_update": "_environment_update_handler",
            "script_upload": "_script_upload_handler",
            "function_execute": "_function_execute_handler",
            "task_result": "_task_result_handler",
            "task_status": "_task_status_handler",
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
            "queue_autostart": "_queue_autostart_handler",
            "kernel_interrupt": "_kernel_interrupt_handler",
            "re_pause": "_re_pause_handler",
            "re_resume": "_re_resume_handler",
            "re_stop": "_re_stop_handler",
            "re_abort": "_re_abort_handler",
            "re_halt": "_re_halt_handler",
            "re_runs": "_re_runs_handler",
            "lock": "_lock_handler",
            "lock_info": "_lock_info_handler",
            "unlock": "_unlock_handler",
            "manager_stop": "_manager_stop_handler",
            "manager_kill": "_manager_kill_handler",
            "manager_test": "_manager_test_handler",
        }

        try:
            if isinstance(msg, str):
                raise Exception(f"Failed to decode the request: {msg}")
            if not isinstance(msg, dict):
                raise Exception(f"Incorrect request type: {type(msg)}. Dictionary is expected")
            if "method" not in msg:
                raise Exception(f"Invalid request format: method is not specified: {msg!r}")
            # Check that the request contains no extra keys
            allowed_keys = ("method", "params")
            extra_keys = [_ for _ in msg.keys() if _ not in allowed_keys]
            if extra_keys:
                raise Exception(f"Request contains unexpected keys {extra_keys}. Allowed keys: {allowed_keys}")

            method = msg["method"]  # Required
            params = msg.get("params", {})  # Optional

            handler_name = handler_dict[method]
            handler = getattr(self, handler_name)
            result = await handler(params)
        except KeyError:
            result = {"success": False, "msg": f"Unknown method {method!r}"}
        except AttributeError:
            result = {
                "success": False,
                "msg": f"Handler for the command {method!r} is not implemented",
            }
        except Exception as ex:
            result = {"success": False, "msg": str(ex)}
        return result

    # ======================================================================
    #          Functions that support communication via 0MQ

    async def _zmq_receive(self):
        try:
            msg_in = await self._zmq_socket.recv_json()
        except Exception as ex:
            msg_in = f"JSON decode error: {ex}"
        return msg_in

    async def _zmq_send(self, msg):
        await self._zmq_socket.send_json(msg)

    async def zmq_server_comm(self):
        """
        This function is executed by asyncio.run() to start the manager.
        """
        self._ctx = zmq.asyncio.Context()

        self._loop = asyncio.get_running_loop()
        self._exec_loop_deactivated_event = asyncio.Event()
        self._queue_autostart_event = asyncio.Event()

        self._comm_to_watchdog = PipeJsonRpcSendAsync(
            conn=self._watchdog_conn,
            use_json=False,
            name="RE Manager-Watchdog Comm",
        )
        self._comm_to_worker = PipeJsonRpcSendAsync(
            conn=self._worker_conn,
            use_json=False,
            name="RE Manager-Worker Comm",
            timeout=self._comm_to_worker_timeout,
        )
        self._comm_to_watchdog.start()
        self._comm_to_worker.start()

        self._task_results = TaskResults(retention_time=120)

        # Start heartbeat generator
        self._heartbeat_generator_task = asyncio.ensure_future(self._heartbeat_generator(), loop=self._loop)
        self._worker_status_task = asyncio.ensure_future(self._periodic_worker_state_request(), loop=self._loop)

        self._plan_queue = PlanQueueOperations(
            redis_host=self._ip_redis_server, name_prefix=self._redis_name_prefix
        )
        await self._plan_queue.start()

        # Delete Redis entries (for testing and debugging)
        # self._plan_queue.delete_pool_entries()

        # 'stop_pending' is set False when the application starts, and read from Redis on restarts
        queue_stop_pending_redis = await self._plan_queue.stop_pending_retrieve()
        queue_stop_pending_redis = queue_stop_pending_redis or {"enabled": False}
        queue_stop_pending_redis = queue_stop_pending_redis.get("enabled", False)
        await self.set_queue_stop_pending(False if self._number_of_restarts == 1 else queue_stop_pending_redis)

        # 'autostart_enabled' is set False when the application starts, and read from Redis on restarts
        autostart_enabled_redis = await self._plan_queue.autostart_mode_retrieve()
        autostart_enabled_redis = autostart_enabled_redis or {"enabled": False}
        autostart_enabled_redis = autostart_enabled_redis.get("enabled", False)
        await self.set_queue_autostart_enabled(False if self._number_of_restarts == 1 else autostart_enabled_redis)

        # Load user group permissions
        if (self._user_group_permissions_reload_option == "ON_STARTUP") and (self._number_of_restarts == 1):
            logger.debug("Loading user permissions from disk ...")
            await self._load_permissions_from_disk()
        else:
            logger.debug("Loading user permissions from Redis ...")
            await self._load_permissions_from_redis()

        # Load lock info from Redis if possible
        try:
            await self._load_lock_info_from_redis()
        except Exception as ex:
            logger.exception(ex)

        # Set the environment state based on whether the worker process is alive (request Watchdog)
        self._environment_exists = await self._is_worker_alive()

        # Now check if the plan is still being executed (if it was executed)
        if self._environment_exists:
            # The environment is expected to contain the most recent version of the list of plans and devices
            #   and a copy of user group permissions, so they could be downloaded from the worker.
            #   If the request to download plans and devices fails, then the lists of existing and allowed
            #   devices and plans and the dictionary of user group permissions are going to be empty ({}).
            await self._load_existing_plans_and_devices_from_worker()

            try:
                self._update_allowed_plans_and_devices(restore_plans_devices=False)
            except Exception as ex:
                logger.exception("Exception: %s", ex)

            # Attempt to load the list of active runs.
            re_run_list, _ = await self._worker_request_run_list()
            self._re_run_list = re_run_list["run_list"] if re_run_list else []
            self._re_run_list_uid = _generate_uid()

            self._worker_state_info, err_msg = await self._worker_request_state()
            if self._worker_state_info:
                item_uid_running = self._worker_state_info["running_item_uid"]
                running_task_uid = self._worker_state_info["running_task_uid"]
                re_state = self._worker_state_info["re_state"]
                re_report_available = self._worker_state_info["re_report_available"]
                re_deferred_pause_requested = self._worker_state_info["re_deferred_pause_requested"]
                if (re_state == "executing_task") and running_task_uid:
                    self._manager_state = MState.EXECUTING_TASK
                    self._running_task_uid = running_task_uid
                elif item_uid_running and (re_report_available or (re_state != "idle")):
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
            # Load lists of allowed plans and devices
            logger.info("Loading the lists of allowed plans and devices ...")
            try:
                self._update_allowed_plans_and_devices(restore_plans_devices=True)
            except Exception as ex:
                logger.exception("Exception: %s", ex)

            self._worker_state_info = None

        if self._manager_state not in (MState.EXECUTING_QUEUE, MState.PAUSED):
            # TODO: logic may need to be revised
            await self._plan_queue.set_processed_item_as_completed(
                exit_status="unknown",
                run_uids=[],
                scan_ids=[],
                err_msg="Plan exit status was lost due to restart of the RE Manager process",
                err_tb="",
            )

        # We also need to start the autostart loop if necessary
        if self.queue_autostart_enabled:
            self._loop.create_task(self._autostart_task())

        logger.info("Starting ZeroMQ server ...")
        self._zmq_socket = self._ctx.socket(zmq.REP)

        if self._zmq_private_key is not None:
            self._zmq_socket.set(zmq.CURVE_SERVER, 1)
            self._zmq_socket.set(zmq.CURVE_SECRETKEY, self._zmq_private_key.encode("utf-8"))

        self._zmq_socket.bind(self._zmq_ip_server)
        logger.info("ZeroMQ server is waiting on %s", str(self._zmq_ip_server))

        if self._manager_state == MState.INITIALIZING:
            self._manager_state = MState.IDLE

        # Initialization is complete, enable the watchdog.
        await self._watchdog_enable()

        while True:
            #  Wait for next request from client
            msg_in = await self._zmq_receive()
            logger.debug("ZeroMQ server received request: %s", ppfl(msg_in))

            msg_out = await self._zmq_execute(msg_in)

            #  Send reply back to client
            logger.debug("ZeroMQ server sending response: %s", ppfl(msg_out))
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
                self._heartbeat_generator_task.cancel()
                self._comm_to_watchdog.stop()
                self._comm_to_worker.stop()
                await self._plan_queue.stop()
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
        setup_loggers(log_level=self._log_level)

        logger.info("Starting RE Manager process")
        try:
            asyncio.run(self.zmq_server_comm())
        except Exception as ex:
            logger.exception("Exiting RE Manager with exception %s", str(ex))
        except KeyboardInterrupt:
            # TODO: RE Manager must be orderly closed before Watchdog module is stopped.
            #   Right now it is just killed by SIGINT.
            logger.info("RE Manager Process was stopped by SIGINT. Handling of Ctrl-C has to be revised!!!")
