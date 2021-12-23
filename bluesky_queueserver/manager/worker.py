from multiprocessing import Process
import threading
import queue
import time as ttime
import os
import asyncio
from functools import partial
import logging
import uuid
import enum
import traceback
import copy
import json

from .comms import PipeJsonRpcReceive
from .output_streaming import setup_console_output_redirection
from .logging_setup import setup_loggers

from event_model import RunRouter

import msgpack
import msgpack_numpy as mpn

from .profile_ops import (
    load_worker_startup_code,
    load_allowed_plans_and_devices,
    update_existing_plans_and_devices,
    prepare_plan,
    existing_plans_and_devices_from_nspace,
    extract_script_root_path,
    load_script_into_existing_nspace,
    compare_existing_plans_and_devices,
    prepare_function,
)

logger = logging.getLogger(__name__)


# State of the worker environment
class EState(enum.Enum):
    INITIALIZING = "initializing"
    IDLE = "idle"
    EXECUTING_PLAN = "executing_plan"
    EXECUTING_TASK = "executing_task"
    CLOSING = "closing"
    CLOSED = "closed"  # For completeness


class RejectedError(RuntimeError):
    ...


class RunEngineWorker(Process):
    """
    The class implementing Run Engine Worker thread.

    Parameters
    ----------
    conn: multiprocessing.Connection
        One end of bidirectional (input/output) pipe. The other end is used by RE Manager.
    args, kwargs
        `args` and `kwargs` of the `multiprocessing.Process`
    """

    def __init__(
        self,
        *args,
        conn,
        config=None,
        msg_queue=None,
        log_level=logging.DEBUG,
        user_group_permissions=None,
        **kwargs,
    ):

        if not conn:
            raise RuntimeError("Invalid value of parameter 'conn': %S.", str(conn))

        super().__init__(*args, **kwargs)

        self._log_level = log_level
        self._msg_queue = msg_queue

        self._user_group_permissions = user_group_permissions or {}

        # The end of bidirectional Pipe assigned to the worker (for communication with Manager process)
        self._conn = conn

        self._exit_event = None
        self._exit_confirmed_event = None

        self._execution_queue = None

        # Reference to Bluesky Run Engine
        self._RE = None

        # The following variable determine the state of RE Worker
        self._env_state = EState.CLOSED
        self._running_plan_info = None
        self._running_plan_completed = False
        self._running_task_uid = None  # UID of the running foreground task (if running a task)

        self._background_tasks_num = 0  # The number of background tasks

        # Report (dict) generated after execution of a command. The report can be downloaded
        #   by RE Manager.
        self._re_report = None
        self._re_report_lock = None  # threading.Lock

        # Class that supports communication over the pipe
        self._comm_to_manager = PipeJsonRpcReceive(conn=self._conn, name="RE Worker-Manager Comm")

        self._db = None

        # Note: 'self._config' is a private attribute of 'multiprocessing.Process'. Overriding
        #   this variable may lead to unpredictable and hard to debug issues.
        self._config_dict = config or {}
        self._existing_plans_and_devices_changed = False
        self._existing_plans, self._existing_devices = {}, {}
        self._allowed_plans, self._allowed_devices = {}, {}

        self._allowed_items_lock = None  # threading.Lock()
        self._existing_items_lock = None  # threading.Lock()

        self._completed_tasks = []
        self._completed_tasks_lock = None  # threading.Lock()

        # Indicates when to update the existing plans and devices
        update_pd = self._config_dict["update_existing_plans_devices"]
        if update_pd not in ("NEVER", "ENVIRONMENT_OPEN", "ALWAYS"):
            logger.error(
                "Unknown option for updating lists of existing plans and devices: '%s'. "
                "The lists stored on disk are not going to be updated.",
                update_pd,
            )
            update_pd = "NEVER"
        self._update_existing_plans_devices_on_disk = update_pd

        # The list of runs that were opened as part of execution of the currently running plan.
        # Initialized with 'RunList()' in 'run()' method.
        self._active_run_list = None

        self._re_namespace, self._plans_in_nspace, self._devices_in_nspace = {}, {}, {}

    def _execute_plan(self, plan, is_resuming):
        """
        Start Run Engine to execute a plan

        Parameters
        ----------
        plan: function
            Reference to a function that calls Run Engine. Run Engine may be called to execute,
            resume, abort, stop or halt the plan. The function should not accept any arguments.
        is_resuming: bool
            A flag indicates if the plan is going to be resumed (executed). It is True if
            'plan' starts a new plan or resumes paused plan. It is False if paused plan is
            aborted, stopped or halted.
        """
        logger.debug("Starting execution of a plan")
        try:
            result = plan()
            with self._re_report_lock:
                self._re_report = {
                    "action": "plan_exit",
                    "success": True,
                    "result": result,
                    "err_msg": "",
                }
                if is_resuming:
                    self._re_report["plan_state"] = "completed"
                    self._running_plan_completed = True
                else:
                    # Here we don't distinguish between stop/abort/halt
                    self._re_report["plan_state"] = "stopped"
                    self._running_plan_completed = True

                # Include RE state
                self._re_report["re_state"] = str(self._RE._state)

                # Clear the list of active runs
                self._active_run_list.clear()

        except BaseException as ex:
            with self._re_report_lock:

                self._re_report = {
                    "action": "plan_exit",
                    "result": "",
                    "err_msg": str(ex),
                }

                if self._RE._state == "paused":
                    # Run Engine was paused
                    self._re_report["plan_state"] = "paused"
                    self._re_report["success"] = True

                else:
                    # RE crashed. Plan execution can not be resumed. (Environment may have to be restarted.)
                    # TODO: clarify how this situation must be handled. Also additional error handling
                    #       may be required
                    self._re_report["plan_state"] = "error"
                    self._re_report["success"] = False
                    self._running_plan_completed = True

                    # Clear the list of active runs (don't clean the list for the paused plan).
                    self._active_run_list.clear()

                # Include RE state
                self._re_report["re_state"] = str(self._RE._state)

        self._env_state = EState.IDLE
        logger.debug("Plan execution is completed or interrupted")

    def _load_new_plan(self, plan_info):
        """
        Loads a new plan into `self._execution_queue`. The plan plan name and
        device names are represented as strings. Parsing of the plan in this
        function replaces string representation with references.

        Parameters
        ----------
        plan_name: str
            name of the plan, represented as a string
        plan_args: list
            plan args, devices are represented as strings
        plan_kwargs: dict
            plan kwargs
        """
        if self._env_state != EState.IDLE:
            raise RejectedError(
                f"Attempted to start a plan in '{self._env_state.value}' environment state. Accepted state: 'idle'"
            )

        # Save reference to the currently executed plan
        self._running_plan_info = plan_info
        self._running_plan_completed = False
        self._env_state = EState.EXECUTING_PLAN

        logger.info("Starting a plan '%s'.", plan_info["name"])

        try:
            with self._allowed_items_lock:
                allowed_plans, allowed_devices = self._allowed_plans, self._allowed_devices

            plan_parsed = prepare_plan(
                plan_info,
                plans_in_nspace=self._plans_in_nspace,
                devices_in_nspace=self._devices_in_nspace,
                allowed_plans=allowed_plans,
                allowed_devices=allowed_devices,
            )

            plan_func = plan_parsed["callable"]
            plan_args_parsed = plan_parsed["args"]
            plan_kwargs_parsed = plan_parsed["kwargs"]
            plan_meta_parsed = plan_parsed["meta"]

            def get_plan(plan_func, plan_args, plan_kwargs, plan_meta):
                def plan():
                    if self._RE._state == "panicked":
                        raise RuntimeError(
                            "Run Engine is in the 'panicked' state. The environment must be "
                            "closed and opened again before plans could be executed."
                        )
                    elif self._RE._state != "idle":
                        raise RuntimeError(
                            f"Run Engine is in '{self._RE._state}' state. Stop or finish any running plan."
                        )
                    else:
                        result = self._RE(plan_func(*plan_args, **plan_kwargs), **plan_meta)
                    return result

                return plan

            plan = get_plan(plan_func, plan_args_parsed, plan_kwargs_parsed, plan_meta_parsed)
            # 'is_resuming' is true (we start a new plan that is supposedly runs to completion
            #   as opposed to aborting/stopping/halting a plan)
        except Exception as ex:
            # We want the exception to be raised in the main thread (plan execution)
            def get_plan(err_msg):
                def plan():
                    raise Exception(err_msg)

                return plan()

            plan = get_plan(str(ex))

        self._execution_queue.put((plan, True))

    def _continue_plan(self, option):
        """
        Continue/stop execution of a plan after it was paused.

        Parameters
        ----------
        option: str
            Option on how to proceed with previously paused plan. The values are
            "resume", "abort", "stop", "halt".
        """
        if self._env_state != EState.IDLE:
            raise RejectedError(
                f"Attempted to start a plan in '{self._env_state.value}' environment state. "
                f"Accepted state: '{EState.IDLE.value}'"
            )

        self._env_state = EState.EXECUTING_PLAN

        logger.info("Continue plan execution with the option '%s'", option)

        available_options = ("resume", "abort", "stop", "halt")

        # We are not parsing 'kwargs' at this time
        def get_plan(option, available_options):
            def plan():
                if self._RE._state == "panicked":
                    raise RuntimeError(
                        "Run Engine is in the 'panicked' state. "
                        "You need to recreate the environment before you can run plans."
                    )
                elif self._RE._state != "paused":
                    raise RuntimeError(
                        f"Run Engine is in '{self._RE._state}' state. Only 'paused' plan can be continued."
                    )
                elif option not in available_options:
                    raise RuntimeError(
                        f"Option '{option}' is not supported. Supported options: {available_options}"
                    )
                else:
                    result = getattr(self._RE, option)()
                return result

            return plan

        plan = get_plan(option, available_options)
        is_resuming = option == "resume"
        self._execution_queue.put((plan, is_resuming))

    def _generate_lists_of_allowed_plans_and_devices(self):
        """
        Generate lists of allowed plans and devices based on the existing plans and devices and user permissions.
        """
        logger.info("Generating lists of allowed plans and devices")

        with self._existing_items_lock:
            existing_plans, existing_devices = self._existing_plans, self._existing_devices

        allowed_plans, allowed_devices = load_allowed_plans_and_devices(
            existing_plans=existing_plans,
            existing_devices=existing_devices,
            user_group_permissions=self._user_group_permissions,
        )

        with self._allowed_items_lock:
            self._allowed_plans, self._allowed_devices = allowed_plans, allowed_devices

        logger.info("List of allowed plans and devices was successfully generated")

    def _update_existing_pd_file(self, *, options):
        """
        Update existing plans and devices on disk. ``options`` parameter is a list (or tuple)
        of options which are compared to ``self._update_existing_plans_devices_on_disk`` to
        determine if the lists should be saved.
        """

        path_pd = self._config_dict["existing_plans_and_devices_path"]

        if self._update_existing_plans_devices_on_disk in options:
            with self._existing_items_lock:
                existing_plans, existing_devices = self._existing_plans, self._existing_devices
            update_existing_plans_and_devices(
                path_to_file=path_pd,
                existing_plans=existing_plans,
                existing_devices=existing_devices,
            )

    def _load_script_into_environment(self, *, script, update_re):
        """
        Load script passed as a string variable (``script``) into RE environment namespace.
        Boolean variable ``update_re`` controls whether ``RE`` and ``db`` are updated if
        the new values are defined in the script.
        """
        startup_dir = self._config_dict.get("startup_dir", None)
        startup_module_name = self._config_dict.get("startup_module_name", None)
        startup_script_path = self._config_dict.get("startup_script_path", None)

        script_root_path = extract_script_root_path(
            startup_dir=startup_dir,
            startup_module_name=startup_module_name,
            startup_script_path=startup_script_path,
        )

        load_script_into_existing_nspace(
            script=script,
            nspace=self._re_namespace,
            script_root_path=script_root_path,
            update_re=update_re,
        )

        if update_re:
            if ("RE" in self._re_namespace) and (self._RE != self._re_namespace["RE"]):
                self._RE = self._re_namespace["RE"]

                from .plan_monitoring import CallbackRegisterRun

                run_reg_cb = CallbackRegisterRun(run_list=self._active_run_list)
                self._RE.subscribe(run_reg_cb)

                logger.info("Run Engine instance ('RE') was replaced while executing the uploaded script.")

            if ("db" in self._re_namespace) and (self._db != self._re_namespace["db"]):
                self._db = self._re_namespace["db"]
                logger.info("Data Broker instance ('db') was replaced while executing the uploaded script.")

        epd = existing_plans_and_devices_from_nspace(nspace=self._re_namespace)
        existing_plans, existing_devices, plans_in_nspace, devices_in_nspace = epd

        self._existing_plans_and_devices_changed = not compare_existing_plans_and_devices(
            existing_plans=existing_plans,
            existing_devices=existing_devices,
            existing_plans_ref=self._existing_plans,
            existing_devices_ref=self._existing_devices,
        )

        # Dictionaries of references to plans and devices from the namespace (may change even
        #   if the list of existing plans and devices was not changed)
        self._plans_in_nspace = plans_in_nspace
        self._devices_in_nspace = devices_in_nspace

        if self._existing_plans_and_devices_changed:
            # Descriptions of existing plans and devices
            with self._existing_items_lock:
                self._existing_plans, self._existing_devices = existing_plans, existing_devices
            self._generate_lists_of_allowed_plans_and_devices()
            self._update_existing_pd_file(options=("ALWAYS",))

        logger.info("The script was successfully loaded into RE environment")

    def _execute_function_in_environment(self, *, func_info):
        """
        Execute function in the worker environment.
        """
        func_parsed = prepare_function(
            func_info=func_info,
            nspace=self._re_namespace,
            user_group_permissions=self._user_group_permissions,
        )

        func_callable = func_parsed["callable"]
        func_args = func_parsed["args"]
        func_kwargs = func_parsed["kwargs"]

        return_value = func_callable(*func_args, **func_kwargs)

        func_name = func_info.get("name", "--")
        logger.debug(f"The execution of the function {func_name!r} was successfully completed.")

        return return_value

    # =============================================================================
    #               Handlers for messages from RE Manager

    def _request_state_handler(self):
        """
        Returns the state information of RE Worker environment.
        """
        item_uid = self._running_plan_info["item_uid"] if self._running_plan_info else None
        task_uid = self._running_task_uid
        plan_completed = self._running_plan_completed
        # TODO: replace RE._state with RE.state property in the worker code (improve code style).
        re_state = str(self._RE._state) if self._RE else "null"
        try:
            re_deferred_pause_requested = self._RE.deferred_pause_requested if self._RE else False
        except AttributeError:
            # TODO: delete this branch once Bluesky supporting
            #   ``RunEngine.deferred_pause_pending``` is widely deployed.
            re_deferred_pause_requested = self._RE._deferred_pause_requested if self._RE else False
        env_state_str = self._env_state.value
        re_report_available = self._re_report is not None
        run_list_updated = self._active_run_list.is_changed()  # True - updates are available
        plans_and_devices_list_updated = self._existing_plans_and_devices_changed
        completed_tasks_available = bool(self._completed_tasks)
        background_tasks_num = self._background_tasks_num

        msg_out = {
            "running_item_uid": item_uid,
            "running_plan_completed": plan_completed,
            "re_report_available": re_report_available,
            "re_state": re_state,
            "re_deferred_pause_requested": re_deferred_pause_requested,
            "environment_state": env_state_str,
            "run_list_updated": run_list_updated,
            "plans_and_devices_list_updated": plans_and_devices_list_updated,
            "running_task_uid": task_uid,
            "completed_tasks_available": completed_tasks_available,
            "background_tasks_num": background_tasks_num,
        }
        return msg_out

    def _request_plan_report_handler(self):
        """
        Returns the report on recently completed plan. Note that the report is `None` if
        plan execution was not completed. The report should be requested only after
        `re_report_available` is set in RE Worker state. The report is cleared once
        it is read.
        """
        # TODO: may be report should be cleared only after the reset? Check the logic.
        with self._re_report_lock:
            msg_out = self._re_report
            self._re_report = None
        return msg_out

    def _request_run_list_handler(self):
        """
        Returns the list of runs for the currently executed plan and clears the state
        of the list. The list can be requested at any time, but it is recommended that
        the state of the list is checked first (`re_report` field `run_list_updated`)
        and update is loaded only if updates exist (`run_list_updated` is True).
        """
        msg_out = {"run_list": self._active_run_list.get_run_list(clear_state=True)}
        return msg_out

    def _request_task_results_handler(self):
        """
        Returns the list of results of completed tasks and clears the list.
        """
        with self._completed_tasks_lock:
            msg_out = {"task_results": copy.copy(self._completed_tasks)}
            self._completed_tasks.clear()
        return msg_out

    def _request_plans_and_devices_list_handler(self):
        """
        Returns currents lists of existing plans and devices. Also returns the current
        dictionary of user group permissions. It is assumed that the dictionary of user group
        permissions is relatively small and passing it with the lists of existing plans
        and devices should not affect the performance. If performance becomes an issue, then
        create a separate API for passing user group permissions.
        """
        with self._existing_items_lock:
            existing_plans, existing_devices = self._existing_plans, self._existing_devices
        msg_out = {
            "existing_plans": existing_plans,
            "existing_devices": existing_devices,
            "user_group_permissions": self._user_group_permissions,
        }
        self._existing_plans_and_devices_changed = False
        return msg_out

    def _command_close_env_handler(self):
        """
        Close RE Worker environment in orderly way.
        """
        # Stop the loop in main thread
        logger.info("Closing RE Worker environment ...")
        # TODO: probably the criteria on when the environment could be more precise.
        #       For now simply assume that we can not close the environment in which
        #       Run Engine is running using this method. Different method that kills
        #       the worker process is needed.
        err_msg = None
        if (self._RE is None) or (self._RE._state != "running"):
            try:
                self._exit_event.set()
                status = "accepted"
            except Exception as ex:
                status = "error"
                err_msg = str(ex)
        else:
            status = "rejected"
            err_msg = "Can not close the environment with running Run Engine. Stop the running plan and try again."
        msg_out = {"status": status, "err_msg": err_msg}
        return msg_out

    def _command_confirm_exit_handler(self):
        """
        Confirm that the environment is closed. The 'accepted' status returned by
        the function indicates that the environment is almost closed. After the command
        is sent two things should happen: the environment is closed completely and the process
        is terminated; the caller may safely assume that the environment does not exist
        (no messages can be sent to the closed environment).
        """
        err_msg = ""
        if self._exit_event.is_set():
            self._exit_confirmed_event.set()
            status = "accepted"
        else:
            status = "rejected"
            err_msg = (
                "Environment closing was not initiated. Use command 'command_close_env' "
                "to initiate closing and wait for RE Worker state: "
                f"environment state is '{self._env_state.value}'"
            )
        msg_out = {"status": status, "err_msg": err_msg}
        return msg_out

    def _command_run_plan_handler(self, *, plan_info):
        """
        Initiate execution of a new plan.
        """
        logger.info("Starting execution of a plan ...")
        # TODO: refine the conditions that are verified before a new plan is accepted.
        invalid_state = 0
        if not self._execution_queue.empty():
            invalid_state = 1
        elif self._RE._state == "running":
            invalid_state = 2
        elif self._running_plan_info or self._running_plan_completed:
            invalid_state = 3

        err_msg = ""
        if not invalid_state:  # == 0
            try:
                # Value is a dictionary with plan parameters
                self._load_new_plan(plan_info=plan_info)
                status = "accepted"
            except RejectedError as ex:
                status = "rejected"
                err_msg = str(ex)
            except Exception as ex:
                status = "error"
                err_msg = str(ex)
        else:
            status = "rejected"
            msg_list = [
                "the execution queue is not empty",
                "another plan is running",
                "worker is not reset after completion of the previous plan",
            ]
            try:
                s = msg_list[invalid_state - 1]
            except Exception:
                s = "UNDETERMINED CONDITION IS PRESENT"  # Shouldn't ever be printed
            err_msg = (
                f"Trying to run a plan (start Run Engine) while {s}.\n"
                "This may indicate a serious issue with the plan queue execution mechanism.\n"
                "Please report the issue to developers."
            )

        msg_out = {"status": status, "err_msg": err_msg}
        return msg_out

    def _command_pause_plan_handler(self, *, option):
        """
        Pause running plan. Options: `deferred` and `immediate`.
        """
        # Stop the loop in main thread
        logger.info("Pausing Run Engine ...")
        pausing_options = ("deferred", "immediate")
        # TODO: the question is whether it is possible or should be allowed to pause a plan in
        #       any other state than 'running'???
        err_msg = ""
        if self._RE._state == "running":
            try:
                if option not in pausing_options:
                    raise RuntimeError(f"Option '{option}' is not supported. Available options: {pausing_options}")

                defer = {"deferred": True, "immediate": False}[option]
                self._RE.request_pause(defer=defer)
                status = "accepted"
            except Exception as ex:
                status = "error"
                err_msg = str(ex)
        else:
            status = "rejected"
            err_msg = "Run engine can be paused only in 'running' state. " f"Current state: '{self._RE._state}'"

        msg_out = {"status": status, "err_msg": err_msg}
        return msg_out

    def _command_continue_plan_handler(self, *, option):
        """
        Continue execution of a paused plan. Options: `resume`, `stop`, `abort` and `halt`.
        """
        # Continue execution of the plan
        err_msg = ""
        if self._RE.state == "paused":
            try:
                logger.info("Run Engine: %s", option)
                self._continue_plan(option)
                status = "accepted"
            except RejectedError as ex:
                status = "rejected"
                err_msg = str(ex)
            except Exception as ex:
                status = "error"
                err_msg = str(ex)
        else:
            status = "rejected"
            err_msg = "Run Engine must be in 'paused' state to continue. " f"The state is '{self._RE._state}'"

        msg_out = {"status": status, "err_msg": err_msg}
        return msg_out

    def _command_reset_worker_handler(self):
        """
        Reset state of RE Worker environment (prepare for execution of a new plan)
        """
        err_msg = ""
        if self._RE._state == "idle":
            self._running_plan_info = None
            self._running_plan_completed = False
            with self._re_report_lock:
                self._re_report = None
            status = "accepted"
        else:
            status = "rejected"
            err_msg = f"Run Engine must be in 'idle' state to continue. The state is '{self._RE._state}'"

        msg_out = {"status": status, "err_msg": err_msg}
        return msg_out

    def _command_permissions_reload_handler(self, user_group_permissions):
        """
        Initiate reloading of permissions and computing new lists of existing plans and devices.
        Computations are performed in a separate thread. The function is not waiting for computations
        to complete. Status ('accepted' or 'rejected') and error message is returned. 'accepted' status
        does not mean that the operation was successful.
        """
        self._user_group_permissions = user_group_permissions
        status, err_msg, task_uid, payload = self._run_in_separate_thread(
            name="Reload Permissions",
            target=self._generate_lists_of_allowed_plans_and_devices,
            run_in_background=True,
        )
        msg_out = {"status": status, "err_msg": err_msg, "task_uid": task_uid, "payload": payload}
        return msg_out

    def _command_load_script(self, script, update_re, run_in_background):
        """
        Load the script passed as a string variable into the existing RE environment.
        The task could be started in the background (when a plan or another foreground task
        is running), but it is not recommended.
        """
        status, err_msg, task_uid, payload = self._run_in_separate_thread(
            name="Load script",
            target=self._load_script_into_environment,
            kwargs={"script": script, "update_re": update_re},
            run_in_background=run_in_background,
        )
        msg_out = {"status": status, "err_msg": err_msg, "task_uid": task_uid, "payload": payload}
        return msg_out

    def _command_execute_function(self, func_info, run_in_background):
        """
        Start execution of a function in the worker namespace. The item type must be ``function``.
        The function may be started in the background.
        """
        # Reuse item UID as task UID
        task_uid = func_info.get("item_uid", None)

        status, err_msg, task_uid, payload = self._run_in_separate_thread(
            name="Execute function",
            target=self._execute_function_in_environment,
            kwargs={"func_info": func_info},
            run_in_background=run_in_background,
            task_uid=task_uid,
        )
        msg_out = {"status": status, "err_msg": err_msg, "task_uid": task_uid, "payload": payload}
        return msg_out

    # ------------------------------------------------------------

    def _execute_in_main_thread(self):
        """
        Run this function to block the main thread. The function is polling
        `self._execution_queue` and executes the plans that are in the queue.
        If the queue is empty, then the thread remains idle.
        """
        # This function blocks the main thread
        while True:
            # Polling 10 times per second. This is fast enough for slowly executed plans.
            ttime.sleep(0.1)
            # Exit the thread if the Event is set (necessary to gracefully close the process)
            if self._exit_event.is_set():
                break
            try:
                plan, is_resuming = self._execution_queue.get(False)
                self._execute_plan(plan, is_resuming)
            except queue.Empty:
                pass

    def _run_in_separate_thread(
        self, *, name, target, run_in_background=True, args=None, kwargs=None, task_uid=None
    ):
        """
        Run ``target`` (any callable) in a separate daemon thread. The callable is passed arguments ``args``
        and ``kwargs`` and ``name`` is used to generate thread name and in error messages. The function
        returns once the thead is started without waiting for the result. The function will generate
        a new task UID if it is not passed as a parameter
        """
        args, kwargs = args or [], kwargs or {}
        status, msg = "accepted", ""

        task_uid = task_uid or str(uuid.uuid4())
        time_start = ttime.time()
        logger.debug(f"Starting task {name!r}. Task UID: {task_uid!r}.")

        try:
            # Verify that the environment is ready
            acceptable_states = (EState.IDLE, EState.EXECUTING_PLAN, EState.EXECUTING_TASK)
            if self._env_state not in acceptable_states:
                raise RejectedError(
                    f"Incorrect environment state: '{self._env_state.value}'. "
                    f"Acceptable states: {[_.value for _ in acceptable_states]}"
                )
            # Verify that the environment is idle (no plans or tasks are executed)
            if not run_in_background and (self._env_state != EState.IDLE):
                raise RejectedError(
                    f"Incorrect environment state: '{self._env_state.value}'. Acceptable state: 'idle'"
                )

            task_uid_short = task_uid.split("-")[-1]
            thread_name = f"BS QServer - {name} {task_uid_short} "

            def thread_func_wrapper(*, task_uid, time_start):
                def thread_func():
                    # This is the function executed in a separate thread
                    try:
                        return_value = target(*args, **kwargs)

                        # Attempt to serialize the result to JSON. The result can not be sent to the client
                        #   if it can not be serialized, so it is better for the function to fail here so that
                        #   proper error message could be sent to the client.
                        try:
                            json.dumps(return_value)  # The result of the conversion is intentionally discarded
                        except Exception as ex_json:
                            raise ValueError(f"Task result can not be serialized as JSON: {ex_json}") from ex_json

                        success, msg = True, ""
                    except Exception as ex:
                        logger.exception("Error occurred while executing the function ('%s'): %s", name, str(ex))
                        return_value = traceback.format_exc()
                        success, msg = False, f"Exception: {str(ex)}"
                    finally:
                        if not run_in_background:
                            self._env_state = EState.IDLE
                            self._running_task_uid = None
                        else:
                            self._background_tasks_num = max(self._background_tasks_num - 1, 0)

                    with self._completed_tasks_lock:
                        task_res = {
                            "task_uid": task_uid,
                            "success": success,
                            "msg": msg,
                            "return_value": return_value,
                            "time_start": time_start,
                            "time_stop": ttime.time(),
                        }
                        self._completed_tasks.append(task_res)

                return thread_func

            th = threading.Thread(
                target=thread_func_wrapper(task_uid=task_uid, time_start=time_start), name=thread_name, daemon=True
            )

            if not run_in_background:
                self._env_state = EState.EXECUTING_TASK
                self._running_task_uid = task_uid
            else:
                self._background_tasks_num += 1

            th.start()

        except RejectedError as ex:
            status, msg = "rejected", f"Task {name!r} was rejected by RE Worker process: {ex}"
        except Exception as ex:
            status, msg = "error", f"Error occurred while to starting the task {name!r}: {ex}"

        logger.debug(
            f"Completing the request to start the task {name!r} ({task_uid!r}): status={status!r} msg={msg!r}."
        )

        # Payload contains information that may be useful for tracking the execution of the task.
        payload = {"task_uid": task_uid, "time_start": time_start, "run_in_background": run_in_background}

        return status, msg, task_uid, payload

    # ------------------------------------------------------------

    def run(self):
        """
        Overrides the `run()` function of the `multiprocessing.Process` class. Called
        by the `start` method.
        """
        setup_console_output_redirection(msg_queue=self._msg_queue)

        logging.basicConfig(level=max(logging.WARNING, self._log_level))
        setup_loggers(name="bluesky_queueserver", log_level=self._log_level)

        success = True

        from .profile_tools import set_re_worker_active, clear_re_worker_active

        self._env_state = EState.INITIALIZING

        # Set the environment variable indicating that RE Worker is active. Status may be
        #   checked using 'is_re_worker_active()' in startup scripts or modules.
        set_re_worker_active()

        self._completed_tasks_lock = threading.Lock()

        from .plan_monitoring import RunList, CallbackRegisterRun

        self._active_run_list = RunList()  # Initialization should be done before communication is enabled.

        self._comm_to_manager.add_method(self._request_state_handler, "request_state")
        self._comm_to_manager.add_method(self._request_plan_report_handler, "request_plan_report")
        self._comm_to_manager.add_method(self._request_run_list_handler, "request_run_list")
        self._comm_to_manager.add_method(
            self._request_plans_and_devices_list_handler, "request_plans_and_devices_list"
        )
        self._comm_to_manager.add_method(self._request_task_results_handler, "request_task_results")
        self._comm_to_manager.add_method(self._command_close_env_handler, "command_close_env")
        self._comm_to_manager.add_method(self._command_confirm_exit_handler, "command_confirm_exit")
        self._comm_to_manager.add_method(self._command_run_plan_handler, "command_run_plan")
        self._comm_to_manager.add_method(self._command_pause_plan_handler, "command_pause_plan")
        self._comm_to_manager.add_method(self._command_continue_plan_handler, "command_continue_plan")
        self._comm_to_manager.add_method(self._command_reset_worker_handler, "command_reset_worker")
        self._comm_to_manager.add_method(self._command_permissions_reload_handler, "command_permissions_reload")

        self._comm_to_manager.add_method(self._command_load_script, "command_load_script")
        self._comm_to_manager.add_method(self._command_execute_function, "command_execute_function")

        self._comm_to_manager.start()

        self._exit_event = threading.Event()
        self._exit_confirmed_event = threading.Event()
        self._re_report_lock = threading.Lock()

        self._allowed_items_lock = threading.Lock()
        self._existing_items_lock = threading.Lock()

        from bluesky import RunEngine
        from bluesky.run_engine import get_bluesky_event_loop
        from bluesky.callbacks.best_effort import BestEffortCallback
        from bluesky_kafka import Publisher as kafkaPublisher
        from bluesky.utils import PersistentDict

        from .profile_tools import global_user_namespace

        # TODO: TC - Do you think that the following code may be included in RE.__init__()
        #   (for Python 3.8 and above)
        # Setting the default event loop is needed to make the code work with Python 3.8.
        loop = get_bluesky_event_loop()
        asyncio.set_event_loop(loop)

        try:
            keep_re = self._config_dict["keep_re"]
            startup_dir = self._config_dict.get("startup_dir", None)
            startup_module_name = self._config_dict.get("startup_module_name", None)
            startup_script_path = self._config_dict.get("startup_script_path", None)

            self._re_namespace = load_worker_startup_code(
                startup_dir=startup_dir,
                startup_module_name=startup_module_name,
                startup_script_path=startup_script_path,
                keep_re=keep_re,
            )

            if keep_re and ("RE" not in self._re_namespace):
                raise RuntimeError(
                    "Run Engine is not created in the startup code and 'keep_re' option is activated."
                )

            epd = existing_plans_and_devices_from_nspace(nspace=self._re_namespace)
            existing_plans, existing_devices, plans_in_nspace, devices_in_nspace = epd

            # self._existing_plans_and_devices_changed = not compare_existing_plans_and_devices(
            #     existing_plans = existing_plans,
            #     existing_devices = existing_devices,
            #     existing_plans_ref = self._existing_plans,
            #     existing_devices_ref = self._existing_devices,
            # )

            # Descriptions of existing plans and devices
            with self._existing_items_lock:
                self._existing_plans, self._existing_devices = existing_plans, existing_devices

            # Dictionaries of references to plans and devices from the namespace
            self._plans_in_nspace = plans_in_nspace
            self._devices_in_nspace = devices_in_nspace

            # Always download existing plans and devices when loading the new environment
            self._existing_plans_and_devices_changed = True

            logger.info("Startup code loading was completed")

        except Exception as ex:
            logger.exception(
                "Failed to start RE Worker environment. Error while loading startup code: %s.",
                str(ex),
            )
            success = False

        if success:
            self._generate_lists_of_allowed_plans_and_devices()
            self._update_existing_pd_file(options=("ENVIRONMENT_OPEN", "ALWAYS"))

            logger.info("Instantiating and configuring Run Engine ...")

            try:
                # Make RE namespace available to the plan code.
                global_user_namespace.set_user_namespace(user_ns=self._re_namespace, use_ipython=False)

                if self._config_dict["keep_re"]:
                    # Copy references from the namespace
                    self._RE = self._re_namespace["RE"]
                    self._db = self._re_namespace.get("db", None)
                else:
                    # Instantiate a new Run Engine and Data Broker (if needed)
                    md = {}
                    if self._config_dict["use_persistent_metadata"]:
                        # This code is temporarily copied from 'nslsii' before better solution for keeping
                        #   continuous sequence Run ID is found. TODO: continuous sequence of Run IDs.
                        directory = os.path.expanduser("~/.config/bluesky/md")
                        os.makedirs(directory, exist_ok=True)
                        md = PersistentDict(directory)

                    self._RE = RunEngine(md)
                    self._re_namespace["RE"] = self._RE

                    def factory(name, doc):
                        # Documents from each run are routed to an independent
                        #   instance of BestEffortCallback
                        bec = BestEffortCallback()
                        return [bec], []

                    # Subscribe to Best Effort Callback in the way that works with multi-run plans.
                    rr = RunRouter([factory])
                    self._RE.subscribe(rr)

                    # Subscribe RE to databroker if config file name is provided
                    self._db = None
                    if "databroker" in self._config_dict:
                        config_name = self._config_dict["databroker"].get("config", None)
                        if config_name:
                            logger.info("Subscribing RE to Data Broker using configuration '%s'.", config_name)
                            from databroker import Broker

                            self._db = Broker.named(config_name)
                            self._re_namespace["db"] = self._db

                            self._RE.subscribe(self._db.insert)

                # Subscribe Run Engine to 'CallbackRegisterRun'. This callback is used internally
                #   by the worker process to keep track of the runs that are open and closed.
                run_reg_cb = CallbackRegisterRun(run_list=self._active_run_list)
                self._RE.subscribe(run_reg_cb)

                if "kafka" in self._config_dict:
                    logger.info(
                        "Subscribing to Kafka: topic '%s', servers '%s'",
                        self._config_dict["kafka"]["topic"],
                        self._config_dict["kafka"]["bootstrap"],
                    )
                    kafka_publisher = kafkaPublisher(
                        topic=self._config_dict["kafka"]["topic"],
                        bootstrap_servers=self._config_dict["kafka"]["bootstrap"],
                        key="kafka-unit-test-key",
                        # work with a single broker
                        producer_config={"acks": 1, "enable.idempotence": False, "request.timeout.ms": 5000},
                        serializer=partial(msgpack.dumps, default=mpn.encode),
                    )
                    self._RE.subscribe(kafka_publisher)

                if "zmq_data_proxy_addr" in self._config_dict:
                    from bluesky.callbacks.zmq import Publisher

                    publisher = Publisher(self._config_dict["zmq_data_proxy_addr"])
                    self._RE.subscribe(publisher)

                self._execution_queue = queue.Queue()

                self._env_state = EState.IDLE

            except BaseException as ex:
                success = False
                logger.exception("Error occurred while initializing the environment: %s.", str(ex))

        if success:
            logger.info("RE Environment is ready")
            self._execute_in_main_thread()
        else:
            self._exit_event.set()

        logger.info("Environment is waiting to be closed ...")
        self._env_state = EState.CLOSING

        # Wait until confirmation is received from RE Manager
        while not self._exit_confirmed_event.is_set():
            ttime.sleep(0.02)

        # Clear the environment variable indicating that RE Worker is active. It is an optional step
        #   since the process is about to close, but we still do it for consistency.
        clear_re_worker_active()

        self._RE = None

        self._comm_to_manager.stop()

        logger.info("Run Engine environment was closed successfully")
