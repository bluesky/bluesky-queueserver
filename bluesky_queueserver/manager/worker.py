import asyncio
import copy
import enum
import json
import logging
import os
import queue
import re
import sys
import threading
import time as ttime
import traceback
import uuid
from functools import partial
from multiprocessing import Process
from threading import Thread

import msgpack
import msgpack_numpy as mpn
from event_model import RunRouter

from .comms import PipeJsonRpcReceive
from .config import profile_name_to_startup_dir
from .logging_setup import PPrintForLogging as ppfl
from .logging_setup import setup_loggers
from .output_streaming import setup_console_output_redirection
from .profile_ops import (
    compare_existing_plans_and_devices,
    existing_plans_and_devices_from_nspace,
    extract_script_root_path,
    load_allowed_plans_and_devices,
    load_script_into_existing_nspace,
    load_worker_startup_code,
    prepare_function,
    prepare_plan,
    update_existing_plans_and_devices,
)

logger = logging.getLogger(__name__)

# Change the variable to change the default behavior
DEFAULT_RUN_FOREGROUND_TASKS_IN_SEPARATE_THREADS = False


# State of the worker environment
class EState(enum.Enum):
    INITIALIZING = "initializing"
    IDLE = "idle"
    FAILED = "failed"
    RESERVED = "reserved"
    EXECUTING_PLAN = "executing_plan"
    EXECUTING_TASK = "executing_task"
    CLOSING = "closing"
    CLOSED = "closed"  # For completeness


# State of IPKernel
class IPKernelState(enum.Enum):
    DISABLED = "disabled"  # Kernel is not started (or worker is in Python mode)
    BUSY = "busy"
    IDLE = "idle"
    STARTING = "starting"


class PlanExecState(enum.Enum):
    RESET = "reset"  # The environment is reset and ready to start a new plan.
    COMPLETED = "completed"  # Plan is not running, results are passed to the manager (report is generated)
    SUBMITTED = "submitted"  # Plan is scheduled, but Run Engine is not started yet
    RUNNING = "running"  # Plan is running or finished, but the results are not passed to the manager.


class ExecOption(enum.Enum):
    NEW = "new"  # New plan
    RESUME = "resume"  # Resume paused plan
    STOP = "stop"  # Stop paused plan (successful completion)
    ABORT = "abort"  # Abort paused plan (failed status)
    HALT = "halt"  # Halt paused plan (failed status, panicked state of RE)
    TASK = "task"  # Execute task (a function, not a plan)


# Expected exit status for supported plan execution options (plans could be paused or fail)
_plan_exit_status_expected = {
    ExecOption.NEW: "completed",
    ExecOption.RESUME: "completed",
    ExecOption.STOP: "stopped",
    ExecOption.ABORT: "aborted",
    ExecOption.HALT: "halted",
}


class RejectedError(RuntimeError): ...


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

        # Indicates if the kernel is captured to run execution loop (kernel is 'busy')
        self._ip_kernel_captured = False
        # Indicates that the execution loop is was started, but was not stopped yet.
        #   Kernel may still remain 'captured' for some time until the updated kernel state is reported.
        self._exec_loop_active = False
        self._exec_loop_active_cnd = None
        self._exit_main_loop_event = None  # Used with IPython kernel
        self._exit_event = None
        self._exit_confirmed_event = None
        self._execution_queue = None

        # Reference to Bluesky Run Engine
        self._RE = None

        # The following variable determine the state of RE Worker
        self._env_state = EState.CLOSED
        self._running_plan_info = None
        self._running_plan_exec_state = PlanExecState.COMPLETED
        self._running_task_uid = None  # UID of the running foreground task (if running a task)

        self._background_tasks_num = 0  # The number of background tasks

        # Report (dict) generated after execution of a command. The report can be downloaded
        #   by RE Manager.
        self._re_report = None
        self._re_report_lock = None  # threading.Lock

        # Class that supports communication over the pipe
        self._comm_to_manager = None

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

        self._use_ipython_kernel = self._config_dict["use_ipython_kernel"]
        self._ip_kernel_app = None  # Reference to IPKernelApp, None if IPython is not used
        self._ip_connect_file = ""  # Filename with connection info for the running IP kernel
        self._ip_connect_info = {}  # Connection info for the running IP Kernel
        self._ip_kernel_client = None  # Kernel client for communication with IP kernel.
        self._ip_kernel_state = IPKernelState.DISABLED
        self._ip_kernel_monitor_stop = False
        # List of message types that are allowed to be printed even if the message does not contain parent header
        self._ip_kernel_monitor_always_allow_types = []
        # The list of collected tracebacks. If the variable is a list, then tracebacks (strings) are appended
        #   to the list. If the variable is None, then tracebacks are not collected.
        self._ip_kernel_monitor_collected_tracebacks = None

        # The event is used to monitor shutdown of IPython kernel.
        self._ip_kernel_is_shut_down_event = None

        # Timeout after which the reserved kernel is released unless a plan or a task is started
        self._ip_kernel_reserve_timeout = 2.0
        # Time when reservation will expire
        self._ip_kernel_reserve_expire_at = 0

        # The list of runs that were opened as part of execution of the currently running plan.
        # Initialized with 'RunList()' in 'run()' method.
        self._active_run_list = None
        self._run_reg_cb = None  # Callback for RE

        self._re_namespace, self._plans_in_nspace, self._devices_in_nspace = {}, {}, {}

        self._worker_shutdown_initiated = False  # Indicates if shutdown is initiated by request
        self._unexpected_shutdown = False  # Indicates if shutdown is in progress, but it was not requested

        self._success_startup = True  # Indicates if worker startup is proceding successfully

    @property
    def re_state(self):
        """
        Returns RE state if possible (RE is a RunEngine object), otherwise returns ``None``.
        """
        try:
            state = str(self._RE.state)
        except Exception:
            state = None
        return state

    @property
    def re_deferred_pause_requested(self):
        """
        Indicates whether the deferred pause was requested if RE is a valid RunEngine object,
        otherwise returns ``False``.
        """
        try:
            try:
                value = self._RE.deferred_pause_requested if self._RE else False
            except AttributeError:
                # TODO: delete this branch once Bluesky supporting
                #   ``RunEngine.deferred_pause_pending``` is widely deployed.
                value = self._RE._deferred_pause_requested if self._RE else False
        except Exception:
            value = False

        return value

    def _execute_plan_or_task(self, parameters, exec_option):
        """
        Execute a plan or a task pulled from ``self._execution_queue``. Note, that the queue
        is used exclusively to pass data between threads and may contain at most 1 element.
        This is not a plan queue. The function is expected to run in the main thread.
        """
        if exec_option == ExecOption.TASK:
            self._execute_task(parameters, exec_option)
        else:
            self._execute_plan(parameters, exec_option)

    def _execute_plan(self, parameters, exec_option):
        """
        Execute a plan. The function is expected to run in the main thread.

        Parameters
        ----------
        parameters: dict
            A dictionary with plan parameters
        exec_option: ExecOption
            Execution option for the plan. See ``ExecOption`` for available options.
        """
        logger.debug("Starting execution of a plan")
        try:
            plan_continue_options = (
                ExecOption.RESUME,
                ExecOption.STOP,
                ExecOption.HALT,
                ExecOption.ABORT,
            )

            if exec_option == ExecOption.NEW:
                func = self._generate_new_plan(parameters)
                self._active_run_list.enable()
            elif exec_option in plan_continue_options:
                func = self._generate_continued_plan(parameters)
            else:
                raise RuntimeError(f"Unsupported plan execution option: {exec_option!r}")

            # ------------------------------------------------------------------------------------------
            # TODO: The purpose of using the thread is to avoid (or minimize) the chance that the state
            # is triggered before RE state changes, which may lead to issues (a report for 'abandoned' plan
            # may be generated before the plan is started). It would be useful to have a RE hook that could
            # be used to monitor state changes or at least hooks for the beginning and the end of
            # the plan (not a run).
            def set_plan_exec_state_as_running():
                ttime.sleep(0.02)  # Short delay: wait until Run Engine starts and switches to IDLE state
                # Make sure that the plan is not finished/failed while we were waiting.
                # The expected state is 'SUBMITTED'.
                if self._running_plan_exec_state != PlanExecState.COMPLETED:
                    self._running_plan_exec_state = PlanExecState.RUNNING

            th = Thread(target=set_plan_exec_state_as_running, daemon=True)
            th.start()
            # -------------------------------------------------------------------------------------------

            result = func()

            uids, scan_ids = self._active_run_list.get_uids(), self._active_run_list.get_scan_ids()

            with self._re_report_lock:
                self._re_report = {
                    "action": "plan_exit",
                    "success": True,
                    "result": result,
                    "uids": uids,
                    "scan_ids": scan_ids,
                    "err_msg": "",
                    "traceback": "",
                    "stop_queue": False,  # True - request manager not to start the next plan
                }
                if exec_option in (ExecOption.NEW, ExecOption.RESUME):
                    self._re_report["plan_state"] = "completed"
                    self._running_plan_exec_state = PlanExecState.COMPLETED
                else:
                    # Here we don't distinguish between stop/abort/halt
                    self._re_report["plan_state"] = _plan_exit_status_expected[exec_option]
                    self._running_plan_exec_state = PlanExecState.COMPLETED

                # Include RE state
                self._re_report["re_state"] = self.re_state

                # Clear the list of active runs
                self._active_run_list.clear()
                self._active_run_list.disable()

                logger.info("The plan was exited. Plan state: %s", self._re_report["plan_state"])

        except BaseException as ex:
            uids, scan_ids = self._active_run_list.get_uids(), self._active_run_list.get_scan_ids()

            with self._re_report_lock:
                self._re_report = {
                    "action": "plan_exit",
                    "uids": uids,
                    "scan_ids": scan_ids,
                    "result": "",
                    "traceback": traceback.format_exc(),
                    "stop_queue": False,  # True - request manager not to start the next plan
                }

                if self.re_state == "paused":
                    # Run Engine was paused
                    self._re_report["plan_state"] = "paused"
                    self._re_report["success"] = True
                    self._re_report["err_msg"] = "The plan is paused"
                    logger.info("The plan is paused ...")

                else:
                    # RE crashed. Plan execution can not be resumed. (Environment may have to be restarted.)
                    # TODO: clarify how this situation must be handled. Also additional error handling
                    #       may be required
                    self._re_report["plan_state"] = "failed"
                    self._re_report["success"] = False
                    self._re_report["err_msg"] = f"Plan failed: {ex}"
                    self._running_plan_exec_state = PlanExecState.COMPLETED

                    # Clear the list of active runs (don't clean the list for the paused plan).
                    self._active_run_list.clear()
                    self._active_run_list.disable()
                    logger.error("The plan failed: %s", self._re_report["err_msg"])

                # Include RE state
                self._re_report["re_state"] = self.re_state

        self._env_state = EState.IDLE
        logger.debug("Plan execution is completed or interrupted")

    def _generate_report_for_abandoned_plan(self):
        """
        Generate report on a completed 'abandoned' plan: the plan that was started by
        the manager, then paused and completed from command line. Used only if the worker
        is using IPython kernel.
        """
        # Plan state based on exit status (returns limited set of states):
        #   'success' may be returned if the plan is completed or stopped, so we pass 'unknown'.
        exit_status_to_plan_state = {"fail": "failed", "abort": "aborted", "success": "unknown"}
        run_list = self._active_run_list.get_run_list()
        exit_status_list = [_["exit_status"] for _ in run_list]
        exit_status = "success"
        for es in exit_status_to_plan_state:
            if es in exit_status_list:
                exit_status = es
                break
        plan_state = exit_status_to_plan_state[exit_status]

        uids, scan_ids = self._active_run_list.get_uids(), self._active_run_list.get_scan_ids()

        with self._re_report_lock:
            self._re_report = {
                "action": "plan_exit",
                "success": plan_state == "success",
                "plan_state": plan_state,
                "result": tuple(uids),  # List of UIDs
                "uids": uids,
                "scan_ids": scan_ids,
                "err_msg": "The plan is completed outside RE Manager",  # List of UIDs
                "traceback": "",
                "stop_queue": True,  # True - request manager not to start the next plan
                "re_state": self.re_state,
            }

        self._running_plan_exec_state = PlanExecState.COMPLETED
        self._active_run_list.clear()
        self._active_run_list.disable()

        logger.debug(f"Plan was completed outside RE Manager. Plan state: {plan_state!r}. Report was generated.")

    def _monitor_abandoned_plans_thread(self):
        """
        The thread is monitoring 'abandoned' plans, for which execution is completed using e.g.
        Jupyter console and generates reports once the plans are completed. The thread is expected
        to run only if the worker is using IPython kernel.
        """
        while True:
            ttime.sleep(0.5)
            # Exit the thread if the Event is set (necessary to gracefully close the process)
            if self._exit_event.is_set():
                break
            if self._use_ipython_kernel and not self._exec_loop_active:
                plan_is_running = self._running_plan_exec_state == PlanExecState.RUNNING
                if plan_is_running and self.re_state in ("idle", "panicked"):
                    self._generate_report_for_abandoned_plan()

    def _execute_task(self, parameters, exec_option):
        """
        Execute a task (a function). The function is expected to run in the main thread.

        Parameters
        ----------
        parameters: dict
            A dictionary with parameters used to generate a function
        exec_option: ExecOption
            Execution option for the plan. Only the value ``ExecOption.TASK`` is accepted.
        """
        logger.debug("Starting execution of a task in main thread ...")
        try:
            if exec_option == ExecOption.TASK:
                func = self._generate_task_func(parameters)
            else:
                raise RuntimeError(f"Unsupported option for executing a task: {exec_option!r}")

            # The function 'func' is self-sufficient: it is responsible for catching and processing
            #   exceptions and handling execution results.
            func()

        except BaseException as ex:
            # The exception was raised while preparing the function for execution.
            logger.exception("Failed to execute task in main thread: %s", ex)
        else:
            logger.debug("Task execution is completed.")

    def _generate_new_plan(self, parameters):
        """
        Generate the function that starts execution of a plan based on plan parameters.
        """
        plan_info = parameters

        try:
            from bluesky.preprocessors import subs_wrapper

            with self._allowed_items_lock:
                allowed_plans, allowed_devices = self._allowed_plans, self._allowed_devices

            plan_parsed = prepare_plan(
                plan_info,
                plans_in_nspace=self._plans_in_nspace,
                devices_in_nspace=self._devices_in_nspace,
                allowed_plans=allowed_plans,
                allowed_devices=allowed_devices,
                nspace=self._re_namespace,
            )

            plan_func = plan_parsed["callable"]
            plan_args_parsed = plan_parsed["args"]
            plan_kwargs_parsed = plan_parsed["kwargs"]
            plan_meta_parsed = plan_parsed["meta"]

            if self.re_state == "panicked":
                raise RuntimeError(
                    "Run Engine is in the 'panicked' state. The environment must be "
                    "closed and opened again before plans could be executed."
                )
            elif self.re_state not in ("idle", None):
                raise RuntimeError(f"Run Engine is in {self.re_state!r} state. Stop or finish any running plan.")

            def get_start_plan_func(plan_func, plan_args, plan_kwargs, plan_meta):
                def start_plan_func():
                    g = subs_wrapper(plan_func(*plan_args, **plan_kwargs), {"all": [self._run_reg_cb]})
                    return self._RE(g, **plan_meta)

                return start_plan_func

            return get_start_plan_func(plan_func, plan_args_parsed, plan_kwargs_parsed, plan_meta_parsed)

        except Exception as ex:
            logger.exception(ex)
            raise RuntimeError(
                f"Error occurred while processing plan parameters or starting the plan: {ex}"
            ) from ex

    def _generate_continued_plan(self, parameters):
        """
        Generate a function that resumes/aborts/stops/halts execution of a paused plan.
        """
        option = parameters["option"]
        available_options = ("resume", "abort", "stop", "halt")

        if self.re_state == "panicked":
            raise RuntimeError(
                "Run Engine is in the 'panicked' state. "
                "The worker environment must be closed and reopened before plans could be executed."
            )
        elif self.re_state != "paused":
            raise RuntimeError(f"Run Engine is in {self.re_state!r} state. Only 'paused' plan can be continued.")
        elif option not in available_options:
            raise RuntimeError(f"Option '{option}' is not supported. Supported options: {available_options}")

        def get_continued_plan_func(option):
            def continued_plan_func():
                return getattr(self._RE, option)()

            return continued_plan_func

        return get_continued_plan_func(option)

    def _generate_task_func(self, parameters):
        """
        Generate function for execution of a task (target function). The function is
        performing all necessary steps to complete execution of the task and report
        the results and could be executed in main or background thread.
        """
        name = parameters["name"]
        task_uid = parameters["task_uid"]
        time_start = parameters["time_start"]
        target = parameters["target"]
        target_args = parameters["target_args"]
        target_kwargs = parameters["target_kwargs"]
        run_in_background = parameters["run_in_background"]
        run_in_separate_thread = parameters["run_in_separate_thread"]

        def task_func():
            # This is the function executed in a separate thread
            try:
                # Use set Run Engine event loop as a current loop for this thread
                if (run_in_background or run_in_separate_thread) and hasattr(self._RE, "loop"):
                    asyncio.set_event_loop(self._RE.loop)

                return_value = target(*target_args, **target_kwargs)

                # Attempt to serialize the result to JSON. The result can not be sent to the client
                #   if it can not be serialized, so it is better for the function to fail here so that
                #   proper error message could be sent to the client.
                try:
                    json.dumps(return_value)  # The result of the conversion is intentionally discarded
                except Exception as ex_json:
                    raise ValueError(f"Task result can not be serialized as JSON: {ex_json}") from ex_json

                success, err_msg, err_tb = True, "", ""
            except BaseException as ex:
                s = f"Error occurred while executing {name!r}"
                err_msg = f"{s}: {str(ex)}"
                if hasattr(ex, "tb"):  # ScriptLoadingError
                    err_tb = str(ex.tb)
                else:
                    err_tb = traceback.format_exc()
                logger.error("%s:\n%s\n", err_msg, err_tb)

                return_value, success = None, False
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
                    "msg": err_msg,
                    "traceback": err_tb,
                    "return_value": return_value,
                    "time_start": time_start,
                    "time_stop": ttime.time(),
                }
                self._completed_tasks.append(task_res)

        return task_func

    def _start_new_plan(self, plan_info):
        """
        Loads a new plan into `self._execution_queue`. The plan plan name and
        device names are represented as strings. Parsing of the plan in this
        function replaces string representation with references.

        Parameters
        ----------
        plan_info: dict
            Dictionary with plan parameters
        """
        if self._env_state not in (EState.IDLE, EState.RESERVED):
            raise RejectedError(
                f"Attempted to start a plan in '{self._env_state.value}' environment state. Accepted state: 'idle'"
            )

        # Save reference to the currently executed plan
        self._running_plan_info = plan_info
        self._running_plan_exec_state = PlanExecState.SUBMITTED
        self._env_state = EState.EXECUTING_PLAN

        logger.info("Starting a plan '%s'.", plan_info["name"])
        self._execution_queue.put((plan_info, ExecOption.NEW))

    def _continue_plan(self, option):
        """
        Continue/stop execution of a plan after it was paused.

        Parameters
        ----------
        option: str
            Option on how to proceed with previously paused plan. The values are
            "resume", "abort", "stop", "halt".
        """
        available_options = ("resume", "abort", "stop", "halt")

        if option not in available_options:
            raise RuntimeError(f"Option '{option}' is not supported. Supported options: {available_options}")

        if self._env_state not in (EState.IDLE, EState.RESERVED):
            raise RejectedError(
                f"Attempted to {option} a plan in '{self._env_state.value}' environment state. "
                f"Accepted state: '{EState.IDLE.value}'"
            )

        self._env_state = EState.EXECUTING_PLAN

        logger.info("Continue plan execution with the option '%s'", option)
        self._execution_queue.put(({"option": option}, ExecOption(option)))

    def _start_task(
        self,
        *,
        name,
        target,
        target_args=None,
        target_kwargs=None,
        run_in_background=True,
        run_in_separate_thread=DEFAULT_RUN_FOREGROUND_TASKS_IN_SEPARATE_THREADS,
        task_uid=None,
    ):
        """
        Run ``target`` (any callable) in the main thread or a separate daemon thread. The callable
        is passed arguments ``target_args`` and ``target_kwargs``. The function returns once
        the generated function is passed to the main thread, started in a background thead or fails
        to start. The function is not waiting for the execution result.

        Parameters
        ----------
        name: str
            The name used in background thread name and error messages.
        target: callable
            Callable (function or method) to execute.
        target_args: list
            List of target args (passed to ``target``).
        target_kwargs: dict
            Dictionary of target kwargs (passed to ``target``).
        run_in_background: boolean
            Run as a background task (in a background thread) if ``True``, run as a foreground
            task otherwise. The foreground task may be run in a separate thread or in the main
            thread depending on ``run_in_separate_thread`` parameter.
        run_in_separate_thread: boolean
            Run a foreground task in a separate thread if ``True`` or in the main thread otherwise.
            Background tasks are always run in a separate background thread.
        task_uid: str or None
            UID of the task. If ``None``, then the new UID is generated.
        """
        target_args, target_kwargs = target_args or [], target_kwargs or {}
        status, msg = "accepted", ""

        task_uid = task_uid or str(uuid.uuid4())
        time_start = ttime.time()
        logger.debug("Starting task '%s'. Task UID: '%s'.", name, task_uid)

        try:
            # Verify that the environment is ready
            acceptable_states = (EState.IDLE, EState.RESERVED, EState.EXECUTING_PLAN, EState.EXECUTING_TASK)
            if self._env_state not in acceptable_states:
                raise RejectedError(
                    f"Incorrect environment state: '{self._env_state.value}'. "
                    f"Acceptable states: {[_.value for _ in acceptable_states]}"
                )
            # Verify that the environment is idle (no plans or tasks are executed)
            if not run_in_background and (self._env_state not in (EState.IDLE, EState.RESERVED)):
                raise RejectedError(
                    f"Incorrect environment state: '{self._env_state.value}'. "
                    "Acceptable states: 'idle', 'reserved'"
                )

            if not run_in_background:
                if self._use_ipython_kernel and not self._exec_loop_active:
                    if not self._ip_kernel_capture():
                        raise RejectedError("Failed to start execution loop ('capture' the kernel)")

            task_uid_short = task_uid.split("-")[-1]
            thread_name = f"BS QServer - {name} {task_uid_short} "

            parameters = {
                "name": name,
                "task_uid": task_uid,
                "time_start": time_start,
                "target": target,
                "target_args": target_args,
                "target_kwargs": target_kwargs,
                "run_in_background": run_in_background,
                "run_in_separate_thread": run_in_separate_thread,
            }
            # Generate the target function even if it is not used here to validate parameters
            #   (if it is executed in the main thread), because this is the right place to fail.
            target_func = self._generate_task_func(parameters)

            if run_in_background:
                self._background_tasks_num += 1
            else:
                self._env_state = EState.EXECUTING_TASK
                self._running_task_uid = task_uid

            if run_in_background or run_in_separate_thread:
                th = threading.Thread(target=target_func, name=thread_name, daemon=True)
                th.start()
            else:
                self._execution_queue.put((parameters, ExecOption.TASK))

        except RejectedError as ex:
            status, msg = "rejected", f"Task {name!r} was rejected by RE Worker process: {ex}"
        except BaseException as ex:
            status, msg = "error", f"Error occurred while starting the task {name!r}: {ex}"

        logger.debug(
            "Completing the request to start the task '%s' ('%s'): status='%s' msg='%s'.",
            name,
            task_uid,
            status,
            msg,
        )

        # Payload contains information that may be useful for tracking the execution of the task.
        payload = {"task_uid": task_uid, "time_start": time_start, "run_in_background": run_in_background}

        return status, msg, task_uid, payload

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

    def _load_script_into_environment(self, *, script, update_lists, update_re):
        """
        Load script passed as a string variable (``script``) into RE environment namespace.
        Boolean variable ``update_re`` controls whether ``RE`` and ``db`` are updated if
        the new values are defined in the script. Boolean variable ``update_lists`` controls
        if lists of existing and available plans and devices are updated after execution
        of the script.
        """
        startup_dir = self._config_dict.get("startup_dir", None)
        startup_module_name = self._config_dict.get("startup_module_name", None)
        startup_script_path = self._config_dict.get("startup_script_path", None)

        startup_profile = self._config_dict.get("startup_profile", None)
        ipython_dir = self._config_dict.get("ipython_dir", None)

        script_root_path = extract_script_root_path(
            startup_dir=startup_dir,
            startup_module_name=startup_module_name,
            startup_script_path=startup_script_path,
        )

        script_root_path = script_root_path or profile_name_to_startup_dir(startup_profile, ipython_dir)

        load_script_into_existing_nspace(
            script=script,
            nspace=self._re_namespace,
            script_root_path=script_root_path,
            update_re=update_re,
        )

        if update_re:
            if ("RE" in self._re_namespace) and (self._RE != self._re_namespace["RE"]):
                self._RE = self._re_namespace["RE"]
                logger.info("Run Engine instance ('RE') was replaced.")

            if ("db" in self._re_namespace) and (self._db != self._re_namespace["db"]):
                self._db = self._re_namespace["db"]
                logger.info("Data Broker instance ('db') was replaced.")

        if update_lists:
            logger.info("Updating lists of existing and available plans and devices ...")

            epd = existing_plans_and_devices_from_nspace(
                nspace=self._re_namespace,
                ignore_invalid_plans=self._config_dict["ignore_invalid_plans"],
                max_depth=self._config_dict["device_max_depth"],
            )
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

            if script:
                logger.info("The script was successfully loaded into RE environment")
            else:
                logger.info("RE environment was successfully updated")

        else:
            # The script was executed, but the lists were not updated and may be out of sync.
            #   This option saves time, but should be used only to run scripts that do not add,
            #   delete or modify plans and devices. The script may add functions to the namespace.
            logger.info("The script was successfully executed in RE environment")

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
        logger.debug("The execution of the function '%s' was successfully completed.", func_name)

        return return_value

    # =============================================================================
    #               Handlers for messages from RE Manager

    def _request_state_handler(self):
        """
        Returns the state information of RE Worker environment.
        """
        item_uid = self._running_plan_info["item_uid"] if self._running_plan_info else None
        task_uid = self._running_task_uid
        plan_completed = self._running_plan_exec_state == PlanExecState.COMPLETED
        re_state = self.re_state
        re_deferred_pause_requested = self.re_deferred_pause_requested
        env_state_str = self._env_state.value
        re_report_available = self._re_report is not None
        run_list_updated = self._active_run_list.is_changed()  # True - updates are available
        plans_and_devices_list_updated = self._existing_plans_and_devices_changed
        completed_tasks_available = bool(self._completed_tasks)
        background_tasks_num = self._background_tasks_num
        ip_kernel_state = self._ip_kernel_state.value
        ip_kernel_captured = self._ip_kernel_captured
        unexpected_shutdown = self._unexpected_shutdown
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
            "ip_kernel_state": ip_kernel_state,
            "ip_kernel_captured": ip_kernel_captured,
            "unexpected_shutdown": unexpected_shutdown,
        }
        return msg_out

    def _request_ip_connect_info(self):
        """
        Return IP connect info obtained from IPython kernel. Returns ``{}`` if
        worker is using pure Python.
        """
        connect_info = copy.deepcopy(self._ip_connect_info)
        connect_info["key"] = connect_info["key"].decode("utf-8")
        return {"ip_connect_info": connect_info}

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

        if self._ip_kernel_state == IPKernelState.BUSY:
            # The condition for IP Kernel 'busy' state accounts for the case when IP is not used.
            status = "rejected"
            err_msg = "IPython kernel is busy and can not be stopped."
        elif self.re_state == "running":
            status = "rejected"
            err_msg = "Run Engine is executing a plan.Stop the running plan and try again."
        else:
            try:
                if self._use_ipython_kernel:
                    # Send 'quit' command to the kernel, 'exit_event' is set elsewhere.
                    self._ip_kernel_shutdown()
                else:
                    self._exit_event.set()
                self._worker_shutdown_initiated = True  # Shutdown is initiated by request
                status = "accepted"
            except Exception as ex:
                status = "error"
                err_msg = str(ex)
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

    def _command_reserve_kernel_handler(self):
        status, err_msg = "accepted", ""
        logger.debug("Attempting to reserve (capture) IPython kernel ...")
        if not self._ip_kernel_capture(timeout=0.2):
            status, err_msg = "failed", "Timeout occurred while trying to reserve IPython kernel"
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
        elif self.re_state == "running":
            invalid_state = 2
        elif self._running_plan_info or (self._running_plan_exec_state != PlanExecState.RESET):
            invalid_state = 3
        elif self._use_ipython_kernel and not self._exec_loop_active:
            if not self._ip_kernel_capture():
                # This error is unlikely to happen. The possible issue is that the main thread
                #   was blocked by another client in the process of starting the loop.
                #   This is possible during normal use, but probablity is very low.
                invalid_state = 4

        err_msg = ""
        if not invalid_state:  # == 0
            try:
                # Value is a dictionary with plan parameters
                self._start_new_plan(plan_info=plan_info)
                status = "accepted"
            except RejectedError as ex:
                status = "rejected"
                err_msg = str(ex)
            except Exception as ex:
                status = "error"
                err_msg = str(ex)
        elif invalid_state == 2 and self._use_ipython_kernel:
            # Since IPython kernel may be accessed bypassing the manager, and users may
            #   run plans using other clients (e.g. jupyter console). This state is still
            #   invalid, but is likely to occur during normal use. There is no need to print
            #   scary error message.
            status = "rejected"
            err_msg = f"Run Engine must be in 'idle' state to start plans. Current state: {self.re_state!r}"
        else:
            status = "rejected"
            msg_list = [
                "the execution queue is not empty",
                "another plan is running",
                "worker is not reset after completion of the previous plan",
                "failing to start execution loop ('capture' the IPython kernel)",
            ]
            try:
                s = msg_list[invalid_state - 1]
            except Exception:
                s = "UNKNOWN CONDITION IS DETECTED IN THE WORKER PROCESS"  # Shouldn't ever be printed
            err_msg = (
                f"Attempt to start a plan (Run Engine) while {s}.\n"
                "This may indicate a serious issue with the plan queue execution mechanism.\n"
                "Please report the issue to developer team."
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
        if self.re_state == "running":
            try:
                if option not in pausing_options:
                    raise RuntimeError(f"Option '{option}' is not supported. Available options: {pausing_options}")

                defer = {"deferred": True, "immediate": False}[option]
                # The official 'request_pause' public function is blocking and does not work very well here,
                #   because if RE event loop is blocked (a plan is stuck in the infinite loop), the operation
                #   will never complete and block communication thread of the worker.
                # self._RE.request_pause(defer=defer)  # WILL BLOCK THE LOOP
                asyncio.run_coroutine_threadsafe(self._RE._request_pause_coro(defer), loop=self._RE.loop)
                status = "accepted"
            except Exception as ex:
                status = "error"
                err_msg = str(ex)
        else:
            status = "rejected"
            err_msg = "Run engine can be paused only in 'running' state. " f"Current state: '{self.re_state}'"

        msg_out = {"status": status, "err_msg": err_msg}
        return msg_out

    def _command_continue_plan_handler(self, *, option):
        """
        Continue execution of a paused plan. Options: `resume`, `stop`, `abort` and `halt`.
        """
        # Continue execution of the plan
        status, err_msg = "accepted", ""

        if self.re_state != "paused":
            status = "rejected"
            err_msg = f"Run Engine must be in 'paused' state to continue. The state is {self.re_state!r}"
        else:
            if self._use_ipython_kernel and not self._exec_loop_active:
                if not self._ip_kernel_capture():
                    status = "rejected"
                    err_msg = "Timeout occurred while trying to start the execution loop"

            if status != "rejected":
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

        msg_out = {"status": status, "err_msg": err_msg}
        return msg_out

    def _command_reset_worker_handler(self):
        """
        Reset state of RE Worker environment (prepare for execution of a new plan)
        """
        err_msg = ""
        if self.re_state in ("idle", None):
            self._running_plan_info = None
            self._running_plan_exec_state = PlanExecState.RESET
            with self._re_report_lock:
                self._re_report = None
            status = "accepted"
        else:
            status = "rejected"
            err_msg = f"Run Engine must be in 'idle' state to continue. The state is {self.re_state!r}"

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
        status, err_msg, task_uid, payload = self._start_task(
            name="Reload Permissions",
            target=self._generate_lists_of_allowed_plans_and_devices,
            run_in_background=True,
        )
        msg_out = {"status": status, "err_msg": err_msg, "task_uid": task_uid, "payload": payload}
        return msg_out

    def _command_load_script(self, script, update_lists, update_re, run_in_background):
        """
        Load the script passed as a string variable into the existing RE environment.
        The task could be started in the background (when a plan or another foreground task
        is running), but it is not recommended.
        """
        status, err_msg, task_uid, payload = self._start_task(
            name="Load script",
            target=self._load_script_into_environment,
            target_kwargs={"script": script, "update_lists": update_lists, "update_re": update_re},
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

        status, err_msg, task_uid, payload = self._start_task(
            name="Execute function",
            target=self._execute_function_in_environment,
            target_kwargs={"func_info": func_info},
            run_in_background=run_in_background,
            task_uid=task_uid,
        )
        msg_out = {"status": status, "err_msg": err_msg, "task_uid": task_uid, "payload": payload}
        return msg_out

    def _command_exec_loop_stop_handler(self):
        """
        Initiate stopping the execution loop. Call fails if the worker is running on Python
        (not IPython kernel).
        """
        try:
            success = self._ip_kernel_release()
            status = "accepted" if success else "rejected"
            err_msg = "Failed to initiate stopping the execution loop" if not success else ""
        except Exception as ex:
            status, err_msg = "rejected", f"Error: {ex}"

        return {"status": status, "err_msg": err_msg}

    def _command_interrupt_kernel_handler(self, interrupt_task, interrupt_plan):
        """
        Initiate stopping the execution loop. Call fails if the worker is running on Python
        (not IPython kernel).
        """
        logger.debug("Interrupting kernel ...")
        try:
            status, err_msg = "accepted", ""

            # The same checks are already performed in the manager, but we repeat them here with
            #   more up-to-date information
            if not interrupt_plan and self._env_state == EState.EXECUTING_PLAN:
                raise RuntimeError("Not allowed to interrupt running plan")

            if not interrupt_task and self._env_state == EState.EXECUTING_TASK:
                raise RuntimeError("Not allowed to interrupt running task")

            msg = self._ip_kernel_client.session.msg("interrupt_request", content={})
            self._ip_kernel_client.control_channel.send(msg)

        except Exception as ex:
            status, err_msg = "rejected", f"Error: {ex}"

        return {"status": status, "err_msg": err_msg}

    # ------------------------------------------------------------

    def _execute_in_main_thread(self):
        """
        Run this function to block the main thread. The function is polling
        `self._execution_queue` and executes the plans that are in the queue.
        If the queue is empty, then the thread remains idle.
        """
        # This function blocks the main thread
        try:
            with self._exec_loop_active_cnd:
                self._ip_kernel_captured = True
                self._exec_loop_active = True
                self._exec_loop_active_cnd.notify_all()

            self._exit_main_loop_event.clear()
            while True:
                try:
                    parameters, plan_exec_option = self._execution_queue.get(block=True, timeout=0.1)
                    self._execute_plan_or_task(parameters, plan_exec_option)
                except queue.Empty:
                    pass

                # Exit the thread if the Event is set (necessary to gracefully close the process)
                if self._exit_event.is_set() or self._exit_main_loop_event.is_set():
                    break
                if (self._env_state == EState.RESERVED) and (self._ip_kernel_reserve_expire_at < ttime.time()):
                    self._env_state = EState.IDLE
                    break
        finally:
            with self._exec_loop_active_cnd:
                if not self._use_ipython_kernel:
                    self._ip_kernel_captured = False
                self._exec_loop_active = False
                self._exec_loop_active_cnd.notify_all()

            self._exit_main_loop_event.clear()

    # ------------------------------------------------------------

    def _worker_prepare_for_startup(self):
        """
        Operations necessary to prepare for worker startup (before loading)
        """
        from .plan_monitoring import CallbackRegisterRun
        from .profile_tools import set_ipython_mode, set_re_worker_active

        self._ip_kernel_is_shut_down_event = threading.Event()  # Used with IPython kernel

        # Set the environment variable indicating that RE Worker is active. Status may be
        #   checked using 'is_re_worker_active()' in startup scripts or modules.
        set_re_worker_active()
        set_ipython_mode(self._use_ipython_kernel)

        self._completed_tasks_lock = threading.Lock()

        from .plan_monitoring import RunList

        self._active_run_list = RunList()  # Initialization should be done before communication is enabled.
        self._run_reg_cb = CallbackRegisterRun(run_list=self._active_run_list)

        # Class that supports communication over the pipe
        self._comm_to_manager = PipeJsonRpcReceive(conn=self._conn, use_json=False, name="RE Worker-Manager Comm")

        self._comm_to_manager.add_method(self._request_state_handler, "request_state")
        self._comm_to_manager.add_method(self._request_ip_connect_info, "request_ip_connect_info")
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

        self._comm_to_manager.add_method(self._command_reserve_kernel_handler, "command_reserve_kernel")
        self._comm_to_manager.add_method(self._command_exec_loop_stop_handler, "command_exec_loop_stop")
        self._comm_to_manager.add_method(self._command_interrupt_kernel_handler, "command_interrupt_kernel")

        self._comm_to_manager.add_method(self._command_load_script, "command_load_script")
        self._comm_to_manager.add_method(self._command_execute_function, "command_execute_function")

        self._comm_to_manager.start()

        self._ip_kernel_captured = False
        self._exec_loop_active = False
        self._exec_loop_active_cnd = threading.Condition()
        self._exit_main_loop_event = threading.Event()
        self._exit_event = threading.Event()
        self._exit_confirmed_event = threading.Event()
        self._re_report_lock = threading.Lock()

        self._allowed_items_lock = threading.Lock()
        self._existing_items_lock = threading.Lock()

        from bluesky.run_engine import get_bluesky_event_loop

        # Setting the default event loop is needed to make the code work with Python 3.8.
        loop = get_bluesky_event_loop() or asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    def _worker_startup_code(self):
        """
        Perform startup tasks for the worker.
        """
        from bluesky import RunEngine
        from bluesky.callbacks.best_effort import BestEffortCallback
        from bluesky.utils import PersistentDict
        from bluesky_kafka import Publisher as kafkaPublisher

        from .profile_tools import global_user_namespace

        try:
            keep_re = self._config_dict["keep_re"]
            startup_dir = self._config_dict.get("startup_dir", None)
            startup_module_name = self._config_dict.get("startup_module_name", None)
            startup_script_path = self._config_dict.get("startup_script_path", None)

            ipython_matplotlib = self._config_dict.get("ipython_matplotlib", None)

            # If IPython kernel is used, the startup code is loaded during kernel initialization.
            if not self._use_ipython_kernel:
                self._re_namespace = load_worker_startup_code(
                    startup_dir=startup_dir,
                    startup_module_name=startup_module_name,
                    startup_script_path=startup_script_path,
                    keep_re=keep_re,
                    nspace=self._re_namespace,
                )

            if keep_re and ("RE" not in self._re_namespace):
                raise RuntimeError(
                    "Run Engine is not created in the startup code and 'keep_re' option is activated."
                )

            epd = existing_plans_and_devices_from_nspace(
                nspace=self._re_namespace,
                ignore_invalid_plans=self._config_dict["ignore_invalid_plans"],
                max_depth=self._config_dict["device_max_depth"],
            )
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

            logger.info("Startup code was successfully loaded.")

        except BaseException as ex:
            s = "Failed to start RE Worker environment. Error while loading startup code"
            if hasattr(ex, "tb"):  # ScriptLoadingError
                logger.error("%s:\n%s\n", s, ex.tb)
            else:
                logger.exception("%s: %s.", s, ex)
            self._success_startup = False

        if self._success_startup:
            self._generate_lists_of_allowed_plans_and_devices()
            self._update_existing_pd_file(options=("ENVIRONMENT_OPEN", "ALWAYS"))

            logger.info("Instantiating and configuring Run Engine ...")

            try:
                # Make RE namespace available to the plan code.
                global_user_namespace.set_user_namespace(
                    user_ns=self._re_namespace, use_ipython=self._use_ipython_kernel
                )

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
                        if not self._use_ipython_kernel or not ipython_matplotlib:
                            bec.disable_plots()
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

                # If IPython kernel is used, then the environment state should be updated
                #     once the kernel is 'idle'
                if not self._use_ipython_kernel:
                    self._env_state = EState.IDLE

                logger.info("RE Environment is ready")

            except BaseException as ex:
                self._success_startup = False
                logger.exception("Error occurred while initializing the environment: %s.", ex)

        if not self._success_startup:
            self._env_state = EState.FAILED

    def _worker_shutdown_code(self):
        """
        Perform shutdown tasks for the worker.
        """
        from .profile_tools import clear_ipython_mode, clear_re_worker_active

        # If shutdown was not initiated by request from manager, then the manager needs to know this,
        #   since it still needs to send a request to the worker to confirm the orderly exit.
        if not self._worker_shutdown_initiated:
            self._unexpected_shutdown = True

        logger.info("Environment is waiting to be closed ...")
        self._env_state = EState.CLOSING

        # Wait until confirmation is received from RE Manager
        while not self._exit_confirmed_event.is_set():
            ttime.sleep(0.02)

        # Clear the environment variable indicating that RE Worker is active. It is an optional step
        #   since the process is about to close, but we still do it for consistency.
        clear_re_worker_active()
        clear_ipython_mode()

        self._RE = None

        self._comm_to_manager.stop()

    def _run_loop_python(self):
        """
        Run loop (Python kernel). The loop is blocking the main thread until the environment is closed.
        """
        if self._success_startup:
            self._execute_in_main_thread()
        else:
            self._exit_event.set()

    def _run_loop_ipython(self):
        """
        Run loop (IPython kernel). The loop is blocking IPython kernel main thread while the queue
        (or other foreground task) is running, 'capturing' the kernel.
        """
        self._ip_kernel_reserve_expire_at = ttime.time() + self._ip_kernel_reserve_timeout
        self._env_state = EState.RESERVED
        self._execute_in_main_thread()

    def _ip_kernel_capture(self, timeout=0.5):
        """
        'Capture' IPython kernel by starting an execution loop. Once the kernel is 'captured', the server
        may start submitting tasks. Returns True if the execution loop was started and False otherwise.
        If the loop was not started because of timeout, it may start later, but it will exit quickly
        without executing any tasks.
        """
        if not self._use_ipython_kernel:
            return True
        if self._ip_kernel_state != IPKernelState.IDLE:
            if self._env_state == EState.RESERVED:
                self._ip_kernel_reserve_expire_at = ttime.time() + self._ip_kernel_reserve_timeout
                return True
            else:
                return False

        start_loop_task = "___ip_execution_loop_start___()"
        self._ip_kernel_execute_command(command=start_loop_task)
        with self._exec_loop_active_cnd:
            success = self._exec_loop_active_cnd.wait_for(lambda: self._exec_loop_active, timeout=timeout)
        return success

    def _ip_kernel_release(self):
        """
        Initiate the release of captured loop (only for IPython kernel).
        The loop is considered released once the kernel returns to the 'idle' state.
        """
        if self._use_ipython_kernel:
            self._exit_main_loop_event.set()
            return True
        else:
            return False

    def _ip_kernel_iopub_monitor_thread(self, output_stream, error_stream):
        while True:
            if self._ip_kernel_monitor_stop:
                break

            try:
                msg = self._ip_kernel_client.get_iopub_msg(timeout=0.5)
                if msg["header"]["msg_type"] == "status":
                    self._ip_kernel_state = IPKernelState(msg["content"]["execution_state"])
                    # Set kernel as not captured if the exec loop was stopped and we are
                    #   waiting for the kernel to become idle.
                    if (
                        self._ip_kernel_captured
                        and not self._exec_loop_active
                        and self._ip_kernel_state == IPKernelState.IDLE
                    ):
                        self._ip_kernel_captured = False

                    if (self._env_state == EState.INITIALIZING) and (self._ip_kernel_state == IPKernelState.IDLE):
                        logger.info("IPython kernel is in 'idle' state")
                        self._env_state = EState.IDLE

                try:
                    discard = msg["header"]["msg_type"] not in self._ip_kernel_monitor_always_allow_types
                    if discard and "parent_header" in msg and msg["parent_header"]:
                        session_id = self._ip_kernel_client.session.session
                        discard = msg["parent_header"]["session"] != session_id

                    if not discard:
                        if msg["header"]["msg_type"] == "stream":
                            stream_name = msg["content"]["name"]
                            stream_text = msg["content"]["text"]
                            if stream_name == "stdout":
                                output_stream.write(stream_text)
                            elif stream_name == "stderr":
                                error_stream.write(stream_text)
                        elif msg["header"]["msg_type"] == "error":
                            tb = msg["content"]["traceback"]
                            # Remove escape characters from traceback
                            ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
                            tb = [ansi_escape.sub("", _) for _ in tb]
                            tb = "\n".join(tb)
                            if self._ip_kernel_monitor_collected_tracebacks is not None:
                                self._ip_kernel_monitor_collected_tracebacks.append(tb)
                            print(f"Traceback: {tb}", file=error_stream)
                        elif msg["header"]["msg_type"] == "execute_result":
                            res = msg["content"]["data"]["text/plain"]
                            print(f">> {res}", file=output_stream)
                except KeyError:
                    pass
            except queue.Empty:
                pass
            except BaseException as ex:
                logger.exception(ex)

    def _ip_kernel_execute_command(self, *, command: str, except_on: bool = False):
        try:
            self._ip_kernel_client.execute(command, reply=False, store_history=False)
        except Exception as ex:
            if except_on:
                raise
            logger.exception(
                "Error occurred while sending request to IPython kernel: Command: %r.\n%s", command, ex
            )

    def _ip_kernel_shutdown_thread(self):
        """
        Simply sending 'shutdown_request' or 'quit' to the kernel does not always work.
        It was found that on the beamline machines the kernel does not shut down until
        it receives additional command (e.g. the kernel does not quit until jupyter console
        application is started). The following procedure sends periodic requests to the
        kernel for 20 seconds and then kills the kernel (stops ioloop) if it did not quit.
        It may look like overkill, since in simulated environments the kernel quits immediately
        after shutdown request, and on the beamline machines after the first request
        (to execute an empty cell), but it is may be important that the operation of closing
        the environment works reliably.
        """
        logger.info("Requesting kernel to shut down ...")
        self._ip_kernel_client.shutdown()  # Sends 'shutdown_request' to the kernel

        # Alternative method is to send 'quit' command. It seems like 0MQ sockets
        #   of the client may be closed explicitly. TODO: should shutdown or 'quit' be used?
        # self._ip_kernel_execute_command(command="quit")

        timeout = 20  # This is time before the kernel is terminated. It can be parametrized if necessary.
        t_stop = ttime.time() + timeout
        while not self._ip_kernel_is_shut_down_event.wait(1):
            if ttime.time() > t_stop:
                break
            logger.debug("Sending 'quit' command to IP kernel")
            self._ip_kernel_execute_command(command="quit")

        if not self._ip_kernel_is_shut_down_event.is_set():
            logger.info("Kernel failed to stop normaly. Killing the ioloop ...")
            self._ip_kernel_app.io_loop.stop()

        logger.debug("Request to shutdown IP kernel is completed. Exiting the thread ...")

    def _ip_kernel_shutdown(self):
        # The manager is now designed not to send repeated requests to stop the environment.
        #   This code needs to be revised if this behavior is changed.
        self._ip_kernel_is_shut_down_event.clear()

        th = threading.Thread(target=self._ip_kernel_shutdown_thread, daemon=True)
        th.start()

    def _ip_kernel_startup_init(self):
        with self._exec_loop_active_cnd:
            self._ip_kernel_captured = True
            self._exec_loop_active = False  # Loop is not running
            self._exec_loop_active_cnd.notify_all()

    def run(self):
        """
        Overrides the `run()` function of the `multiprocessing.Process` class. Called
        by the `start` method.
        """
        setup_console_output_redirection(msg_queue=self._msg_queue)
        # No output should be printed directly on the screen
        sys.__stdout__, sys.__stderr__ = sys.stdout, sys.stderr

        logging.basicConfig(level=max(logging.WARNING, self._log_level))
        setup_loggers(name="bluesky_queueserver", log_level=self._log_level)

        self._success_startup = True
        self._env_state = EState.INITIALIZING

        if not self._use_ipython_kernel:
            self._worker_prepare_for_startup()
            self._worker_startup_code()
            self._run_loop_python()
        else:
            import socket

            from ipykernel.kernelapp import IPKernelApp

            from .utils import generate_random_port

            self._re_namespace["___ip_execution_loop_start___"] = self._run_loop_ipython
            self._re_namespace["___ip_kernel_startup_init___"] = self._ip_kernel_startup_init
            self._ip_kernel_app = IPKernelApp.instance(user_ns=self._re_namespace)
            out_stream, err_stream = sys.stdout, sys.stderr

            self._worker_prepare_for_startup()

            startup_profile = self._config_dict.get("startup_profile", None)
            startup_module_name = self._config_dict.get("startup_module_name", None)
            startup_script_path = self._config_dict.get("startup_script_path", None)
            ipython_dir = self._config_dict.get("ipython_dir", None)
            ipython_matplotlib = self._config_dict.get("ipython_matplotlib", None)

            if startup_profile:
                self._ip_kernel_app.profile = startup_profile
            if startup_module_name:
                # NOTE: Startup files are still loaded.
                self._ip_kernel_app.module_to_run = startup_module_name
            if startup_script_path:
                # NOTE: Startup files are still loaded.
                self._ip_kernel_app.file_to_run = startup_script_path
            if ipython_dir:
                self._ip_kernel_app.ipython_dir = ipython_dir

            self._ip_kernel_app.matplotlib = ipython_matplotlib if ipython_matplotlib else "agg"

            # Prevent kernel from capturing stdout/stderr, otherwise it creates a mess.
            # See https://github.com/ipython/ipykernel/issues/795
            self._ip_kernel_app.capture_fd_output = False

            # Echo all the output to sys.__stdout__ and sys.__stderr__ during kernel initialization
            self._ip_kernel_app.quiet = False

            def find_kernel_ip(ip_str):
                if ip_str == "localhost":
                    ip = "127.0.0.1"
                elif ip_str == "auto":
                    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    s.connect(("8.8.8.8", 80))
                    ip = s.getsockname()[0]
                else:
                    ip = ip_str
                return ip

            logger.info("Generating random port numbers for IPython kernel ...")
            kernel_ip = self._config_dict["ipython_kernel_ip"]
            try:
                kernel_ip = find_kernel_ip(kernel_ip)
                self._ip_kernel_app.ip = kernel_ip
                self._ip_kernel_app.shell_port = generate_random_port(kernel_ip)
                self._ip_kernel_app.iopub_port = generate_random_port(kernel_ip)
                self._ip_kernel_app.stdin_port = generate_random_port(kernel_ip)
                self._ip_kernel_app.hb_port = generate_random_port(kernel_ip)
                self._ip_kernel_app.control_port = generate_random_port(kernel_ip)
                self._ip_connect_info = self._ip_kernel_app.get_connection_info()
            except Exception as ex:
                self._success_startup = False
                logger.error("Failed to generates kernel ports for IP %r: %s", kernel_ip, ex)

            if self._success_startup:

                def start_jupyter_client():
                    from jupyter_client import BlockingKernelClient

                    self._ip_kernel_client = BlockingKernelClient()
                    self._ip_kernel_client.load_connection_info(self._ip_connect_info)
                    logger.info(
                        "Session ID for communication with IP kernel: %s", self._ip_kernel_client.session.session
                    )
                    self._ip_kernel_client.start_channels()

                    ip_kernel_iopub_monitor_thread = Thread(
                        target=self._ip_kernel_iopub_monitor_thread,
                        kwargs=dict(output_stream=out_stream, error_stream=err_stream),
                        daemon=True,
                    )
                    ip_kernel_iopub_monitor_thread.start()

                start_jupyter_client()

                self._ip_kernel_monitor_always_allow_types = ["error"]
                self._ip_kernel_monitor_collected_tracebacks = []

                ttime.sleep(0.5)  # Wait unitl 0MQ monitor is connected to the kernel ports

                logger.info("Initializing IPython kernel ...")
                self._ip_kernel_app.initialize([])
                logger.info("IPython kernel initialization is complete.")

                ttime.sleep(0.2)  # Wait until the error message are delivered (if startup fails)

                self._ip_kernel_monitor_always_allow_types = ["stream", "error", "execute_result"]
                collected_tracebacks = self._ip_kernel_monitor_collected_tracebacks
                self._ip_kernel_monitor_collected_tracebacks = None

                self._ip_connect_file = self._ip_kernel_app.connection_file

                # This is a very naive idea: if no exceptions were raised during kernel initialization
                #   the we consider that startup code was loaded and the environment is fully functional
                #   Otherwise we assume that loading failed and the collected tracebacks are used
                #   to generate report (if needed). TODO: there could be some other non-obvious way to
                #   detect if startup code was loaded. Ideas are appreciated.
                if collected_tracebacks:
                    self._success_startup = False
                    logger.error("The environment can not be opened: failed to load startup code.")

                # Disable echoing, since startup code is already loaded
                self._ip_kernel_app.quiet = True
                self._ip_kernel_app.init_io()

                # Print connect info for the kernel (after kernel initialization)
                cinfo = copy.deepcopy(self._ip_connect_info)
                cinfo["key"] = cinfo["key"].decode("utf-8")
                logger.info("IPython kernel connection info:\n %r", ppfl(cinfo))

                th_abandoned_plans = threading.Thread(target=self._monitor_abandoned_plans_thread, daemon=True)
                th_abandoned_plans.start()

            # --------------------------------------------------------------------------
            #               Run startup code outside the IPython kernel1
            if self._success_startup:
                logger.info("Configuring the environment ...")
                self._worker_startup_code()

            if self._success_startup:
                logger.info("Preparing to start IPython kernel ...")
                # Execute some useless command in kernel to make it report IDLE state
                self._ip_kernel_execute_command(command="___ip_kernel_startup_init___()", except_on=False)
                self._ip_kernel_app.start()

            self._ip_kernel_is_shut_down_event.set()

            self._ip_kernel_state = IPKernelState.DISABLED
            # self._ip_kernel_app.close()  # Does not work well if kernel is shut down
            self._exit_event.set()
            self._ip_kernel_monitor_stop = True

            # Restore 'sys.stdout' and 'sys.stderr' changed during kernel initialization
            sys.stdout, sys.stderr = out_stream, err_stream

        self._worker_shutdown_code()

        logger.info("Run Engine environment was closed successfully")
