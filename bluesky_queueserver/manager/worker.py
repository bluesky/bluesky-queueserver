from multiprocessing import Process
import threading
import queue
import time as ttime
import asyncio
from functools import partial
import logging

from .comms import PipeJsonRpcReceive

import msgpack
import msgpack_numpy as mpn

from .profile_ops import (
    load_profile_collection,
    plans_from_nspace,
    devices_from_nspace,
    load_allowed_plans_and_devices,
    parse_plan,
)

logger = logging.getLogger(__name__)


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

    def __init__(self, *args, conn, config=None, **kwargs):

        if not conn:
            raise RuntimeError("Invalid value of parameter 'conn': %S.", str(conn))

        super().__init__(*args, **kwargs)

        # The end of bidirectional Pipe assigned to the worker (for communication with Manager process)
        self._conn = conn

        self._exit_event = None
        self._exit_confirmed_event = None

        self._execution_queue = None

        # Dictionary that holds current RE Worker state
        self._state = {
            # The dictionary of the currently running plan or the plan that was executed last
            "running_plan": None,
            # Boolean value that indicates if the current plan is completed (finished or stopped)
            "running_plan_completed": False,
            # Status of the RE environment: "initializing", "read", "closing"
            "environment_state": "initializing",
        }

        # Reference to Bluesky Run Engine
        self._RE = None

        # Report (dict) generated after execution of a command. The report can be downloaded
        #   by RE Manager.
        self._re_report = None
        self._re_report_lock = None  # threading.Lock

        # Class that supports communication over the pipe
        self._comm_to_manager = PipeJsonRpcReceive(conn=self._conn, name="RE Watchdog-Manager Comm")

        self._db = None
        self._config = config or {}
        self._allowed_plans, self._allowed_devices = {}, {}

        self._re_namespace, self._existing_plans, self._existing_devices = {}, {}, {}

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
        logger.debug("Starting execution of a task")
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
                    self._state["running_plan_completed"] = True
                else:
                    # Here we don't distinguish between stop/abort/halt
                    self._re_report["plan_state"] = "stopped"
                    self._state["running_plan_completed"] = True

                # Include RE state
                self._re_report["re_state"] = str(self._RE._state)

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
                    self._state["running_plan_completed"] = True

                # Include RE state
                self._re_report["re_state"] = str(self._RE._state)

        logger.debug("Finished execution of a task")

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
        # Save reference to the currently executed plan
        self._state["running_plan"] = plan_info
        self._state["running_plan_completed"] = False

        logger.info("Starting a plan '%s'.", plan_info["name"])

        try:
            plan_parsed = parse_plan(
                plan_info, allowed_plans=self._existing_plans, allowed_devices=self._existing_devices
            )

            plan_func = plan_parsed["name"]
            plan_args_parsed = plan_parsed["args"]
            plan_kwargs_parsed = plan_parsed["kwargs"]

            def get_plan(plan_func, plan_args, plan_kwargs):
                def plan():
                    if self._RE._state == "panicked":
                        raise RuntimeError(
                            "Run Engine is in the 'panicked' state. "
                            "You need to recreate the environment before you can run plans."
                        )
                    elif self._RE._state != "idle":
                        raise RuntimeError(
                            f"Run Engine is in '{self._RE._state}' state. Stop or finish any running plan."
                        )
                    else:
                        result = self._RE(plan_func(*plan_args, **plan_kwargs))
                    return result

                return plan

            plan = get_plan(plan_func, plan_args_parsed, plan_kwargs_parsed)
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

    # =============================================================================
    #               Handlers for messages from RE Manager

    def _request_state_handler(self):
        """
        Returns the state information of RE Worker environment.
        """
        plan_uid = self._state["running_plan"]["plan_uid"] if self._state["running_plan"] else None
        plan_completed = self._state["running_plan_completed"]
        re_state = str(self._RE._state) if self._RE else "null"
        env_state = self._state["environment_state"]
        re_report_available = self._re_report is not None
        msg_out = {
            "running_plan_uid": plan_uid,
            "running_plan_completed": plan_completed,
            "re_report_available": re_report_available,
            "re_state": re_state,
            "environment_state": env_state,
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
        msg_out = self._re_report
        self._re_report = None
        return msg_out

    def _command_close_env_handler(self):
        """
        Close RE Worker environment in orderly way.
        """
        # Stop the loop in main thread
        logger.info("Closing RE Worker environment")
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
                "self._state['environment_state']=='closing'"
            )
        msg_out = {"status": status, "err_msg": err_msg}
        return msg_out

    def _command_run_plan_handler(self, *, plan_info):
        """
        Initiate execution of a new plan.
        """
        logger.info("Starting execution of a plan")
        # TODO: refine the criteria of acceptance of the new plan.
        invalid_state = 0
        if not self._execution_queue.empty():
            invalid_state = 1
        elif self._RE._state == "running":
            invalid_state = 2
        elif self._state["running_plan"] or self._state["running_plan_completed"]:
            invalid_state = 3

        err_msg = ""
        if not invalid_state:  # == 0
            try:
                # Value is a dictionary with plan parameters
                self._load_new_plan(plan_info=plan_info)
                status = "accepted"
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
        logger.info("Pausing Run Engine")
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
            self._state["running_plan"] = None
            self._state["running_plan_completed"] = False
            with self._re_report_lock:
                self._re_report = None
            status = "accepted"
        else:
            status = "rejected"
            err_msg = "Run Engine must be in 'idle' state to continue. " f"The state is '{self._RE._state}'"

        msg_out = {"status": status, "err_msg": err_msg}
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
            # Polling once per second. This is fast enough for slowly executed plans.
            ttime.sleep(1)
            # Exit the thread if the Event is set (necessary to gracefully close the process)
            if self._exit_event.is_set():
                break
            try:
                plan, is_resuming = self._execution_queue.get(False)
                self._execute_plan(plan, is_resuming)
            except queue.Empty:
                pass

    # ------------------------------------------------------------

    def run(self):
        """
        Overrides the `run()` function of the `multiprocessing.Process` class. Called
        by the `start` method.
        """
        success = True

        self._comm_to_manager.add_method(self._request_state_handler, "request_state")
        self._comm_to_manager.add_method(self._request_plan_report_handler, "request_plan_report")
        self._comm_to_manager.add_method(self._command_close_env_handler, "command_close_env")
        self._comm_to_manager.add_method(self._command_confirm_exit_handler, "command_confirm_exit")
        self._comm_to_manager.add_method(self._command_run_plan_handler, "command_run_plan")
        self._comm_to_manager.add_method(self._command_pause_plan_handler, "command_pause_plan")
        self._comm_to_manager.add_method(self._command_continue_plan_handler, "command_continue_plan")
        self._comm_to_manager.add_method(self._command_reset_worker_handler, "command_reset_worker")
        self._comm_to_manager.start()

        self._exit_event = threading.Event()
        self._exit_confirmed_event = threading.Event()
        self._re_report_lock = threading.Lock()

        from bluesky import RunEngine
        from bluesky.run_engine import get_bluesky_event_loop
        from bluesky.callbacks.best_effort import BestEffortCallback
        from bluesky_kafka import Publisher as kafkaPublisher
        from databroker import Broker

        # TODO: subscribe to local databroker (currently there is no way to access
        #    'temp' databroker from outside the process). Production version will use Kafka.
        self._db = Broker.named("temp")

        # TODO: TC - Do you think that the following code may be included in RE.__init__()
        #   (for Python 3.8 and above)
        # Setting the default event loop is needed to make the code work with Python 3.8.
        loop = get_bluesky_event_loop()
        asyncio.set_event_loop(loop)

        def init_namespace():
            self._re_namespace = {}
            self._existing_plans = {}
            self._existing_devices = {}

        if "profile_collection_path" not in self._config:
            logger.warning("Path to profile collection was not specified. No profile collection will be loaded.")
            init_namespace()
        else:
            path = self._config["profile_collection_path"]
            logger.info("Loading beamline profile collection from directory '%s' ...", path)
            try:
                self._re_namespace = load_profile_collection(path)
                self._existing_plans = plans_from_nspace(self._re_namespace)
                self._existing_devices = devices_from_nspace(self._re_namespace)
                logger.info("Beamline profile collection was loaded completed.")
            except Exception as ex:
                logger.exception(
                    "Failed to start RE Worker environment. Error while " "loading profile collection: %s.",
                    str(ex),
                )
                success = False

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
                "Error occurred while loading lists of allowed plans and devices from '%s': %s", path_pd, str(ex)
            )

        if success:
            logger.info("Instantiating and configuring Run Engine ...")

            try:
                self._RE = RunEngine({})

                bec = BestEffortCallback()
                self._RE.subscribe(bec)

                self._RE.subscribe(self._db.insert)

                if "kafka" in self._config:
                    logger.info(
                        "Subscribing to Kafka: topic '%s', servers '%s'",
                        self._config["kafka"]["topic"],
                        self._config["kafka"]["bootstrap"],
                    )
                    kafka_publisher = kafkaPublisher(
                        topic=self._config["kafka"]["topic"],
                        bootstrap_servers=self._config["kafka"]["bootstrap"],
                        key="kafka-unit-test-key",
                        # work with a single broker
                        producer_config={"acks": 1, "enable.idempotence": False, "request.timeout.ms": 5000},
                        serializer=partial(msgpack.dumps, default=mpn.encode),
                    )
                    self._RE.subscribe(kafka_publisher)

                self._execution_queue = queue.Queue()

                self._state["environment_state"] = "ready"

            except BaseException as ex:
                success = False
                logger.exception("Error occurred while initializing the environment: %s.", str(ex))

        if success:
            logger.info("RE Environment is ready.")
            self._execute_in_main_thread()
        else:
            self._exit_event.set()

        logger.info("Environment is waiting to be closed ...")
        self._state["environment_state"] = "closing"

        # Wait until confirmation is received from RE Manager
        while not self._exit_confirmed_event.is_set():
            ttime.sleep(0.02)

        self._RE = None

        self._comm_to_manager.stop()

        logger.info("Run Engine environment was closed successfully.")
