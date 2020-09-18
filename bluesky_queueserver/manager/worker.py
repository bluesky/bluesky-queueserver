from multiprocessing import Process
import threading
import queue
import time as ttime
from collections.abc import Iterable
import asyncio
from functools import partial
import logging
import os

import msgpack
import msgpack_numpy as mpn

from bluesky import RunEngine
from bluesky.run_engine import get_bluesky_event_loop

from bluesky.callbacks.best_effort import BestEffortCallback
from databroker import Broker

from bluesky_kafka import Publisher as kafkaPublisher

from .profile_ops import load_profile_collection, plans_from_nspace, devices_from_nspace

logger = logging.getLogger(__name__)

DB = [Broker.named('temp')]


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
    def __init__(self, *args, conn, env_config=None, **kwargs):

        if not conn:
            raise RuntimeError("Invalid value of parameter 'conn': %S.", str(conn))

        super().__init__(*args, **kwargs)

        # The end of bidirectional Pipe assigned to the worker (for communication with Manager process)
        self._conn = conn

        self._exit_event = None
        self._execution_queue = None

        # Dictionary that contains parameters of currently executed plan or None if no plan is
        #   currently executed. Plan is considered as being executed if it is paused.
        self._running_plan = None

        # Reference to Bluesky Run Engine
        self._RE = None

        # The thread that receives packets from the pipe 'self._conn'
        self._thread_conn = None

        self._db = DB[0]
        self._env_config = env_config or {}

        self._re_namespace, self._allowed_plans, self._allowed_devices = {}, {}, {}

    def _receive_packet_thread(self):
        """
        The function is running in a separate thread and monitoring the output
        of the communication Pipe.
        """
        while True:
            if self._exit_event.is_set():
                break
            if self._conn.poll(0.1):
                try:
                    msg = self._conn.recv()
                    self._conn_received(msg)
                except Exception as ex:
                    logger.exception("Exception occurred while waiting for packet: %s", str(ex))
                    break

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
            msg = {"type": "report",
                   "value": {"action": "plan_exit",
                             "success": True,
                             "result": result,
                             "err_msg": ""}}
            if is_resuming:
                msg["value"]["plan_state"] = "completed"
            else:
                msg["value"]["plan_state"] = "stopped"  # Here we don't distinguish between stop/abort/halt

            self._running_plan = None

        except BaseException as ex:
            msg = {"type": "report",
                   "value": {"action": "plan_exit",
                             "result": "",
                             "err_msg": str(ex)}}

            if self._RE._state == "paused":
                # Run Engine was paused
                msg["value"]["plan_state"] = "paused"
                msg["value"]["success"] = True
            else:
                # RE crashed. Plan execution can not be resumed. (Environment may have to be restarted.)
                # TODO: clarify how this situation must be handled. Also additional error handling
                #       may be required
                msg["value"]["plan_state"] = "error"
                msg["value"]["success"] = False
                self._running_plan = None

        # Include RE state
        msg["value"]["re_state"] = str(self._RE._state)

        self._conn.send(msg)
        logger.debug("Finished execution of a task")

    def _load_new_plan(self, plan):
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
        self._running_plan = plan

        plan_name = plan["name"]
        plan_args = plan["args"]
        plan_kwargs = plan["kwargs"]

        logger.info("Starting a plan '%s'.", plan_name)

        def ref_from_name(v, allowed_items):
            if isinstance(v, str):
                if v in allowed_items:
                    v = allowed_items[v]
            return v

        def process_argument(v, allowed_items):
            if isinstance(v, str):
                v = ref_from_name(v, allowed_items)
            elif isinstance(v, dict):
                for key, value in v.copy().items():
                    v[key] = process_argument(value, allowed_items)
            elif isinstance(v, Iterable):
                v_original = v
                v = list()
                for item in v_original:
                    v.append(process_argument(item, allowed_items))
            return v

        # TODO: should we allow plan names as arguments
        allowed_items = self._allowed_devices
        plan_func = process_argument(plan_name, self._allowed_plans)
        plan_args_parsed = process_argument(plan_args, allowed_items)
        plan_kwargs_parsed = process_argument(plan_kwargs, allowed_items)

        # We are not parsing 'kwargs' at this time
        def get_plan(plan_func, plan_args, plan_kwargs):
            def plan():
                if self._RE._state == 'panicked':
                    raise RuntimeError("Run Engine is in the 'panicked' state. "
                                       "You need to recreate the environment before you can run plans.")
                elif self._RE._state != 'idle':
                    raise RuntimeError(f"Run Engine is in '{self._RE._state}' state. "
                                       "Stop or finish any running plan.")
                else:
                    result = self._RE(plan_func(*plan_args, **plan_kwargs))
                return result
            return plan

        plan = get_plan(plan_func, plan_args_parsed, plan_kwargs_parsed)
        # 'is_resuming' is true (we start a new plan that is supposedly runs to completion
        #   as opposed to aborting/stopping/halting a plan)
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
                if self._RE._state == 'panicked':
                    raise RuntimeError("Run Engine is in the 'panicked' state. "
                                       "You need to recreate the environment before you can run plans.")
                elif self._RE._state != 'paused':
                    raise RuntimeError(f"Run Engine is in '{self._RE._state}' state. "
                                       f"Only 'paused' plan can be continued.")
                elif option not in available_options:
                    raise RuntimeError(f"Option '{option}' is not supported. "
                                       f"Supported options: {available_options}")
                else:
                    result = getattr(self._RE, option)()
                return result
            return plan

        plan = get_plan(option, available_options)
        is_resuming = (option == "resume")
        self._execution_queue.put((plan, is_resuming))

    def _conn_received(self, msg):
        """
        The function is processing the received message 'msg'.
        """
        type, value = msg["type"], msg["value"]

        if type == "request":
            if value == "status":
                plan_uid = self._running_plan["plan_uid"] if self._running_plan else None
                msg_out = {"type": "result",
                           "contains": "status",
                           "value": {"running_plan_uid": plan_uid,
                                     "re_state": str(self._RE._state),
                                     }
                           }
                self._conn.send(msg_out)

        else:
            # The default acknowledge message (will be sent to `self._conn` if
            #   the message is not recognized.
            msg_ack = {"type": "acknowledge",
                       "value": {"status": "unrecognized",
                                 "msg": msg,  # Send back the message
                                 "result": ""}}

            # Exit the main thread and close the environment
            if type == "command" and value == "quit":
                # Stop the loop in main thread
                logger.info("Closing RE Worker environment")
                # TODO: probably the criteria on when the environment could be more precise.
                #       For now simply assume that we can not close the environment in which
                #       Run Engine is running using this method. Different method that kills
                #       the worker process is needed.
                if self._RE._state != "running":
                    try:
                        self._exit_event.set()
                        msg_ack["value"]["status"] = "accepted"
                    except Exception as ex:
                        msg_ack["value"]["status"] = "error"
                        msg_ack["value"]["result"] = str(ex)
                else:
                    msg_ack["value"]["status"] = "rejected"
                    msg_ack["value"]["result"] = "Can not close the environment with running Run Engine. " \
                                                 "Stop the running plan and try again."

            # Execute a plan
            if type == "plan":
                logger.info("Starting execution of a plan")
                # TODO: refine the criteria of acceptance of the new plan.
                invalid_state = 0
                if not self._execution_queue.empty():
                    invalid_state = 1
                elif self._RE._state == 'running':
                    invalid_state = 2

                if not invalid_state:  # == 0
                    try:
                        # Value is a dictionary with plan parameters
                        self._load_new_plan(value)
                        msg_ack["value"]["status"] = "accepted"
                    except Exception as ex:
                        msg_ack["value"]["status"] = "error"
                        msg_ack["value"]["result"] = str(ex)
                else:
                    msg_ack["value"]["status"] = "rejected"
                    msg_list = ["the execution queue is not empty", "another plan is running"]
                    try:
                        s = msg_list[invalid_state - 1]
                    except Exception:
                        s = "UNDETERMINED CONDITION IS PRESENT"  # Shouldn't ever be printed
                    msg_ack["value"]["result"] = \
                        f"Trying to run a plan (start Run Engine) while {s}.\n" \
                        "This may indicate a serious issue with the plan queue execution mechanism.\n" \
                        "Please report the issue to developers."

            # Pause a running plan
            if type == "command" and value == "pause":
                # Stop the loop in main thread
                logger.info("Pausing Run Engine")
                pausing_options = ("deferred", "immediate")
                # TODO: the question is whether it is possible or should be allowed to pause a plan in
                #       any other state than 'running'???
                if self._RE._state == 'running':
                    try:
                        option = msg["option"]
                        if option not in pausing_options:
                            raise RuntimeError(f"Option '{option}' is not supported. "
                                               f"Available options: {pausing_options}")

                        defer = {'deferred': True, 'immediate': False}[option]
                        self._RE.request_pause(defer=defer)
                        msg_ack["value"]["status"] = "accepted"
                    except Exception as ex:
                        msg_ack["value"]["status"] = "error"
                        msg_ack["value"]["result"] = str(ex)
                else:
                    msg_ack["value"]["status"] = "rejected"
                    msg_ack["value"]["result"] = \
                        "Run engine can be paused only in 'running' state. " \
                        f"Current state: '{self._RE._state}'"

            # Continue the previously paused plan (resume, abort, stop or halt)
            if type == "command" and value == "continue":
                # Continue execution of the plan
                if self._RE.state == 'paused':
                    try:
                        option = msg["option"]
                        logger.info("Run Engine: %s", option)
                        self._continue_plan(option)
                        msg_ack["value"]["status"] = "accepted"
                    except Exception as ex:
                        msg_ack["value"]["status"] = "error"
                        msg_ack["value"]["result"] = str(ex)
                else:
                    msg_ack["value"]["status"] = "rejected"
                    msg_ack["value"]["result"] = \
                        "Run Engine must be in 'paused' state to continue. " \
                        f"The state is '{self._RE._state}'"

            self._conn.send(msg_ack)

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

        self._exit_event = threading.Event()

        # TODO: TC - Do you think that the following code may be included in RE.__init__()
        #   (for Python 3.8 and above)
        # Setting the default event loop is needed to make the code work with Python 3.8.
        loop = get_bluesky_event_loop()
        asyncio.set_event_loop(loop)

        # Load profile collection
        # TODO: error processing while reading the profile collection
        #   - failed to load the profile collection
        path = os.path.realpath(__file__)
        path = path[:-1] if path[-1] in "/\\" else path  # Remove slash/backslash from the end
        path = os.path.split(path)[0]  # Remove file name
        path = os.path.split(path)[0]  # Remove bottom dir name
        path = os.path.join(path, "profile_collection_sim")
        logger.info(f"Loading beamline profiles located at '%s'", path)
        self._re_namespace = load_profile_collection(path)
        self._allowed_plans = plans_from_nspace(self._re_namespace)
        self._allowed_devices = devices_from_nspace(self._re_namespace)

        self._RE = RunEngine({})

        bec = BestEffortCallback()
        self._RE.subscribe(bec)

        # db = Broker.named('temp')

        if 'kafka' in self._env_config:
            kafka_publisher = kafkaPublisher(
                topic=self._env_config['kafka']['topic'],
                bootstrap_servers=self._env_config['kafka']['bootstrap'],
                key="kafka-unit-test-key",
                # work with a single broker
                producer_config={
                    "acks": 1,
                    "enable.idempotence": False,
                    "request.timeout.ms": 5000,
                },
                serializer=partial(msgpack.dumps, default=mpn.encode),
            )
            self._RE.subscribe(kafka_publisher)

        self._RE.subscribe(self._db.insert)

        self._execution_queue = queue.Queue()

        self._thread_conn = threading.Thread(target=self._receive_packet_thread,
                                             name="RE Worker Receive")
        self._thread_conn.start()

        # Environment is initialized: send a report
        msg = {"type": "report",
               "value": {"action": "environment_created"}}
        self._conn.send(msg)

        # Now make the main thread busy
        self._execute_in_main_thread()

        self._thread_conn.join()

        del self._RE

        # Finally send a report
        msg = {"type": "report",
               "value": {"action": "environment_closed"}}
        self._conn.send(msg)
