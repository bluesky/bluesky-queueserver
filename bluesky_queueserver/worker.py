from multiprocessing import Process
import threading
import queue
import time as ttime
import os
import signal
from collections.abc import Iterable

from bluesky import RunEngine

from bluesky.callbacks.best_effort import BestEffortCallback
from bluesky.log import config_bluesky_logging

from ophyd.log import config_ophyd_logging

# from databroker import Broker

# The following plans/devices must be imported (otherwise plan parsing wouldn't work)
from ophyd.sim import det1, det2, motor  # noqa: F401
from bluesky.plans import count, scan  # noqa: F401

import logging
logger = logging.getLogger(__name__)

config_bluesky_logging(level='INFO')
config_ophyd_logging(level='INFO')

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


class RunEngineWorker(Process):
    """
    The class implementing BlueskyWorker thread.

    Parameters
    ----------
    conn
        The end of bidirectional (input/output) pipe.
    """
    def __init__(self, *, conn):

        super().__init__(name="RE Worker")

        # The end of bidirectional Pipe assigned to the worker (for communication with the server)
        self._conn = conn

        self._exit_event = threading.Event()
        self._execution_queue = None

        # Reference to Bluesky Run Engine
        self._RE = None

        # The thread that receives packets from the pipe 'self._conn'
        self._thread_conn = None

        # Thread used for to send the second sigint after short timeout
        self._thread_sigint = None

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
                    logger.error(f"Exception occurred while waiting for packet: {ex}")
                    break

    def _execute_plan(self, plan, is_resuming):
        """
        Start Run Engine to execute a plan

        Parameters
        ----------
        plan_func: function
            Reference to a function that implements a Bluesky plan
        plan_args: list
            List of the plan arguments
        plan_kwargs: dict
            Dictionary of the plan kwargs.
        """
        logger.debug("Starting execution of a task")
        try:
            result = plan()
            msg = {"type": "report",
                   "value": {"completed": is_resuming, "success": True, "result": result}}
        except BaseException as ex:
            msg = {"type": "report",
                   "value": {"completed": False, "success": False, "result": str(ex)}}

        self._conn.send(msg)
        logger.debug("Finished execution of a task")

    def _load_new_plan(self, plan_name, plan_args, plan_kwargs):
        """
        Loads a new plan into `self._execution_queue`. The plan is
        later picked up by the function running in the main thread and
        executed by the Run Engine.
        """
        logger.info(f"Starting a plan {plan_name}")

        def ref_from_name(v):
            if isinstance(v, str):
                try:
                    v = globals()[v]
                except KeyError:
                    pass
            return v

        # The following is some primitive parsing of the plan that replaces names
        #   with references. No error handling is implemented, so it is better if
        #   the submitted plans contain no errors.
        plan_func = ref_from_name(plan_name)
        plan_args_parsed = []
        for arg in plan_args:
            if isinstance(arg, Iterable) and not isinstance(arg, str):
                arg_parsed = [ref_from_name(_) for _ in arg]
            else:
                arg_parsed = ref_from_name(arg)
            plan_args_parsed.append(arg_parsed)

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

        plan = get_plan(plan_func, plan_args_parsed, plan_kwargs)
        # 'is_resuming' is true (we start a new plan that is supposedly runs to completion
        #   as opposed to aborting/stopping/halting a plan)
        self._execution_queue.put((plan, True))

    def _continue_plan(self, option):
        """
        Continue/stop execution of a plan after it was paused.
        """
        logger.info(f"Continue plan execution with the option '{option}'")

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

        msg_ack = {"type": "acknowledge",
                   "value": {"status": "unrecognized",
                             "msg": msg,  # Send back the message
                             "result": ""}}

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

        if type == "plan":
            logger.info("Starting a plan")
            # TODO: refine the criteria of acceptance of the new plan.
            invalid_state = 0
            if not self._execution_queue.empty():
                invalid_state = 1
            elif self._RE._state == 'running':
                invalid_state = 2

            if not invalid_state:  # == 0
                try:
                    plan_name = value["name"]
                    plan_args = value["args"]
                    plan_kwargs = value["kwargs"]
                    self._load_new_plan(plan_name, plan_args, plan_kwargs)
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

        if type == "command" and value == "pause":
            # Stop the loop in main thread
            logger.info("Pausing Run Engine")
            pausing_options = ("deferred", "immediate")
            if self._RE._state == 'running':
                try:
                    option = msg["option"]
                    if option not in pausing_options:
                        raise RuntimeError(f"Option '{option}' is not supported. "
                                           f"Available options: {pausing_options}")
                    pid = os.getpid()

                    def send_sigint():
                        try:
                            os.kill(pid, signal.SIGINT)
                        except Exception:
                            pass

                    def send_second_sigint():
                        ttime.sleep(0.05)
                        send_sigint()

                    send_sigint()  # 1st SIGINT
                    # TODO: I am not sure that this is a good way to initiate immediate pause.
                    #       It seems to work in this prototype, but it needs to be well understood
                    #       before it can be used in production.
                    if option == "immediate":
                        # Run it in a separate thread. So that the function could return immediately.
                        self._thread_sigint = threading.Thread(target=send_second_sigint,
                                                               name="RE Worker 2nd SIGINT",
                                                               daemon=True)
                        self._thread_sigint.start()

                    msg_ack["value"]["status"] = "accepted"
                except Exception as ex:
                    msg_ack["value"]["status"] = "error"
                    msg_ack["value"]["result"] = str(ex)
            else:
                msg_ack["value"]["status"] = "rejected"
                msg_ack["value"]["result"] = \
                    "Run engine can be paused only in 'running' state. " \
                    f"Current state: '{self._RE._state}'"

        if type == "command" and value == "continue":
            # Continue execution of the plan
            if self._RE.state == 'paused':
                try:
                    option = msg["option"]
                    logger.info(f"Run Engine: {option}")
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
            ttime.sleep(1)  # Polling once per second should be fine for now.
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
        self._exit_event.clear()

        self._RE = RunEngine({})

        bec = BestEffortCallback()
        self._RE.subscribe(bec)

        # db = Broker.named('temp')
        # self._RE.subscribe(db.insert)

        self._execution_queue = queue.Queue()

        self._thread_conn = threading.Thread(target=self._receive_packet_thread,
                                             name="RE Worker Receive")
        self._thread_conn.start()

        # Now make the main thread busy
        self._execute_in_main_thread()

        self._thread_conn.join()

        del self._RE
