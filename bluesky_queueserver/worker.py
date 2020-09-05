from multiprocessing import Process
import threading
import asyncio
import queue
import time as ttime
from collections.abc import Iterable

from bluesky import RunEngine
from bluesky.run_engine import get_bluesky_event_loop, _ensure_event_loop_running

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

        self._loop = None
        self._th = None  # The thread in which the loop is running
        self._exit_loop_event = None  # Reference to the asyncio.Event

        self._exit_event = threading.Event()
        self._plan_execution_queue = None

        # Reference to Bluesky Run Engine
        self._RE = None

        # The thread that receives packets from the pipe 'self._conn'
        self._thread_conn = None

    def _receive_packet_thread(self):
        """
        The function is running in a separate thread and monitoring the output
        of the communication Pipe.
        """
        while True:
            ttime.sleep(0.1)
            if self._exit_event.is_set():
                break
            if self._conn.poll():
                try:
                    msg = self._conn.recv()
                    self._loop.call_soon_threadsafe(self._conn_received, msg)
                except Exception as ex:
                    logger.error(f"Exception occurred while waiting for packet: {ex}")
                    break

    def _execute_plan(self, plan_func, plan_args, plan_kwargs):
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
        logger.debug("Starting execution of a plan")

        uids = self._RE(plan_func(*plan_args, **plan_kwargs))

        # Very minimalistic report (for the demo).
        msg = {"type": "report",
               "value": {"uids": uids}}

        self._conn.send(msg)
        logger.debug("Finished execution of a plan")

    def _load_new_plan(self, plan_name, plan_args, plan_kwargs):
        """
        Loads a new plan into `self._plan_execution_queue`. The plan is
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
        plan = (plan_func, plan_args_parsed, plan_kwargs)
        self._plan_execution_queue.put(plan)

    def _conn_received(self, msg):
        """
        The function is processing the received message 'msg'.
        """
        type, value = msg["type"], msg["value"]

        if type == "command" and value == "quit":
            # Stop the loop in main thread
            logger.info("Closing RE Worker")
            self._exit_event.set()
            self._exit_loop_event.set()

        if type == "plan":
            plan_name = value["name"]
            plan_args = value["args"]
            plan_kwargs = value["kwargs"]
            self._load_new_plan(plan_name, plan_args, plan_kwargs)

    # ------------------------------------------------------------

    def _execute_in_main_thread(self):
        """
        Run this function to block the main thread. The function is polling
        `self._plan_execution_queue` and executes the plans that are in the queue.
        If the queue is empty, then the thread remains idle.
        """
        # This function blocks the main thread
        while True:
            ttime.sleep(1)  # Polling once per second should be fine for now.
            # Exit the thread if the Event is set (necessary to gracefully close the process)
            if self._exit_event.is_set():
                break
            try:
                plan_func, plan_args, plan_kwargs = self._plan_execution_queue.get(False)
                self._execute_plan(plan_func, plan_args, plan_kwargs)
            except queue.Empty:
                pass

    async def _wait_for_exit(self):
        """
        The function waits for `self._exit_loop_event` to be set. Waiting for
        the result of the returned futures ensures that all functions in the loop complete
        before the process is destroyed.
        """
        # Create the event (must be created from the loop)
        self._exit_loop_event = asyncio.Event()

        # Wait for the event to be set somewhere else (as a response to command from
        #   the server)
        await self._exit_loop_event.wait()

    # ------------------------------------------------------------

    def run(self):
        """
        Overrides the `run()` function of the `multiprocessing.Process` class. Called
        by the `start` method.
        """
        self._exit_event.clear()

        if not self._loop:
            self._loop = get_bluesky_event_loop()
            self._th = _ensure_event_loop_running(self._loop)

        self._RE = RunEngine({})

        bec = BestEffortCallback()
        self._RE.subscribe(bec)

        # db = Broker.named('temp')
        # self._RE.subscribe(db.insert)

        future = asyncio.run_coroutine_threadsafe(self._wait_for_exit(), self._loop)

        self._plan_execution_queue = queue.Queue()

        self._thread_conn = threading.Thread(target=self._receive_packet_thread,
                                             name="RE Worker Receive")
        self._thread_conn.start()

        # Now make the main thread busy
        self._execute_in_main_thread()

        # Wait for the coroutine to exit
        future.result()

        self._thread_conn.join()

        del self._RE
