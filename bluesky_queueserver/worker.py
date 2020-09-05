from multiprocessing import Process
import threading
import asyncio
import queue
import time as ttime
from functools import partial

from bluesky import RunEngine
from bluesky.run_engine import get_bluesky_event_loop, _ensure_event_loop_running

from bluesky.callbacks.best_effort import BestEffortCallback
from bluesky.log import config_bluesky_logging

from ophyd.log import config_ophyd_logging

# from databroker import Broker

from ophyd.sim import det1, det2
from bluesky.plans import count

import logging
logger = logging.getLogger(__name__)

config_bluesky_logging(level='INFO')
config_ophyd_logging(level='INFO')

mpl_logger = logging.getLogger("matplotlib")
mpl_logger.setLevel(logging.WARNING)


class RunEngineWorker(Process):
    """
    The class implementing BlueskyWorker thread.
    """
    def __init__(self, *, conn):

        super().__init__(name="RE Worker")

        self._conn = conn
        print(f"conn={conn}")

        self._loop = None
        self._th = None
        self._exit_loop_event = None

        self._exit_event = threading.Event()
        self._callback_queue = None

        self._RE = None

        # Start thread that receives packets from the pip 'conn
        self._conn_recv_buffer = []

        self._thread_conn = None

    def _receive_packet_thread(self):
        while True:
            ttime.sleep(0.1)
            if self._exit_event.is_set():
                break
            if self._conn.poll():
                try:
                    print(f"Waiting for message")
                    msg = self._conn.recv()
                    self._loop.call_soon_threadsafe(self._conn_received, msg)
                    print(f"Message received")
                except Exception as ex:
                    logger.error(f"Exception occurred while waiting for packet: {ex}")
                    break

    def _execute_in_main_thread(self):
        # This function blocks the main thread
        while True:
            ttime.sleep(1)
            if self._exit_event.is_set():
                break
            try:
                callback = self._callback_queue.get(False)
                callback()
            except queue.Empty:
                pass

    def _conn_received(self, msg):
        print(f"Message is in the loop: {msg}")

        type, value = msg["type"], msg["value"]

        if type == "command" and value == "quit":
            # Stop the loop in main thread
            print(f"Existing ...")
            self._exit_event.set()
            self._exit_loop_event.set()

        if type == "plan":
            logger.info("Starting a plan")
            plan_name = value["name"]
            args = value["args"]
            kwargs = value["kwargs"]

            def ref_from_name(v):
                if isinstance(v, str):
                    try:
                        v = globals()[v]
                    except KeyError:
                        pass
                return v

            # Replace names with references
            plan_name = ref_from_name(plan_name)
            args = [[ref_from_name(_) for _ in arg] for arg in args]
            kwargs = {"num": 5, "delay": [1, 2, 3, 4]}

            def plan():
                yield from plan_name(*args, **kwargs)

            func = partial(self._execute_plan, plan)
            self._callback_queue.put(func)

    async def _wait_for_exit(self):
        self._exit_loop_event = asyncio.Event()
        self._callback_queue = queue.Queue()

        await self._exit_loop_event.wait()

    def run(self):

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

        self._thread_conn = threading.Thread(target=self._receive_packet_thread,
                                             name="RE Worker Receive")
        self._thread_conn.start()

        # Now make the main thread busy
        self._execute_in_main_thread()

        print("Exited main thread")
        # Wait for the coroutine to exit
        future.result()
        print("Future completed")

        self._thread_conn.join()
        print("Thread joined")

        del self._RE
        print("Run engine deleted")

    def _execute_plan(self, plan):
        logger.info("Starting execution of a plan")
        self._RE(plan())
        logger.info("Finished execution of a plan")
