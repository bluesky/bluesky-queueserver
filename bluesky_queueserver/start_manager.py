from multiprocessing import Pipe
import threading
import time as ttime
import pprint

from .worker import RunEngineWorker
from .manager import RunEngineManager

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class WatchdogProcess:
    def __init__(self):
        self._re_manager = None
        self._re_worker = None

        # Create pipes used for connections of the modules
        self._manager_conn = None  # Worker -> Manager
        self._worker_conn = None  # Manager -> Worker
        self._watchdog_to_manager_conn = None  # Watchdog -> Manager
        self._manager_to_watchdog_conn = None  # Manager -> Watchdog
        self._create_conn_pipes()
        self._start_conn_thread()

        self._watchdog_state = 0  # State is currently just time since last notification
        self._watchdog_state_lock = threading.Lock()

    def _create_conn_pipes(self):
        # Manager to worker
        self._manager_conn, self._worker_conn = Pipe()
        # Watchdog to manager
        self._watchdog_to_manager_conn, self._manager_to_watchdog_conn = Pipe()

    def _start_conn_thread(self):
        self._thread_conn = threading.Thread(target=self._receive_packet_thread,
                                             name="RE Server Comm",
                                             daemon=True)
        self._thread_conn.start()

    def _receive_packet_thread(self):
        while True:
            if self._watchdog_to_manager_conn.poll(0.1):
                try:
                    msg = self._watchdog_to_manager_conn.recv()
                    # Messages should be handled in the event loop
                    self._conn_received(msg)
                except Exception as ex:
                    logger.error("Exception occurred while waiting for packet: %s" % str(ex))
                    break

    def _conn_received(self, msg):
        type, value = msg["type"], msg["value"]

        if type == "notification":
            if value == "alive":
                self._register_heartbeat()

        if type == "command":
            logger.debug("Command received from RE Manager: %s" % pprint.pformat(msg))
            if value == "start_re_worker":
                self._start_re_worker()
                msg_out = {"type": "report", "value": {"msg": msg, "success": True}}
                self._watchdog_to_manager_conn.send(msg_out)

            if value == "join_re_worker":
                success = self._join_re_worker()
                msg_out = {"type": "report", "value": {"msg": msg, "success": success}}
                self._watchdog_to_manager_conn.send(msg_out)

            if value == "kill_re_worker":
                self._kill_re_worker()
                msg_out = {"type": "report", "value": {"msg": msg, "success": True}}
                self._watchdog_to_manager_conn.send(msg_out)

        if type == "request":
            if value == "is_worker_alive":
                result = self._is_worker_alive()
                msg_out = {"type": "result", "value": {"request": value, "result": result}}
                self._watchdog_to_manager_conn.send(msg_out)

    def _start_re_worker(self):
        """
        Creates worker process. This is a quick operation, because it starts RE Worker
        process without waiting for initialization.
        """
        logger.info("Starting RE Worker ...")
        self._re_worker = RunEngineWorker(conn=self._manager_conn)
        self._re_worker.start()

    def _join_re_worker(self):
        """
        Join RE Worker process after it was orderly closed by RE Manager. Watchdog module doesn't
        communicate with the worker process directly. This is responsibility of the RE Manager.
        But RE Manager doesn't hold reference to RE Worker, so it needs to be joined here.
        """
        logger.info("Joining RE Worker ...")
        self._re_worker.join(0.5)  # Try to join with timeout
        return not self._re_worker.is_alive()  # Return success

    def _kill_re_worker(self):
        """
        Kill RE Worker by request from RE Manager. This is done only if RE Worker is non-responsive
        and can not be orderly stopped.
        """
        # TODO: kill() or terminate()???
        logger.info("Killing RE Worker ...")
        self._re_worker.kill()

    def _init_watchdog_state(self):
        with self._watchdog_state_lock:
            self._watchdog_state = ttime.time()

    def _register_heartbeat(self):
        """
        Heartbeat is received. Update the state.
        """
        self._init_watchdog_state()

    def _is_worker_alive(self):
        """
        Checks if RE Worker process is in running state. It doesn't mean that it is responsive.
        """
        is_alive = False
        if hasattr(self._re_worker, "is_alive"):
            is_alive = self._re_worker.is_alive()
        return is_alive

    def _start_re_manager(self):
        self._init_watchdog_state()
        self._re_manager = RunEngineManager(conn_watchdog=self._manager_to_watchdog_conn,
                                            conn_worker=self._worker_conn)
        self._re_manager.start()

    def run(self):
        self._start_re_manager()
        while True:
            # Primitive implementation of the loop that restarts the process.
            self._re_manager.join(0.1)  # Small timeout

            if not self._re_manager.is_alive():
                break  # Exit if the program was actually stopped (process joined)

            with self._watchdog_state_lock:
                time_passed = ttime.time() - self._watchdog_state

            # Interval is used to protect the system from restarting in case of clock issues.
            # It may be a better idea to implement a ticker in a separate thread to act as
            #   a clock to be completely independent from system clock.
            if (time_passed > 5.0) and (time_passed < 15.0):
                logger.error("Timeout detected by Watchdog. RE Manager malfunctioned and must be restarted.")
                self._re_manager.kill()
                self._start_re_manager()


if __name__ == "__main__":
    wp = WatchdogProcess()
    try:
        wp.run()
    except KeyboardInterrupt:
        logger.info("The program was manually stopped.")
