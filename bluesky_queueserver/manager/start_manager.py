import argparse
from multiprocessing import Pipe
import threading
import time as ttime
import os

from .worker import RunEngineWorker
from .manager import RunEngineManager
from .comms import PipeJsonRpcReceive
from .profile_ops import get_default_profile_collection_dir

from .. import __version__

import logging

logger = logging.getLogger(__name__)


class WatchdogProcess:
    def __init__(
        self,
        *,
        config_worker=None,
        config_manager=None,
        cls_run_engine_worker=RunEngineWorker,
        cls_run_engine_manager=RunEngineManager,
    ):

        self._cls_run_engine_worker = cls_run_engine_worker
        self._cls_run_engine_manager = cls_run_engine_manager

        self._re_manager = None
        self._re_worker = None

        # Create pipes used for connections of the modules
        self._manager_conn = None  # Worker -> Manager
        self._worker_conn = None  # Manager -> Worker
        self._watchdog_to_manager_conn = None  # Watchdog -> Manager
        self._manager_to_watchdog_conn = None  # Manager -> Watchdog
        self._create_conn_pipes()

        # Class that supports communication over the pipe
        self._comm_to_manager = PipeJsonRpcReceive(
            conn=self._watchdog_to_manager_conn, name="RE Watchdog-Manager Comm"
        )

        self._watchdog_state = 0  # State is currently just time since last notification
        self._watchdog_state_lock = threading.Lock()

        self._manager_is_stopping = False  # Set True to stop the server
        self._heartbeat_timeout = 5  # Time to wait before restarting RE Manager

        # Configuration of the RE environment (passed to RE Worker)
        self._config_worker = config_worker
        self._config_manager = config_manager

    def _create_conn_pipes(self):
        # Manager to worker
        self._manager_conn, self._worker_conn = Pipe()
        # Watchdog to manager
        self._watchdog_to_manager_conn, self._manager_to_watchdog_conn = Pipe()

    # ======================================================================
    #             Handlers for messages from RE Manager

    def _start_re_worker_handler(self):
        """
        Creates worker process. This is a quick operation, because it starts RE Worker
        process without waiting for initialization.
        """
        logger.info("Starting RE Worker ...")
        try:
            self._re_worker = self._cls_run_engine_worker(
                conn=self._manager_conn, name="RE Worker Process", config=self._config_worker
            )
            self._re_worker.start()
            success, err_msg = True, ""
        except Exception as ex:
            success, err_msg = False, str(ex)
        return {"success": success, "err_msg": err_msg}

    def _join_re_worker_handler(self, *, timeout=0.5):
        """
        Join RE Worker process after it was orderly closed by RE Manager. Watchdog module doesn't
        communicate with the worker process directly. This is responsibility of the RE Manager.
        But RE Manager doesn't hold reference to RE Worker, so it needs to be joined here.
        """
        logger.info("Joining RE Worker ...")
        self._re_worker.join(timeout)  # Try to join with timeout
        success = not self._re_worker.is_alive()  # Return success
        return {"success": success}

    def _kill_re_worker_handler(self):
        """
        Kill RE Worker by request from RE Manager. This is done only if RE Worker is non-responsive
        and can not be orderly stopped.
        """
        # TODO: kill() or terminate()???
        logger.info("Killing RE Worker ...")
        self._re_worker.kill()
        self._re_worker.join()  # Not really necessary, but helps with unit testing.
        return {"success": True}

    def _is_worker_alive_handler(self):
        """
        Checks if RE Worker process is in running state. It doesn't mean that it is responsive.
        """
        is_alive = False
        if hasattr(self._re_worker, "is_alive"):
            is_alive = self._re_worker.is_alive()
        return {"worker_alive": is_alive}

    def _manager_stopping_handler(self):
        """
        Manager informed that it is stopping and should not be restarted.
        """
        self._manager_is_stopping = True

    def _init_watchdog_state(self):
        with self._watchdog_state_lock:
            self._watchdog_state = ttime.time()

    def _register_heartbeat_handler(self, *, value):
        """
        Heartbeat is received. Update the state.
        """
        if value == "alive":
            self._init_watchdog_state()

    # ======================================================================

    def _start_re_manager(self):
        self._init_watchdog_state()
        self._re_manager = self._cls_run_engine_manager(
            conn_watchdog=self._manager_to_watchdog_conn,
            conn_worker=self._worker_conn,
            config=self._config_manager,
            name="RE Manager Process",
        )
        self._re_manager.start()

    def run(self):

        # Requests
        self._comm_to_manager.add_method(self._start_re_worker_handler, "start_re_worker")
        self._comm_to_manager.add_method(self._join_re_worker_handler, "join_re_worker")
        self._comm_to_manager.add_method(self._kill_re_worker_handler, "kill_re_worker")
        self._comm_to_manager.add_method(self._is_worker_alive_handler, "is_worker_alive")
        # Notifications
        self._comm_to_manager.add_method(self._manager_stopping_handler, "manager_stopping")
        self._comm_to_manager.add_method(self._register_heartbeat_handler, "heartbeat")

        self._comm_to_manager.start()

        self._start_re_manager()
        while True:
            # Primitive implementation of the loop that restarts the process.
            self._re_manager.join(0.1)  # Small timeout

            if self._manager_is_stopping and not self._re_manager.is_alive():
                break  # Exit if the program was actually stopped (process joined)

            with self._watchdog_state_lock:
                time_passed = ttime.time() - self._watchdog_state

            # Interval is used to protect the system from restarting in case of clock issues.
            # It may be a better idea to implement a ticker in a separate thread to act as
            #   a clock to be completely independent from system clock.
            t_min, t_max = self._heartbeat_timeout, self._heartbeat_timeout + 10.0
            if (time_passed >= t_min) and (time_passed <= t_max) and not self._manager_is_stopping:
                logger.error("Timeout detected by Watchdog. RE Manager malfunctioned and must be restarted.")
                self._re_manager.kill()
                self._start_re_manager()

        self._comm_to_manager.stop()
        logger.info("RE Watchdog is stopped.")


def start_manager():
    parser = argparse.ArgumentParser(
        description="Start a RE Manager", epilog=f"blueksy-queueserver version {__version__}"
    )
    parser.add_argument("--kafka_topic", type=str, help="The kafka topic to publish to.")
    parser.add_argument(
        "--kafka_server", type=str, help="Bootstrap server to connect to.", default="127.0.0.1:9092"
    )
    parser.add_argument(
        "--profile_collection",
        "-p",
        dest="profile_collection_path",
        type=str,
        help="Path to directory that contains profile collection.",
    )
    parser.add_argument(
        "--existing_plans_and_devices",
        dest="existing_plans_and_devices_path",
        type=str,
        help="Path to file that contains the list of existing plans and devices. "
        "The path may be a relative path to the profile collection directory. "
        "If the path is directory, then the default file name "
        "'existing_plans_and_devices.yaml' is used.",
    )
    parser.add_argument(
        "--user_group_permissions",
        dest="user_group_permissions_path",
        type=str,
        help="Path to file that contains lists of plans and devices available to users. "
        "The path may be a relative path to the profile collection directory. "
        "If the path is a directory, then the default file name "
        "'user_group_permissions.yaml' is used.",
    )

    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("bluesky_queueserver").setLevel("DEBUG")

    args = parser.parse_args()
    config_worker = {}
    config_manager = {}
    if args.kafka_topic is not None:
        config_worker["kafka"] = {}
        config_worker["kafka"]["topic"] = args.kafka_topic
        config_worker["kafka"]["bootstrap"] = args.kafka_server

    if args.profile_collection_path:
        pc_path = args.profile_collection_path
        pc_path = os.path.abspath(os.path.expanduser(pc_path))
        if not os.path.exists(pc_path):
            logger.error("Profile collection directory '%s' does not exist", pc_path)
            return 1
        if not os.path.isdir(pc_path):
            logger.error("Path to profile collection '%s' is not a directory", pc_path)
            return 1
    else:
        # The default collection is the collection of simulated Ophyd devices
        #   and built-in Bluesky plans.
        pc_path = get_default_profile_collection_dir()

    config_worker["profile_collection_path"] = pc_path

    default_existing_pd_fln = "existing_plans_and_devices.yaml"
    if args.existing_plans_and_devices_path:
        existing_pd_path = os.path.expanduser(args.existing_plans_and_devices_path)
        if not os.path.isabs(existing_pd_path):
            existing_pd_path = os.path.join(pc_path, existing_pd_path)
        if not existing_pd_path.endswith(".yaml"):
            os.path.join(existing_pd_path, default_existing_pd_fln)
    else:
        existing_pd_path = os.path.join(pc_path, default_existing_pd_fln)
    if not os.path.isfile(existing_pd_path):
        logger.error(
            "The list of allowed plans and devices was not found at "
            "'%s'. Proceed without the list: all plans and devices are allowed.",
            existing_pd_path,
        )
        existing_pd_path = None

    default_user_group_pd_fln = "user_group_permissions.yaml"
    if args.user_group_permissions_path:
        user_group_pd_path = os.path.expanduser(args.user_group_permissions_path)
        if not os.path.isabs(user_group_pd_path):
            user_group_pd_path = os.path.join(pc_path, user_group_pd_path)
        if not user_group_pd_path.endswith(".yaml"):
            os.path.join(user_group_pd_path, default_existing_pd_fln)
    else:
        user_group_pd_path = os.path.join(pc_path, default_user_group_pd_fln)
    if not os.path.isfile(user_group_pd_path):
        logger.error(
            "The file with user permissions was not found at "
            "'%s'. All existing plans and devices will be allowed to all users.",
            user_group_pd_path,
        )
        user_group_pd_path = None

    config_worker["existing_plans_and_devices_path"] = existing_pd_path
    config_manager["existing_plans_and_devices_path"] = existing_pd_path
    config_worker["user_group_permissions_path"] = user_group_pd_path
    config_manager["user_group_permissions_path"] = user_group_pd_path

    wp = WatchdogProcess(config_worker=config_worker, config_manager=config_manager)
    try:
        wp.run()
    except KeyboardInterrupt:
        logger.info("The program was manually stopped.")
