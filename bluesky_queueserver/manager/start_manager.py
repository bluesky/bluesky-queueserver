import argparse
from multiprocessing import Pipe, Queue
import threading
import time as ttime
import os
from importlib.util import find_spec

from .worker import RunEngineWorker
from .manager import RunEngineManager
from .comms import PipeJsonRpcReceive, validate_zmq_key
from .profile_ops import get_default_startup_dir
from .output_streaming import PublishConsoleOutput, setup_console_output_redirection

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
        msg_queue=None,
        log_level=logging.DEBUG,
    ):

        self._log_level = log_level

        self._cls_run_engine_worker = cls_run_engine_worker
        self._cls_run_engine_manager = cls_run_engine_manager

        self._msg_queue = msg_queue

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
                conn=self._manager_conn,
                name="RE Worker Process",
                config=self._config_worker,
                msg_queue=self._msg_queue,
                log_level=self._log_level,
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
            msg_queue=self._msg_queue,
            log_level=self._log_level,
        )
        self._re_manager.start()

    def run(self):

        logging.basicConfig(level=max(logging.WARNING, self._log_level))
        logging.getLogger(__name__).setLevel(self._log_level)

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
                logger.error("Timeout detected by Watchdog. RE Manager malfunctioned and must be restarted")
                self._re_manager.kill()
                self._start_re_manager()

        self._comm_to_manager.stop()
        logger.info("RE Watchdog is stopped")


def start_manager():

    s_enc = (
        "Encryption for ZeroMQ communication server may be enabled by setting QSERVER_ZMQ_PRIVATE_KEY\n"
        "environment variable to a valid private key (z85-encoded 40 character string):\n\n"
        "    export QSERVER_ZMQ_PRIVATE_KEY='<private_key>'\n\n"
        "A public/private key pair may be generated by running 'qserver-zmq-keys'. If RE Manager is\n"
        "configured to use encrypted channel, the encryption must also be enabled at the client side\n"
        "using the public key from the generated pair. Encryption is disabled by default."
    )

    def formatter(prog):
        # Set maximum width such that printed help mostly fits in the RTD theme code block (documentation).
        return argparse.RawDescriptionHelpFormatter(prog, max_help_position=20, width=90)

    parser = argparse.ArgumentParser(
        description=f"Start Run Engine (RE) Manager\nbluesky-queueserver version {__version__}\n\n{s_enc}",
        formatter_class=formatter,
    )
    parser.add_argument(
        "--zmq-addr",
        dest="zmq_addr",
        type=str,
        default="tcp://*:60615",
        help="The address of ZMQ server (control connection), e.g. 'tcp://*:60615' (default: %(default)s).",
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--startup-dir",
        dest="startup_dir",
        type=str,
        help="Path to directory that contains a set of startup files (*.py and *.ipy). All the scripts "
        "in the directory will be sorted in alphabetical order of their names and loaded in "
        "the Run Engine Worker environment. The set of startup files may be located in any accessible "
        "directory.",
    )
    group.add_argument(
        "--startup-profile",
        dest="profile_name",
        type=str,
        help="The name of IPython profile used to find the location of startup files. Example: if IPython is "
        "configured to look for profiles in '~/.ipython' directory (default behavior) and the profile "
        "name is 'testing', then RE Manager will look for startup files in "
        "'~/.ipython/profile_testing/startup' directory.",
    )
    group.add_argument(
        "--startup-module",
        dest="startup_module_name",
        type=str,
        help="The name of the module with startup code. The module is imported each time the RE Worker "
        "environment is opened. Example: 'some.startup.module'. Paths to the list of existing "
        "plans and devices (--existing-plans-and-devices) and user group permissions "
        "(--user-group-permissions) must be explicitly specified if this option is used.",
    )

    group.add_argument(
        "--startup-script",
        dest="startup_script_path",
        type=str,
        help="The path to the script with startup code. The script is loaded each time the RE Worker "
        "environment is opened. Example: '~/startup/scripts/scripts.py'. Paths to the list of existing "
        "plans and devices (--existing-plans-and-devices) and user group permissions "
        "(--user-group-permissions) must be explicitly specified if this option is used.",
    )

    parser.add_argument(
        "--existing-plans-devices",
        dest="existing_plans_and_devices_path",
        type=str,
        help="Path to file that contains the list of existing plans and devices. "
        "The path may be a relative path to the profile collection directory. "
        "If the path is directory, then the default file name "
        "'existing_plans_and_devices.yaml' is used.",
    )
    parser.add_argument(
        "--user-group-permissions",
        dest="user_group_permissions_path",
        type=str,
        help="Path to file that contains lists of plans and devices available to users. "
        "The path may be a relative path to the profile collection directory. "
        "If the path is a directory, then the default file name "
        "'user_group_permissions.yaml' is used.",
    )

    parser.add_argument(
        "--redis-addr",
        dest="redis_addr",
        type=str,
        default="localhost",
        help="The address of Redis server, e.g. 'localhost', '127.0.0.1', 'localhost:6379' "
        "(default: %(default)s). ",
    )

    parser.add_argument("--kafka-topic", dest="kafka_topic", type=str, help="The kafka topic to publish to.")
    parser.add_argument(
        "--kafka-server",
        dest="kafka_server",
        type=str,
        help="Bootstrap server to connect (default: %(default)s).",
        default="127.0.0.1:9092",
    )

    parser.add_argument(
        "--zmq-data-proxy-addr",
        dest="zmq_data_proxy_addr",
        type=str,
        help="The address of ZMQ proxy used to publish data. If the parameter is specified, RE is "
        "subscribed to 'bluesky.callbacks.zmq.Publisher' and documents are published via 0MQ proxy. "
        "0MQ Proxy (see Bluesky 0MQ documentation) should be started before plans are executed. "
        "The address should be in the form '127.0.0.1:5567' or 'localhost:5567'. The address is passed "
        "to 'bluesky.callbacks.zmq.Publisher'. It is recommended to use Kafka instead of 0MQ proxy in "
        "production data acquisition systems and use Kafka instead.",
    )

    parser.add_argument(
        "--keep-re",
        dest="keep_re",
        action="store_true",
        help="Keep RE created in profile collection. If the flag is set, RE must be "
        "created in the profile collection for the plans to run. RE will also "
        "keep all its subscriptions. Also must be subscribed to the Data Broker "
        "inside the profile collection, since '--databroker-config' argument "
        "is ignored.",
    )
    parser.add_argument(
        "--use-persistent-metadata",
        dest="use_persistent_metadata",
        action="store_true",
        help="Use msgpack-based persistent storage for scan metadata. Currently this "
        "is the preferred method to keep continuously incremented sequence of "
        "Run IDs between restarts of RE.",
    )
    parser.add_argument(
        "--databroker-config",
        dest="databroker_config",
        type=str,
        help="Name of the Data Broker configuration file.",
    )

    group_console_output = parser.add_argument_group(
        "Configure console output",
        "The arguments allow to configure printing and publishing of the console output\n"
        "generated by RE Manager. The arguments allow to set the address of 0MQ socket\n"
        "and enable/disable printing and/or publishing of the console output.",
    )

    group_console_output.add_argument(
        "--zmq-publish-console-addr",
        dest="zmq_publish_console_addr",
        type=str,
        default="tcp://*:60625",
        help="The address of ZMQ server (stdout and stderr publishing), e.g. 'tcp://*:60625' "
        "(default: %(default)s).",
    )

    group_console_output.add_argument(
        "--zmq-publish-console",
        dest="zmq_publish_console",
        type=str,
        choices=["ON", "OFF"],
        default="OFF",
        help="Enable (ON) or disable (OFF) publishing of console output to 0MQ (default: %(default)s).",
    )

    group_console_output.add_argument(
        "--console-output",
        dest="console_output",
        type=str,
        choices=["ON", "OFF"],
        default="ON",
        help="Enable (ON) or disable (OFF) printing of console output in the Re Manager terminal. "
        "(default: %(default)s)",
    )

    group_verbosity = parser.add_argument_group(
        "Logging verbosity settings",
        "The default logging settings (loglevel=INFO) provide optimal amount of data to monitor\n"
        "the operation of RE Manager. Select '--verbose' option to see detailed data on received and\n"
        "sent messages, added and executed plans, etc. Use options '--quiet' and '--silent'\n"
        "to see only warnings and error messages or disable logging output.",
    )
    group_v = group_verbosity.add_mutually_exclusive_group()
    group_v.add_argument(
        "--verbose",
        dest="logger_verbose",
        action="store_true",
        help="Set logger level to DEBUG.",
    )
    group_v.add_argument(
        "--quiet",
        dest="logger_quiet",
        action="store_true",
        help="Set logger level to WARNING.",
    )
    group_v.add_argument(
        "--silent",
        dest="logger_silent",
        action="store_true",
        help="Disables logging output.",
    )

    args = parser.parse_args()

    log_level = logging.INFO
    if args.logger_verbose:
        log_level = logging.DEBUG
    elif args.logger_quiet:
        log_level = logging.WARNING
    elif args.logger_silent:
        log_level = logging.CRITICAL + 1

    console_output_on = args.console_output == "ON"
    zmq_publish_console_on = args.zmq_publish_console == "ON"
    zmq_publish_console_addr = args.zmq_publish_console_addr

    msg_queue = Queue()
    setup_console_output_redirection(msg_queue)

    logging.basicConfig(level=max(logging.WARNING, log_level))
    logging.getLogger("bluesky_queueserver").setLevel(log_level)

    stream_publisher = PublishConsoleOutput(
        msg_queue=msg_queue,
        console_output_on=console_output_on,
        zmq_publish_on=zmq_publish_console_on,
        zmq_publish_addr=zmq_publish_console_addr,
    )

    if zmq_publish_console_on:
        # Wait for a short period to allow monitoring applications to connect.
        ttime.sleep(1)

    stream_publisher.start()

    config_worker = {}
    config_manager = {}
    if args.kafka_topic is not None:
        config_worker["kafka"] = {}
        config_worker["kafka"]["topic"] = args.kafka_topic
        config_worker["kafka"]["bootstrap"] = args.kafka_server

    if args.zmq_data_proxy_addr is not None:
        config_worker["zmq_data_proxy_addr"] = args.zmq_data_proxy_addr

    startup_dir, startup_module_name, startup_script_path = None, None, None

    # Find startup directory
    if args.profile_name:
        profile_name = args.profile_name
        if find_spec("IPython"):
            import IPython

            path_to_ipython = IPython.paths.get_ipython_dir()
        else:
            logger.error(
                "IPython is not installed. Specify directory to startup file by using '--startup-dir' option."
            )
            return 1
        ipython_dir = os.path.abspath(path_to_ipython)
        profile_name_full = f"profile_{profile_name}"
        startup_dir = os.path.join(ipython_dir, profile_name_full, "startup")
    elif args.startup_dir:
        startup_dir = args.startup_dir
        startup_dir = os.path.abspath(os.path.expanduser(startup_dir))
    elif args.startup_module_name:
        startup_module_name = args.startup_module_name
    elif args.startup_script_path:
        startup_script_path = os.path.abspath(os.path.expanduser(args.startup_script_path))
    else:
        # The default collection is the collection of simulated Ophyd devices
        #   and built-in Bluesky plans.
        startup_dir = get_default_startup_dir()

    if sum([_ is not None for _ in [startup_dir, startup_module_name, startup_script_path]]) != 1:
        logger.error("Multiple or no startup code sources were specified.")
        return 1

    # Primitive error processing: make sure that all essential data exists.
    if startup_dir is not None:
        if not os.path.exists(startup_dir):
            logger.error("Startup directory '%s' does not exist", startup_dir)
            return 1
        if not os.path.isdir(startup_dir):
            logger.error("Startup directory '%s' is not a directory", startup_dir)
            return 1
    elif (startup_module_name is not None) or (startup_script_path is not None):
        # startup_module_name or startup_script_path is set. This option requires
        #   the paths to existing plans and devices and user group permissions to be set.
        #   (The default directory can not be used in this case).
        if not args.existing_plans_and_devices_path:
            logger.error(
                "The path to the list of existing plans and devices (--existing-plans-and-devices) "
                "is not specified."
            )
            return 1
        if not args.user_group_permissions_path:
            logger.error(
                "The path to the file containing user group permissions (--user-group-permissions) "
                "is not specified."
            )
            return 1
        # Check if startup script exists (if it is specified)
        if startup_script_path is not None:
            if not os.path.isfile(startup_script_path):
                logger.error(f"The script '{startup_script_path}' is not found.")
                return 1

    config_worker["keep_re"] = args.keep_re
    config_worker["use_persistent_metadata"] = args.use_persistent_metadata

    config_worker["databroker"] = {}
    if args.databroker_config:
        config_worker["databroker"]["config"] = args.databroker_config

    config_worker["startup_dir"] = startup_dir
    config_worker["startup_module_name"] = startup_module_name
    config_worker["startup_script_path"] = startup_script_path

    default_existing_pd_fln = "existing_plans_and_devices.yaml"
    if args.existing_plans_and_devices_path:
        existing_pd_path = os.path.expanduser(args.existing_plans_and_devices_path)
        if not os.path.isabs(existing_pd_path) and startup_dir:
            existing_pd_path = os.path.join(startup_dir, existing_pd_path)
        if not existing_pd_path.endswith(".yaml"):
            existing_pd_path = os.path.join(existing_pd_path, default_existing_pd_fln)
    else:
        existing_pd_path = os.path.join(startup_dir, default_existing_pd_fln)
    if not os.path.isfile(existing_pd_path):
        logger.error(
            "The list of allowed plans and devices was not found at "
            "'%s'. Proceed without the list: all plans and devices will be accepted by RE Manager.",
            existing_pd_path,
        )
        existing_pd_path = None

    default_user_group_pd_fln = "user_group_permissions.yaml"
    if args.user_group_permissions_path:
        user_group_pd_path = os.path.expanduser(args.user_group_permissions_path)
        if not os.path.isabs(user_group_pd_path) and startup_dir:
            user_group_pd_path = os.path.join(startup_dir, user_group_pd_path)
        if not user_group_pd_path.endswith(".yaml"):
            user_group_pd_path = os.path.join(user_group_pd_path, default_user_group_pd_fln)
    else:
        user_group_pd_path = os.path.join(startup_dir, default_user_group_pd_fln)
    if not os.path.isfile(user_group_pd_path):
        logger.error(
            "The file with user permissions was not found at "
            "'%s'. User groups are not defined. USERS WILL NOT BE ABLE TO SUBMIT PLANS.",
            user_group_pd_path,
        )
        user_group_pd_path = None

    config_worker["existing_plans_and_devices_path"] = existing_pd_path
    config_manager["existing_plans_and_devices_path"] = existing_pd_path
    config_worker["user_group_permissions_path"] = user_group_pd_path
    config_manager["user_group_permissions_path"] = user_group_pd_path

    # Read private key from the environment variable, then check if the CLI parameter exists
    zmq_private_key = os.environ.get("QSERVER_ZMQ_PRIVATE_KEY", None)
    zmq_private_key = zmq_private_key if zmq_private_key else None  # Case of key==""
    if zmq_private_key is not None:
        try:
            validate_zmq_key(zmq_private_key)
        except Exception as ex:
            logger.error("ZMQ private key is improperly formatted: %s", str(ex))
            return 1

    config_manager["zmq_addr"] = args.zmq_addr
    config_manager["zmq_private_key"] = zmq_private_key

    redis_addr = args.redis_addr
    if redis_addr.count(":") > 1:
        logger.error(f"Redis address is incorrectly formatted: '{redis_addr}'")
        return 1
    config_manager["redis_addr"] = redis_addr

    wp = WatchdogProcess(
        config_worker=config_worker, config_manager=config_manager, msg_queue=msg_queue, log_level=log_level
    )
    try:
        wp.run()
    except KeyboardInterrupt:
        logger.info("The program was manually stopped")
