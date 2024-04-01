import asyncio
import glob
import logging
import os
import pprint
import queue
import shutil
import subprocess
import sys
import time as ttime
from threading import Thread

import pytest
from jupyter_client import BlockingKernelClient

from bluesky_queueserver.manager.comms import zmq_single_request
from bluesky_queueserver.manager.config import to_boolean
from bluesky_queueserver.manager.plan_queue_ops import PlanQueueOperations
from bluesky_queueserver.manager.profile_ops import get_default_startup_dir

logger = logging.Logger(__name__)

# User name and user group name used throughout most of the tests.
_user, _user_group = "Testing Script", "primary"

_test_redis_name_prefix = "qs_unit_tests"


def use_ipykernel_for_tests():
    """
    Returns True/False if the value of USE_IPYKERNEL environment variable has
    a boolean value. The function returns *None* if the environment variable is
    not set, or the value can not be interpreted as boolean. The function
    is intended for using in test configuration.
    """
    return to_boolean(os.environ.get("USE_IPYKERNEL", None))


def copy_default_profile_collection(tmp_path, *, copy_py=True, copy_yaml=True):
    """
    Copy default profile collections (only .py files) to temporary directory.
    Returns the new temporary directory.
    """
    # Default path
    pc_path = get_default_startup_dir()
    # New path
    new_pc_path = os.path.join(tmp_path, "ipython", "profile_collection_sim", "startup")

    os.makedirs(new_pc_path, exist_ok=True)

    # Copy simulated profile collection (only .py files)
    patterns = []
    if copy_py:
        patterns.extend(["*.py", "*.ipy"])
    if copy_yaml:
        patterns.append("*.yaml")
    for pattern in patterns:
        file_pattern = os.path.join(pc_path, pattern)
        file_list = glob.glob(file_pattern)
        for fln in file_list:
            shutil.copy(fln, new_pc_path)

    return new_pc_path


def patch_first_startup_file(pc_path, additional_code):
    """
    Adds code to the beginning of a startup file.

    Parameters
    ----------
    pc_path: str
        Path to the directory with profile collection
    additional_code: str
        Code (text)that should be added to the beginning of the startup file
    """

    # Path to the first file (starts with 00)
    file_pattern_py = os.path.join(pc_path, "*.py")
    file_pattern_ipy = os.path.join(pc_path, "*.ipy")
    file_list = glob.glob(file_pattern_py) + glob.glob(file_pattern_ipy)
    file_list.sort()
    fln = file_list[0]

    # Backup the file which is about to be changed if it was not backed up already.
    fln_tmp = os.path.join(pc_path, "_backup")
    if not os.path.exists(fln_tmp):
        shutil.copy(fln, fln_tmp)

    with open(fln, "r") as file_in:
        code = file_in.readlines()

    with open(fln, "w") as file_out:
        file_out.writelines(additional_code)
        file_out.writelines(code)


def patch_first_startup_file_undo(pc_path):
    """
    Remove patches applied to the first file of profile collection.
    """
    # Path to the first file (starts with 00)
    file_pattern_py = os.path.join(pc_path, "*.py")
    file_pattern_ipy = os.path.join(pc_path, "*.ipy")
    file_list = glob.glob(file_pattern_py) + glob.glob(file_pattern_ipy)
    file_list.sort()
    fln = file_list[0]

    fln_tmp = os.path.join(pc_path, "_backup")
    if os.path.isfile(fln_tmp):
        os.remove(fln)
        os.rename(fln_tmp, fln)


def append_code_to_last_startup_file(pc_path, additional_code):
    """
    Append code to the end of the last startup file.

    Parameters
    ----------
    pc_path: str
        Path to the directory with IPython ``startup`` directory
    additional_code: str
        Code (text) that should be added to the end of the startup file
    """

    # Path to the last file
    file_pattern_py = os.path.join(pc_path, "*.py")
    file_pattern_ipy = os.path.join(pc_path, "*.ipy")
    file_list = glob.glob(file_pattern_py) + glob.glob(file_pattern_ipy)
    file_list.sort()
    fln = file_list[-1]

    # Backup the file which is about to be changed if it was not backed up already.
    fln_tmp = os.path.join(pc_path, "_backup")
    if not os.path.exists(fln_tmp):
        shutil.copy(fln, fln_tmp)

    with open(fln, "r") as file_in:
        code = file_in.readlines()

    with open(fln, "w") as file_out:
        file_out.writelines(code)
        file_out.write("\n\n")
        file_out.writelines(additional_code)


# The name of env. variable holding ZMQ public key. Public key is necessary in tests using encryption
#   and passing it using env variable (set using monkeypatch) is convenient.
_name_ev_public_key = "_TEST_QSERVER_ZMQ_PUBLIC_KEY_"


def set_qserver_zmq_public_key(mpatch, *, server_public_key):
    """
    Temporarily set environment variable holding server public key for using ``zmq_secure_request``.
    Note, that ``monkeypatch`` should precede the fixture that is starting RE Manager in test
    parameters. Otherwise the environment variable will be cleared before RE Manager is stopped and
    the test will fail.

    Parameters
    ----------
    mpatch
        instance of ``monkeypatch``.
    server_public_key : str
        ZMQ server public key represented as 40 character z85 encoded string.
    """
    mpatch.setenv(_name_ev_public_key, server_public_key)


def clear_qserver_zmq_public_key(mpatch):
    """
    Clear the environment variable holding server public key set by ``set_qserver_zmq_public_key``.
    """
    mpatch.delenv(_name_ev_public_key)


_name_ev_zmq_address = "_TEST_QSERVER_ZMQ_ADDRESS_"


def set_qserver_zmq_address(mpatch, *, zmq_server_address):
    """
    Temporarily set environment variable holding server zmq address for using with ``zmq_secure_request``.
    Note, that ``monkeypatch`` should precede the fixture that is starting RE Manager in test
    parameters. Otherwise the environment variable will be cleared before RE Manager is stopped and
    the test will fail.

    Parameters
    ----------
    mpatch
        instance of ``monkeypatch``.
    server_zmq_address : str
        ZMQ address (such as 'tcp://localhost:60615').
    """
    mpatch.setenv(_name_ev_zmq_address, zmq_server_address)


def clear_qserver_zmq_address(mpatch):
    """
    Clear the environment variable holding server zmq address set by ``set_qserver_zmq_address``.
    """
    mpatch.delenv(_name_ev_zmq_address)


def zmq_secure_request(method, params=None, *, zmq_server_address=None, server_public_key=None):
    """
    Wrapper for 'zmq_single_request'. Verifies if environment variable holding server public key is set
    and passes the key to 'zmq_single_request' . Simplifies writing tests that use RE Manager in secure mode.
    Use functions `set_qserver_zmq_public_key()` and `clear_qserver_zmq_public_key()` to set and
    clear the environment variable.

    The function also verifies if the environment variable holding ZMQ server address is set, and
    passes the address to ``zmq_single_request``. If ``zmq_server_address`` is passed as a parameter, then
    the environment variable is ignored (at least in current implementation).

    Parameters
    ----------
    method: str
        Name of the method called in RE Manager
    params: dict or None
        Dictionary of parameters (payload of the message). If ``None`` then
        the message is sent with empty payload: ``params = {}``.
    zmq_server_address: str or None
        Address of the ZMQ control socket of RE Manager. An address from the environment variable or
        the default address is used if the value is ``None``.
    server_public_key: str or None
        Server public key (z85-encoded 40 character string). The Valid public key from the server
        public/private key pair must be passed if encryption is enabled at the 0MQ server side.
        Communication requests will time out if the key is invalid. Exception will be raised if
        the key is improperly formatted. The key from the environment or is used if the environment
        variable is set, otherwise the encryption is disabled.

    """
    # Use the key from env. variable if 'server_public_key' is None
    server_public_key = server_public_key or os.environ.get(_name_ev_public_key, None)

    # Use the address from env. variable if 'zmq_server_address' is None
    zmq_server_address = zmq_server_address or os.environ.get(_name_ev_zmq_address, None)

    return zmq_single_request(
        method=method, params=params, zmq_server_address=zmq_server_address, server_public_key=server_public_key
    )


def get_manager_status():
    method, params = "status", None
    msg, _ = zmq_secure_request(method, params)
    if msg is None:
        raise TimeoutError("Timeout occurred while reading RE Manager status.")
    return msg


def get_queue():
    """
    Returns current queue.
    """
    method, params = "queue_get", None
    msg, _ = zmq_secure_request(method, params)
    if msg is None:
        raise TimeoutError("Timeout occurred while loading queue from RE Manager.")
    return msg


def get_queue_state():
    msg = get_manager_status()
    items_in_queue = msg["items_in_queue"]
    queue_is_running = msg["manager_state"] == "executing_queue"
    items_in_history = msg["items_in_history"]
    return items_in_queue, queue_is_running, items_in_history


def condition_manager_idle(msg):
    return ("manager_state" in msg) and (msg["manager_state"] == "idle")


def condition_manager_paused(msg):
    return msg["manager_state"] == "paused"


def condition_manager_idle_or_paused(msg):
    return ("manager_state" in msg) and (msg["manager_state"] in ("idle", "paused"))


def condition_manager_executing_queue(msg):
    return ("manager_state" in msg) and (msg["manager_state"] == "executing_queue")


def condition_environment_created(msg):
    return msg["worker_environment_exists"] and (msg["manager_state"] in ("idle", "executing_queue"))


def condition_environment_closed(msg):
    return (not msg["worker_environment_exists"]) and (msg["manager_state"] == "idle")


def condition_queue_processing_finished(msg):
    items_in_queue = msg["items_in_queue"]
    manager_idle = msg["manager_state"] == "idle"
    return (items_in_queue == 0) and manager_idle


def condition_ip_kernel_idle(msg):
    return msg["ip_kernel_state"] == "idle"


def condition_ip_kernel_busy(msg):
    return msg["ip_kernel_state"] == "busy"


def wait_for_condition(time, condition):
    """
    Wait until queue is processed. Note: processing of TimeoutError is needed for
    monitoring RE Manager while it is restarted.
    """
    dt = 0.5  # Period for checking the queue status
    time_stop = ttime.time() + time
    while ttime.time() < time_stop:
        ttime.sleep(dt / 2)
        try:
            msg = get_manager_status()
            if condition(msg):
                return True
        except TimeoutError:
            pass
        ttime.sleep(dt / 2)
    return False


def wait_for_task_result(time, task_uid):
    """
    Wait for the results of the task defined by ``task_uid``. Raises ``TimeoutError`` if
    timeout ``time`` is exceeded while waiting for the task result. Returns the task result
    of the task only if it was completed.
    """
    dt = 0.2  # Period for checking if the task is available
    time_stop = ttime.time() + time
    while ttime.time() < time_stop:
        ttime.sleep(dt / 2)
        try:
            resp, _ = zmq_secure_request("task_result", params={"task_uid": task_uid})

            assert resp["success"] is True, f"Request for task result failed: {resp['msg']}"
            assert resp["task_uid"] == task_uid

            if resp["status"] == "completed":
                return resp["result"]

        except TimeoutError:
            pass
        ttime.sleep(dt / 2)

    raise TimeoutError(f"Timeout occurred while waiting for results of the task {task_uid!r}")


def clear_redis_pool(redis_name_prefix=_test_redis_name_prefix):
    # Remove all Redis entries.
    pq = PlanQueueOperations(name_prefix=redis_name_prefix)

    async def run():
        await pq.start()
        await pq.delete_pool_entries()
        await pq.user_group_permissions_clear()
        await pq.lock_info_clear()
        await pq.autostart_mode_clear()
        await pq.stop_pending_clear()
        await pq.stop()

    asyncio.run(run())


class ReManager:
    def __init__(self, params=None, *, stdout=sys.stdout, stderr=sys.stdout, set_redis_name_prefix=True):
        self._p = None
        # The name is saved during the manager startup and used later to clean up Redis keys
        # If the prefix is not passed as CLI parameter, then it will always try to delete default keys.
        # If a testing using non-default prefix (passed using config file), manually set the prefix
        # uisng `set_used_redis_name_prefix` after RE Manager is started.
        self._used_redis_name_prefix = _test_redis_name_prefix
        self.start_manager(params, stdout=stdout, stderr=stderr, set_redis_name_prefix=set_redis_name_prefix)

    def set_used_redis_name_prefix(self, redis_name_prefix):
        """
        Manually set name prefix for used Redis keys. The prefix is used to clean up Redis keys
        after the test. Call this function after the manager is started. This is only needed if
        the prefix is different from default and passed using config file. This is used only in a couple
        of specific tests.
        """
        self._used_redis_name_prefix = redis_name_prefix

    def start_manager(
        self,
        params=None,
        *,
        stdout=sys.stdout,
        stderr=sys.stdout,
        set_redis_name_prefix=True,
        cleanup=True,
    ):
        """
        Start RE manager.

        Parameters
        ----------
        params : list(str)
            The list of command line parameters paqssed to the manager at startup.
        stdout
            Device to forward stdout, ``None`` - print to console.
        stderr
            Device to forward stdout, ``None`` - print to console.
        use_default_name_prefix: boolean
            Set default prefix for names of Redis keys.
        cleanup: boolean
            Perform cleanup (remove related Redis keys) before opening the manager.

        Returns
        -------
        subprocess.Popen
            Subprocess in which the manager is running. It needs to be stopped at certain point.
        """
        params = params or []

        # Set logging level for RE Manager unless it is already set in parameters
        #   Leads to excessive output and some tests may fail at tearup. Enable only when needed.
        logging_levels = ("--verbose", "--quiet", "--silent")
        if not any([_ in params for _ in logging_levels]):
            params.append("--verbose")

        # Start the manager with IPython kernel if the
        if not any([_.startswith("--use-ipython-kernel") for _ in params]) and use_ipykernel_for_tests():
            params.append("--use-ipython-kernel=ON")

        # Set default name prefix for Redis keys
        name_prefix_params = [_ for _ in params if _.startswith("--redis-name-prefix")]
        if name_prefix_params:
            # Try to extract the prefix from the parameter
            self._used_redis_name_prefix = name_prefix_params[0].split("=")[1].strip()
        elif set_redis_name_prefix:
            params.append(f"--redis-name-prefix={_test_redis_name_prefix}")

        if not self._p:
            if cleanup:
                clear_redis_pool(redis_name_prefix=self._used_redis_name_prefix)

            cmd = ["start-re-manager"]
            if params:
                cmd += params
            self._p = subprocess.Popen(cmd, universal_newlines=True, stdout=stdout, stderr=stderr)

    def check_if_stopped(self, timeout=5):
        """
        Waits for the process to be stopped. Returns ``True`` when it is stopped.
        and ``False`` otherwise.
        """
        try:
            # Make sure that the process is terminated
            self._p.wait(5)
            # Disable additional attempts to stop the process
            self._p = None
            return True
        except subprocess.TimeoutExpired:
            return False

    def stop_manager(self, timeout=10, cleanup=True):
        """
        Attempt to exit the subprocess that is running manager in orderly way and kill it
        after timeout.

        Parameters
        ----------
        timeout: float
            Timeout in seconds.
        cleanup: boolean
            Perform cleanup (remove created Redis keys) after closing the manager.
        """
        if self._p:
            try:
                # If the process is already terminated, then don't attempt to communicate with it.
                if self._p.poll() is None:
                    success, msg = False, ""

                    # Try to stop the manager in a nice way first by sending the command
                    resp1, err_msg1 = zmq_secure_request(method="manager_stop", params=None)
                    assert resp1, str(err_msg1)
                    success = resp1["success"]

                    if not success:
                        # If the command was rejected, try 'safe_off' mode that kills the RE worker environment
                        msg += f"Request to stop the manager in with the 'safe_on' option failed: {resp1['msg']}."
                        resp2, err_msg2 = zmq_secure_request(method="manager_stop", params={"option": "safe_off"})
                        assert resp2, str(err_msg2)
                        success = resp2["success"]
                        if not success:
                            msg += (
                                " Request to stop the manager in with the 'safe_off' "
                                f"option failed: {resp2['msg']}."
                            )
                        else:
                            msg += " Attempting to stop the manager using 'safe_off' option"

                    # If both requests failed, then there is nothing to wait
                    if not success:
                        raise RuntimeError(msg)

                    # If only the first request failed, then there is a chance that the manager stops
                    try:
                        self._p.communicate(timeout=timeout)
                        self._p = None
                    except subprocess.TimeoutExpired:
                        raise RuntimeError(f"Timeout occured while waiting for the manager to stop: {msg}")

                    # If at least one of the requests failed and there is an error message, then fail the test
                    if msg:
                        raise RuntimeError(msg)

            except Exception as ex:
                # The manager is not responsive, so just kill the process.
                if self._p:
                    self._p.terminate()
                    self._p.wait(timeout)
                assert False, f"RE Manager failed to stop: {str(ex)}"

            finally:
                if cleanup:
                    clear_redis_pool(redis_name_prefix=self._used_redis_name_prefix)
                self._p = None

    def kill_manager(self, timeout=10):
        """
        Kill the subprocess running RE Manager. Use only to stop RE Manager that failed to start correctly.
        The operation will always succeed.

        Parameters
        ----------
        timeout: float
            Timeout in seconds.
        """
        if self._p:
            self._p.terminate()
            self._p.wait(timeout)
            clear_redis_pool(redis_name_prefix=self._used_redis_name_prefix)


def _reset_queue_mode():
    """Reset queue mode to the default mode"""
    resp, msg = zmq_secure_request("queue_mode_set", params={"mode": "default"})
    if resp["success"] is not True:
        raise RuntimeError(msg)


@pytest.fixture
def re_manager_cmd():
    """
    Start RE Manager by running `start-re-manager` as a subprocess. Pass the list of
    command-line parameters to `start-re-manager`. Tests will communicate with RE Manager via ZeroMQ.

    Examples of using the fixture:
    ``re_manager_cmd()`` or ``re_manager_cmd([]) - create RE Manager without command-line parameters
    ``re_manager_cmd(["-h"])`` - equivalent to ``start-re-manager -h``.
    """
    re = {"re": None}
    failed_to_start = False

    def _create(params, *, stdout, stderr, set_redis_name_prefix):
        """
        Create RE Manager. ``start-re-manager`` is called with command line parameters from
          the list ``params``.
        """
        nonlocal re, failed_to_start
        re["re"] = ReManager(params, stdout=stdout, stderr=stderr, set_redis_name_prefix=set_redis_name_prefix)

        # Wait until RE Manager is started. Raise exception if the server failed to start.
        if not wait_for_condition(time=10, condition=condition_manager_idle):
            failed_to_start = True
            re["re"].kill_manager()
            raise TimeoutError("Timeout: RE Manager failed to start.")

        _reset_queue_mode()

    def _close():
        """
        Close RE Manager if it exists.
        """
        nonlocal re, failed_to_start
        if re["re"] is not None:
            if not failed_to_start:
                re["re"].stop_manager()
            else:
                re["re"].kill_manager()

    def create_re_manager(params=None, *, stdout=sys.stdout, stderr=sys.stdout, set_redis_name_prefix=True):
        params = params or []
        _close()
        _create(params, stdout=stdout, stderr=stderr, set_redis_name_prefix=set_redis_name_prefix)
        return re["re"]

    yield create_re_manager

    _close()


@pytest.fixture
def re_manager():
    """
    Start RE Manager as a subprocess. Tests will communicate with RE Manager via ZeroMQ.
    """
    re = ReManager()
    failed_to_start = False

    # Wait until RE Manager is started. Raise exception if the server failed to start.
    if not wait_for_condition(time=10, condition=condition_manager_idle):
        failed_to_start = True
        re.kill_manager()
        raise TimeoutError("Timeout: RE Manager failed to start.")

    _reset_queue_mode()

    yield re  # Nothing to return
    if not failed_to_start:
        re.stop_manager()
    else:
        re.kill_manager()


@pytest.fixture
def re_manager_pc_copy(tmp_path):
    """
    Start RE Manager as a subprocess. Tests will communicate with RE Manager via ZeroMQ.
    Copy profile collection and return its temporary path.
    """
    pc_path = copy_default_profile_collection(tmp_path)
    re = ReManager(["--startup-dir", pc_path])
    failed_to_start = False

    # Wait until RE Manager is started. Raise exception if the server failed to start.
    if not wait_for_condition(time=10, condition=condition_manager_idle):
        failed_to_start = True
        re.kill_manager()
        raise TimeoutError("Timeout: RE Manager failed to start.")

    _reset_queue_mode()

    yield re, pc_path  # Location of the copy of the default profile collection.
    if not failed_to_start:
        re.stop_manager()
    else:
        re.kill_manager()


@pytest.fixture
def reset_sys_modules():
    """
    Resets `sys.modules` after each test. Allows importing the same module in multiple tests independently.
    Intended for use in the tests for the functions that import modules or execute scripts that import modules.
    """
    # Save the set of keys before the test is called
    sys_modules = list(sys.modules.keys())

    yield

    # Remove entries for all the modules loaded during the test
    for key in list(sys.modules.keys()):
        if key not in sys_modules:
            print(f"Deleting the key '{key}'")
            del sys.modules[key]


class IPKernelClient:
    """
    Simplistic IPython Kernel Client that connects to a kernel running in the worker
    and allows to run commands. The kernel must already exist (the environment must be opened)
    before initializing the client.
    """

    def __init__(self):
        self.ip_kernel_client = None

    def start(self):
        """
        Load IPython kernel connection info from RE Manager and start the client.
        """
        if self.ip_kernel_client:
            self.stop()

        resp, _ = zmq_single_request("config_get")
        assert resp["success"] is True, pprint.pformat(resp)
        assert "config" in resp, pprint.pformat(resp)
        assert "ip_connect_info" in resp["config"], pprint.pformat(resp)
        connect_info = resp["config"]["ip_connect_info"]

        self.ip_kernel_client = BlockingKernelClient()
        self.ip_kernel_client.load_connection_info(connect_info)
        self.ip_kernel_client.start_channels()

    def stop(self):
        """
        Stop the client. Failing to stop the client is causing a leak of opened file
        descriptors and eventually causes the tests fail.
        """
        if self.ip_kernel_client:
            self.ip_kernel_client.stop_channels()
            self.ip_kernel_client = None

    def execute(self, command):
        """
        Run the command (execute a cell) in the remote client. The function does not wait
        for completion. The command is not saved to IPython history.

        Parameters
        ----------
        command: str
            Python code to execute in IPython kernel.
        """
        self.ip_kernel_client.execute(command, reply=False, store_history=False)

    def execute_with_check(self, command, *, pause=1, timeout=10):
        """
        Run the command (execute a cell) in the remote client and wait until the kernel
        starts execution of the command (execution state is changed to 'busy').

        Parameters
        ----------
        command: str
            Python code to execute in IPython kernel.
        """

        def func():
            ttime.sleep(1)
            print(f"Sending direct request to the kernel: command={command!r}")
            self.execute(command)
            print("Direct request is sent")

        th = Thread(target=func)
        th.start()

        assert self.wait_for_execution_state_change(timeout=timeout) == "busy"
        print("Execution of the command by IPython kernel was started.")
        th.join()

    def wait_for_execution_state_change(self, *, timeout=1):
        """
        Waits for change of execution state of the kernel. Returns the new state.

        Parameters
        ----------
        timeout, float
            Timeout in seconds.

        Returns
        -------
        str
            The new state of the kernel: 'busy' or 'idle'.
        """
        t0 = ttime.time()
        while ttime.time() < t0 + timeout:
            try:
                msg = self.ip_kernel_client.get_iopub_msg(timeout=0.1)
                if msg["header"]["msg_type"] == "status":
                    return msg["content"]["execution_state"]
            except queue.Empty:
                pass

        raise TimeoutError("Timeout occurred while waiting for change of the state of IPython kernel")


@pytest.fixture
def ip_kernel_simple_client():
    """
    Instantiates a simple IP kernel client for tests. The client connects to the running client
    and allows to send commands to the kernel (``execute`` method) and wait for execution state changes
    (``wait_for_execution_state_change`` method).

    Use ``ip_kernel_simple_client.start()`` to connect to the running kernel. Repeated calls will make
    the client reconnect to the kernel. The fixture automatically stops the client (closes 0MQ sockets).
    """
    client = IPKernelClient()
    yield client
    client.stop()
