import os
import glob
import shutil
import pytest
import subprocess
import asyncio
import time as ttime
import intake
import tempfile
import sys

from databroker import catalog_search_path

from bluesky_queueserver.manager.profile_ops import get_default_startup_dir
from bluesky_queueserver.manager.plan_queue_ops import PlanQueueOperations
from bluesky_queueserver.manager.comms import zmq_single_request

import logging

logger = logging.Logger(__name__)


def copy_default_profile_collection(tmp_path, *, copy_yaml=True):
    """
    Copy default profile collections (only .py files) to temporary directory.
    Returns the new temporary directory.
    """
    # Default path
    pc_path = get_default_startup_dir()
    # New path
    new_pc_path = os.path.join(tmp_path, "startup")

    os.makedirs(new_pc_path, exist_ok=True)

    # Copy simulated profile collection (only .py files)
    patterns = ["*.py", "*.ipy"]
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


def zmq_secure_request(method, params=None, *, zmq_server_address=None):
    """
    Wrapper for 'zmq_single_request'. Verifies if environment variable holding server public key is set
    and passes the key to 'zmq_single_request' . Simplifies writing tests that use RE Manager in secure mode.
    Use functions `set_qserver_zmq_public_key()` and `clear_qserver_zmq_public_key()` to set and
    clear the environment variable.

    The function also verifies if the environment variable holding ZMQ server address is set, and
    passes the address to ``zmq_single_request``. If ``zmq_server_address`` is passed as a parameter, then
    the environment variable is ignored (at least in current implementation).
    """
    server_public_key = None

    if _name_ev_public_key in os.environ:
        server_public_key = os.environ[_name_ev_public_key]

    # Use the address passed in environment variable only if the parameter 'zmq_server_address' is None
    if (_name_ev_zmq_address in os.environ) and (zmq_server_address is None):
        zmq_server_address = os.environ[_name_ev_zmq_address]

    return zmq_single_request(
        method=method, params=params, zmq_server_address=zmq_server_address, server_public_key=server_public_key
    )


def get_queue_state():
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


def get_reduced_state_info():
    msg = get_queue_state()
    items_in_queue = msg["items_in_queue"]
    queue_is_running = msg["manager_state"] == "executing_queue"
    items_in_history = msg["items_in_history"]
    return items_in_queue, queue_is_running, items_in_history


def condition_manager_idle(msg):
    return ("manager_state" in msg) and (msg["manager_state"] == "idle")


def condition_manager_paused(msg):
    return msg["manager_state"] == "paused"


def condition_environment_created(msg):
    return msg["worker_environment_exists"] and (msg["manager_state"] == "idle")


def condition_environment_closed(msg):
    return (not msg["worker_environment_exists"]) and (msg["manager_state"] == "idle")


def condition_queue_processing_finished(msg):
    items_in_queue = msg["items_in_queue"]
    queue_is_running = msg["manager_state"] == "executing_queue"
    return (items_in_queue == 0) and not queue_is_running


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
            msg = get_queue_state()
            if condition(msg):
                return True
        except TimeoutError:
            pass
        ttime.sleep(dt / 2)
    return False


def clear_redis_pool():
    # Remove all Redis entries.
    pq = PlanQueueOperations()

    async def run():
        await pq.start()
        await pq.delete_pool_entries()

    asyncio.run(run())


@pytest.fixture(scope="module")
def db_catalog():
    """
    Creates msgpack-based catalog, returns reference to the catalog and the catalog name.
    The catalog name may be used as a configuration name for subscribing to databroker.
    Yields the dictionary: ``db_catalog["catalog"]`` - reference to the catalog,
    ``db_catalog["catalog_name"]`` - string that represents the catalog name.
    """
    db_catalog_name = "qserver_tests"

    config_dir = catalog_search_path()[0]
    config_path = os.path.join(config_dir, f"{db_catalog_name}.yml")

    files_dir = os.path.join(tempfile.gettempdir(), "qserver_tests", "db_catalog_files")
    files_dir = os.path.abspath(files_dir)
    files_path = os.path.join(files_dir, "*.msgpack")

    # Delete the directory 'db_catalog_files' and its contents in case it exists.
    if os.path.isdir(files_dir):
        shutil.rmtree(files_dir)

    os.makedirs(config_dir, exist_ok=True)
    os.makedirs(files_dir, exist_ok=True)

    config_file_contents = f"""
sources:
  {db_catalog_name}:
    driver: bluesky-msgpack-catalog
    args:
      paths:
        - "{files_path}"
"""

    with open(config_path, "w") as file_out:
        file_out.writelines(config_file_contents)

    # The catalog can not be opened using intake right away:
    #   cat = intake.open_catalog(config_path)
    # But standard way of opening a catalog such as
    #   from databroker import catalog
    #   catalog[qserver_tests]
    # and subscription of Run Engine to Data Broker will not work.
    # So we need the delay.
    ttime.sleep(2.0)

    cat = intake.open_catalog(config_path)
    cat = cat[db_catalog_name]

    yield {"catalog": cat, "catalog_name": db_catalog_name}

    os.remove(config_path)
    shutil.rmtree(files_dir)


class ReManager:
    def __init__(self, params=None, *, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL):
        self._p = None
        self._start_manager(params, stdout=stdout, stderr=stderr)

    def _start_manager(self, params=None, *, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL):
        """
        Start RE manager.

        Parameters
        ----------
        params : list(str)
            The list of command line parameters passed to the manager at startup
        stdout
            Device to forward stdout, ``None`` - print to console
        stderr
            Device to forward stdout, ``None`` - print to console

        Returns
        -------
        subprocess.Popen
            Subprocess in which the manager is running. It needs to be stopped at certain point.
        """
        if not self._p:
            clear_redis_pool()

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

    def stop_manager(self, timeout=10):
        """
        Attempt to exit the subprocess that is running manager in orderly way and kill it
        after timeout.

        Parameters
        ----------
        p: subprocess.Popen
            Subprocess in which the manager is running
        timeout: float
            Timeout in seconds.
        """
        if self._p:
            try:
                # If the process is already terminated, then don't attempt to communicate with it.
                if self._p.poll() is None:
                    # Try to stop the manager in a nice way first by sending the command
                    resp, err_msg = zmq_secure_request(method="manager_stop", params=None)
                    assert resp, str(err_msg)
                    assert resp["success"] is True, f"Request to stop the manager failed: {resp['msg']}."

                    self._p.wait(timeout)

                clear_redis_pool()

            except Exception as ex:
                # The manager is not responsive, so just kill the process.
                self._p.kill()
                clear_redis_pool()
                assert False, f"RE Manager failed to stop: {str(ex)}"

            self._p = None

    def kill_manager(self):
        """
        Kill the subprocess running RE Manager. Use only to stop RE Manager that failed to start correctly.
        The operation will always succeed.
        """
        if self._p:
            self._p.kill()
            clear_redis_pool()


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

    def _create(params, *, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL):
        """
        Create RE Manager. ``start-re-manager`` is called with command line parameters from
          the list ``params``.
        """
        nonlocal re, failed_to_start
        re["re"] = ReManager(params, stdout=stdout, stderr=stderr)

        # Wait until RE Manager is started. Raise exception if the server failed to start.
        if not wait_for_condition(time=10, condition=condition_manager_idle):
            failed_to_start = True
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

    def create_re_manager(params=None, *, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL):
        params = params or []
        _close()
        _create(params, stdout=stdout, stderr=stderr)
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
