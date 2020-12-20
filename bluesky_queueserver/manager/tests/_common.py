import os
import glob
import shutil
import pytest
import subprocess
import asyncio
import time as ttime
import intake
import tempfile

from databroker import catalog_search_path

from bluesky_queueserver.manager.profile_ops import get_default_profile_collection_dir
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
    pc_path = get_default_profile_collection_dir()
    # New path
    new_pc_path = os.path.join(tmp_path, "startup")

    os.makedirs(new_pc_path, exist_ok=True)

    # Copy simulated profile collection (only .py files)
    patterns = ["[0-9][0-9]*.py"]
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
    file_pattern = os.path.join(pc_path, "[0-9][0-9]*.py")
    file_list = glob.glob(file_pattern)
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
    file_pattern = os.path.join(pc_path, "[0-9][0-9]*.py")
    file_list = glob.glob(file_pattern)
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
    file_pattern = os.path.join(pc_path, "[0-9][0-9]*.py")
    file_list = glob.glob(file_pattern)
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


def get_queue_state():
    method, params = "status", None
    msg, _ = zmq_single_request(method, params)
    if msg is None:
        raise TimeoutError("Timeout occurred while reading RE Manager status.")
    return msg


def get_queue():
    """
    Returns current queue.
    """
    method, params = "queue_get", None
    msg, _ = zmq_single_request(method, params)
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


@pytest.fixture(scope='session')
def db_catalog():
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
    def __init__(self, params=None):
        self._p = None
        self._start_manager(params)

    def _start_manager(self, params=None):
        """
        Start RE manager.

        Parameters
        ----------
        params: list(str)
            The list of command line parameters passed to the manager at startup

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
            self._p = subprocess.Popen(
                cmd, universal_newlines=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )

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
                # Try to stop the manager in a nice way first by sending the command
                resp, _ = zmq_single_request(method="manager_stop", params=None)
                assert resp["success"] is True, f"Request to stop the manager failed: {resp['msg']}."

                self._p.wait(timeout)
                clear_redis_pool()

            except Exception as ex:
                # The manager is not responsive, so just kill the process.
                self._p.kill()
                clear_redis_pool()
                assert False, f"RE Manager failed to stop: {str(ex)}"

            self._p = None


@pytest.fixture(scope='session')
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


@pytest.fixture
def re_manager_cmd():
    """
    Start RE Manager by running `start-re-manager` as a subprocess. Pass the list of
    command-line parameters to `start-re-manager`. Tests will communicate with RE Manager via ZeroMQ.

    Examples of using the fixture:
    ``re_manager_cmd()`` or ``re_manager_cmd([]) - create RE Manager without command-line parameters
    ``re_manager-cmd(["-h"])`` - equivalent to ``start-re-manager -h``.
    """
    re = {"re": None}

    def _create(params):
        """
        Create RE Manager. ``start-re-manager`` is called with command line parameters from
          the list ``params``.
        """
        nonlocal re
        re["re"] = ReManager(params)

        # Wait until RE Manager is started
        assert wait_for_condition(time=10, condition=condition_manager_idle), "Timeout: RE Manager failed to start."

    def _close():
        """
        Close RE Manager if it exists.
        """
        nonlocal re
        if re["re"] is not None:
            re["re"].stop_manager()

    def create_re_manager(params=None):
        params = params or []
        _close()
        _create(params)

    yield create_re_manager  # Nothing to return

    _close()


@pytest.fixture
def re_manager():
    """
    Start RE Manager as a subprocess. Tests will communicate with RE Manager via ZeroMQ.
    """
    re = ReManager()

    # Wait until RE Manager is started
    assert wait_for_condition(time=10, condition=condition_manager_idle), "Timeout: RE Manager failed to start."

    yield re  # Nothing to return
    re.stop_manager()


@pytest.fixture
def re_manager_pc_copy(tmp_path):
    """
    Start RE Manager as a subprocess. Tests will communicate with RE Manager via ZeroMQ.
    Copy profile collection and return its temporary path.
    """
    pc_path = copy_default_profile_collection(tmp_path)
    re = ReManager(["-p", pc_path])

    # Wait until RE Manager is started
    assert wait_for_condition(time=10, condition=condition_manager_idle), "Timeout: RE Manager failed to start."

    yield re, pc_path  # Location of the copy of the default profile collection.
    re.stop_manager()
