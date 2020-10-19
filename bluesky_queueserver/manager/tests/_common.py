import os
import glob
import shutil
import pytest
import subprocess
import asyncio
import time as ttime
import zmq
import zmq.asyncio

from bluesky_queueserver.manager.profile_ops import get_default_profile_collection_dir
from bluesky_queueserver.manager.plan_queue_ops import PlanQueueOperations

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
    code_to_add: str
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


class TinyZmqClient:
    """
    This is a simplified version of ZMQ Client, which is very similar to the client
    used in ``qserver_cli``. The server does not perform any verification of input
    and output data and intended for testing 0MQ API of RE Manager.
    """

    def __init__(self, *, address=None):
        # ZeroMQ communication
        self._ctx = zmq.asyncio.Context()
        self._zmq_socket = None

        if address is None:
            self._zmq_server_address = "tcp://localhost:5555"
        else:
            self._zmq_server_address = address

        # The attributes for storing output command and received input
        self._msg_method_out = ""
        self._msg_params_out = {}
        self._msg_in = {}
        self._msg_err_in = ""

    async def _zmq_send(self, msg):
        await self._zmq_socket.send_json(msg)

    async def _zmq_receive(self):
        return await self._zmq_socket.recv_json()

    async def _zmq_communicate(self, msg_out):
        try:
            await self._zmq_send(msg_out)
            msg_in = await self._zmq_receive()
            return msg_in
        except Exception as ex:
            raise RuntimeError(f"ZeroMQ communication failed: {str(ex)}")

    async def _zmq_open_connection(self, address):
        if self._zmq_socket.connect(address):
            msg_err = f"Failed to connect to the server '{address}'"
            raise RuntimeError(msg_err)

    async def zmq_single_request(self):
        self._zmq_socket = self._ctx.socket(zmq.REQ)
        self._zmq_socket.RCVTIMEO = 2000  # Timeout for 'recv' operation
        self._zmq_socket.SNDTIMEO = 500  # Timeout for 'send' operation
        # Clear the buffer quickly after the socket is closed
        self._zmq_socket.setsockopt(zmq.LINGER, 100)

        try:
            await self._zmq_open_connection(self._zmq_server_address)
            logger.info("Connected to ZeroMQ server '%s'", self._zmq_server_address)
            self._msg_in = await self._send_command(method=self._msg_method_out, params=self._msg_params_out)
            self._msg_err_in = ""
        except Exception as ex:
            self._msg_in = None
            self._msg_err_in = str(ex)
            # Close the socket to clear the buffer quickly in case of an error.
            self._zmq_socket.close()

        if self._msg_err_in:
            logger.warning("Communication with RE Manager failed: %s", str(self._msg_err_in))

    async def _send_command(self, *, method, params=None):
        msg_out = self._create_msg(method=method, params=params)
        msg_in = await self._zmq_communicate(msg_out)
        return msg_in

    def _create_msg(self, method, params=None):
        """
        The function combines 'method' and 'params' to form a request message
        sent over ZMQ.
        """
        params = params or {}
        return {"method": method, "params": params}

    def set_msg_out(self, method, params):
        self._msg_method_out = method
        self._msg_params_out = params

    def get_msg_in(self):
        return self._msg_in, self._msg_err_in


def zmq_communicate(method, params=None):
    """
    Communicate with RE Manager.

    Parameters
    ----------
    method: str
        Name of the method called in RE Manager
    params: dict or None
        Dictionary of parameters (payload of the message). If ``None`` then
        the message is sent with empty payload: ``params = {}``.

    Returns
    -------
    msg: dict or None
        Message received from RE Manager in response to the request. None if communication
        error (timeout) occurred.
    err_msg: str
        Contains a message in case communication error (timeout) occurs. Empty string otherwise.
    """
    zmq_client = TinyZmqClient()
    zmq_client.set_msg_out(method, params)
    asyncio.run(zmq_client.zmq_single_request())
    msg, err_msg = zmq_client.get_msg_in()
    return msg, err_msg


def get_queue_state():
    method, params = "status", None
    msg, _ = zmq_communicate(method, params)
    if msg is None:
        raise TimeoutError("Timeout occurred while reading RE Manager status.")
    return msg


def get_queue():
    """
    Returns current queue.
    """
    method, params = "queue_get", None
    msg, _ = zmq_communicate(method, params)
    if msg is None:
        raise TimeoutError("Timeout occurred while loading queue from RE Manager.")
    return msg


def get_reduced_state_info():
    msg = get_queue_state()
    plans_in_queue = msg["plans_in_queue"]
    queue_is_running = msg["manager_state"] == "executing_queue"
    plans_in_history = msg["plans_in_history"]
    return plans_in_queue, queue_is_running, plans_in_history


def condition_manager_idle(msg):
    return msg["manager_state"] == "idle"


def condition_manager_paused(msg):
    return msg["manager_state"] == "paused"


def condition_environment_created(msg):
    return msg["worker_environment_exists"]


def condition_environment_closed(msg):
    return not msg["worker_environment_exists"]


def condition_queue_processing_finished(msg):
    plans_in_queue = msg["plans_in_queue"]
    queue_is_running = msg["manager_state"] == "executing_queue"
    return (plans_in_queue == 0) and not queue_is_running


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

    def stop_manager(self, timeout=5):
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
            # Try to stop the manager in a nice way first by sending the command
            zmq_communicate(method="manager_stop", params=None)
            try:
                self._p.wait(timeout)
                clear_redis_pool()

            except subprocess.TimeoutExpired:
                # The manager is not responsive, so just kill the process.
                self._p.kill()
                clear_redis_pool()
                assert False, "RE Manager failed to stop"

            self._p = None


@pytest.fixture
def re_manager():
    """
    Start RE Manager as a subprocess. Tests will communicate with RE Manager via ZeroMQ.
    """
    re = ReManager()
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
    yield re, pc_path  # Location of the copy of the default profile collection.
    re.stop_manager()
