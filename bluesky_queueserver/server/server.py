import asyncio
import logging
from enum import Enum

import zmq
import zmq.asyncio
from fastapi import FastAPI, HTTPException

logger = logging.getLogger(__name__)


# Login and authentication are not implemented, but some API methods require
#   login data. So for now we set up fixed user name and group
_login_data = {"user": "John Doe", "user_group": "admin"}


class ZMQComm:
    def __init__(self, zmq_host="localhost", zmq_port="5555"):
        self._loop = asyncio.get_event_loop()

        # ZeroMQ communication
        self._ctx = zmq.asyncio.Context()
        self._zmq_socket = None
        self._zmq_server_address = f"tcp://{zmq_host}:{zmq_port}"

        # Start communication task
        self._event_zmq_stop = None
        self._task_zmq_client_conn = asyncio.ensure_future(self._zmq_start_client_conn())

        self._lock_zmq = asyncio.Lock()

    def __del__(self):
        # Cancel the communication task
        if not self._task_zmq_client_conn.done():
            self._task_zmq_client_conn.cancel()

    def get_loop(self):
        """
        Returns the asyncio loop.
        """
        return self._loop

    # ==========================================================================
    #    Functions that support ZeroMQ communications with RE Manager

    async def _zmq_send(self, msg):
        await self._zmq_socket.send_json(msg)

    async def _zmq_receive(self):
        try:
            msg = await self._zmq_socket.recv_json()
        except Exception as ex:
            # TODO: proper error handling for cases when connection is interrupted.
            # This is primitive error handling to make sure the server doesn't get stuck
            #   if there is no reply after timeout. The server still needs to be restarted
            #   if it was disconnected, since there is not mechanism to reestablish
            #   connection.
            logger.exception("ZeroMQ communication failed: %s" % str(ex))
            msg = {}
        return msg

    async def _zmq_communicate(self, msg_out):
        await self._zmq_send(msg_out)
        msg_in = await self._zmq_receive()
        return msg_in

    def _zmq_socket_open(self):
        self._zmq_socket = self._ctx.socket(zmq.REQ)
        self._zmq_socket.RCVTIMEO = 2000  # Timeout for 'recv' operation
        self._zmq_socket.SNDTIMEO = 500  # Timeout for 'send' operation
        # Clear the buffer quickly after the socket is closed
        self._zmq_socket.setsockopt(zmq.LINGER, 100)

        self._zmq_socket.connect(self._zmq_server_address)
        logger.info("Connected to ZeroMQ server '%s'" % str(self._zmq_server_address))

    def _zmq_socket_restart(self):
        self._zmq_socket.close()
        self._zmq_socket_open()

    async def _zmq_start_client_conn(self):
        self._event_zmq_stop = asyncio.Event()
        self._zmq_socket_open()
        # The event must be set somewhere else
        await self._event_zmq_stop.wait()

    def _create_msg(self, *, command, params=None):
        return {"method": command, "params": params}

    async def send_command(self, *, command, params=None):
        async with self._lock_zmq:
            try:
                msg_out = self._create_msg(command=command, params=params)
                msg_in = await self._zmq_communicate(msg_out)
                if not msg_in:
                    raise Exception("Timeout while waiting for response from RE Manager.")
            except Exception as ex:
                # This is very likely a timeout (RE Manager is not responding)
                msg_in = {"success": False, "msg": f"ZMQ communication error: {str(ex)}"}
                self._zmq_socket_restart()
            return msg_in


logging.basicConfig(level=logging.WARNING)
logging.getLogger("bluesky_queueserver").setLevel("DEBUG")

# Use FastAPI
app = FastAPI()
re_server = ZMQComm()


class REPauseOptions(str, Enum):
    deferred = "deferred"
    immediate = "immediate"


def validate_payload_keys(payload, *, required_keys=None, optional_keys=None):
    """
    Validate keys in the payload. Raise an exception if the request contains unsupported
    keys or if some of the required keys are missing.

    Parameters
    ----------
    payload: dict
        Payload received with the request.
    required_keys: list(str)
        List of the required payload keys. All the keys must be present in the request.
    optional_keys: list(str)
        List of optional keys.

    Raises
    ------
    ValueError
        payload contains unsupported keys or some of the required keys are missing.
    """

    # TODO: it would be better to use something similar to 'jsonschema' validator.
    #   Unfortunately 'jsonschema' provides terrible error reporting.
    #   Any suggestions?
    #   For now let's use primitive validaator that ensures that the dictionary
    #   has necessary and only allowed top level keys.

    required_keys = required_keys or []
    optional_keys = optional_keys or []

    payload_keys = list(payload.keys())
    r_keys = set(required_keys)
    a_keys = set(required_keys).union(set(optional_keys))
    extra_keys = set()

    for key in payload_keys:
        if key not in a_keys:
            extra_keys.add(key)
        else:
            r_keys -= {key}

    err_msg = ""
    if r_keys:
        err_msg += f"Some required keys are missing in the request: {r_keys}. "
    if extra_keys:
        err_msg += f"Request contains keys the are not supported: {extra_keys}."

    if err_msg:
        raise ValueError(err_msg)


@app.get("/")
async def ping_handler():
    """
    May be called to get some response from the server. Currently returns status of RE Manager.
    """
    msg = await re_server.send_command(command="")
    return msg


@app.get("/status")
async def status_handler():
    """
    Returns status of RE Manager.
    """
    msg = await re_server.send_command(command="status")
    return msg


@app.get("/queue/get")
async def queue_get_handler():
    """
    Returns the contents of the current queue.
    """
    msg = await re_server.send_command(command="queue_get")
    return msg


@app.post("/queue/clear")
async def queue_clear_handler():
    """
    Clear the plan queue.
    """
    msg = await re_server.send_command(command="queue_clear")
    return msg


@app.post("/queue/start")
async def queue_start_handler():
    """
    Start execution of the loaded queue. Additional runs can be added to the queue while
    it is executed. If the queue is empty, then nothing will happen.
    """
    msg = await re_server.send_command(command="queue_start")
    return msg


@app.post("/queue/stop")
async def queue_stop():
    """
    Activate the sequence of stopping the queue. The currently running plan will be completed,
    but the next plan will not be started. The request will be rejected if no plans are currently
    running
    """
    msg = await re_server.send_command(command="queue_stop")
    return msg


@app.post("/queue/stop/cancel")
async def queue_stop_cancel():
    """
    Cancel pending request to stop the queue while the current plan is still running.
    It may be useful if the `/queue/stop` request was issued by mistake or the operator
    changed his mind. Since `/queue/stop` takes effect only after the currently running
    plan is completed, user may have time to cancel the request and continue execution of
    the queue. The command always succeeds, but it has no effect if no queue stop
    requests are pending.
    """
    msg = await re_server.send_command(command="queue_stop_cancel")
    return msg


@app.post("/queue/plan/add")
async def queue_plan_add_handler(payload: dict):
    """
    Adds new plan to the queue
    """
    # TODO: validate inputs!
    params = payload
    params["user"] = _login_data["user"]
    params["user_group"] = _login_data["user_group"]
    msg = await re_server.send_command(command="queue_plan_add", params=params)
    return msg


@app.post("/queue/plan/remove")
async def queue_plan_remove_handler(payload: dict):
    """
    Remove plan from the queue
    """
    msg = await re_server.send_command(command="queue_plan_remove", params=payload)
    return msg


@app.post("/queue/plan/get")
async def queue_plan_get_handler(payload: dict):
    """
    Get a plan from the queue
    """
    msg = await re_server.send_command(command="queue_plan_get", params=payload)
    return msg


@app.get("/history/get")
async def history_get_handler():
    """
    Returns the plan history (list of dicts).
    """
    msg = await re_server.send_command(command="history_get")
    return msg


@app.post("/history/clear")
async def history_clear_handler():
    """
    Clear plan history.
    """
    msg = await re_server.send_command(command="history_clear")
    return msg


@app.post("/environment/open")
async def environment_open_handler():
    """
    Creates RE environment: creates RE Worker process, starts and configures Run Engine.
    """
    msg = await re_server.send_command(command="environment_open")
    return msg


@app.post("/environment/close")
async def environment_close_handler():
    """
    Orderly closes of RE environment. The command returns success only if no plan is running,
    i.e. RE Manager is in the idle state. The command is rejected if a plan is running.
    """
    msg = await re_server.send_command(command="environment_close")
    return msg


@app.post("/environment/destroy")
async def environment_destroy_handler():
    """
    Destroys RE environment by killing RE Worker process. This is a last resort command which
    should be made available only to expert level users.
    """
    msg = await re_server.send_command(command="environment_destroy")
    return msg


@app.post("/re/pause")
async def re_pause_handler(payload: dict):
    """
    Pause Run Engine.
    """
    try:
        validate_payload_keys(payload, required_keys=["option"])
        if not hasattr(REPauseOptions, payload["option"]):
            raise ValueError(
                f'The specified option "{payload["option"]}" is not allowed.\n'
                f"Allowed options: {list(REPauseOptions.__members__.keys())}"
            )
    except Exception as ex:
        raise HTTPException(status_code=444, detail=str(ex))

    msg = await re_server.send_command(command="re_pause", params=payload)
    return msg


@app.post("/re/resume")
async def re_resume_handler():
    """
    Run Engine: resume execution of a paused plan
    """
    msg = await re_server.send_command(command="re_resume")
    return msg


@app.post("/re/stop")
async def re_stop_handler():
    """
    Run Engine: stop execution of a paused plan
    """
    msg = await re_server.send_command(command="re_stop")
    return msg


@app.post("/re/abort")
async def re_abort_handler():
    """
    Run Engine: abort execution of a paused plan
    """
    msg = await re_server.send_command(command="re_abort")
    return msg


@app.post("/re/halt")
async def re_halt_handler():
    """
    Run Engine: halt execution of a paused plan
    """
    msg = await re_server.send_command(command="re_halt")
    return msg


@app.get("/plans/allowed")
async def plans_allowed_handler():
    """
    Returns the lists of allowed plans.
    """
    params = {"user_group": _login_data["user_group"]}
    msg = await re_server.send_command(command="plans_allowed", params=params)
    return msg


@app.get("/devices/allowed")
async def devices_allowed_handler():
    """
    Returns the lists of allowed devices.
    """
    params = {"user_group": _login_data["user_group"]}
    msg = await re_server.send_command(command="devices_allowed", params=params)
    return msg


@app.post("/manager/stop")
async def manager_stop_handler(payload: dict):
    """
    Stops of RE Manager. RE Manager will not be restarted after it is stoped.
    """
    msg = await re_server.send_command(command="manager_stop", params=payload)
    return msg


@app.post("/test/manager/kill")
async def test_manager_kill_handler():
    """
    The command stops event loop of RE Manager process. Used for testing of RE Manager
    stability and handling of communication timeouts.
    """
    msg = await re_server.send_command(command="manager_kill")
    return msg
